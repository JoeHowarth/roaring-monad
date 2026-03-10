# Finalized History Query High-Level Pseudocode

This is intentionally not real code. It is a high-level sketch of the current wave-1 architecture so the core control flow is easier to understand before reading the implementation.

The goal is to show:

- what the transport-free service boundary looks like
- which parts of query execution are shared vs log-specific
- how pagination metadata is supposed to work
- how ingest still fits under the new boundaries

## Core Terms

```python
block_window = inclusive finalized block range after validation and clipping
primary_id = family-specific monotonic ID
log_id = the primary_id for logs in wave 1
resume_log_id = declarative log-ID boundary in the request
next_resume_log_id = token returned for the next request
cursor_block = block containing the last returned item, used for reorg handling
effective_limit = min(request.limit, budget.max_results or request.limit)
```

## Main Data Shapes

```python
class QueryOrder:
    ASCENDING


class QueryLogsRequest:
    from_block: int
    to_block: int
    order: QueryOrder
    resume_log_id: int | None
    limit: int
    filter: LogFilter


class ExecutionBudget:
    max_results: int | None


class BlockRef:
    number: int
    hash: bytes32
    parent_hash: bytes32


class QueryPageMeta:
    resolved_from_block: BlockRef
    resolved_to_block: BlockRef
    cursor_block: BlockRef
    has_more: bool
    next_resume_log_id: int | None


class QueryPage[T]:
    items: list[T]
    meta: QueryPageMeta


class FinalizedHeadState:
    indexed_finalized_head: int
    writer_epoch: int


class LogSequencingState:
    next_log_id: int


class BlockIdentity:
    number: int
    hash: bytes32
    parent_hash: bytes32


class LogBlockWindow:
    first_log_id: int
    count: int
```

## Storage Model In Wave 1

Wave 1 still uses the current storage model even though the module boundaries change.

```python
meta_store:
    "meta/state" -> MetaState
    "block_meta/<block_num>" -> BlockMeta
    "block_hash_to_num/<block_hash>" -> block_num
    "log_locator_pages/<page_start_log_id>" -> {slot -> LogLocator}
    "manifests/<stream_id>" -> Manifest
    "tails/<stream_id>" -> roaring_bitmap

blob_store:
    "log_packs/<first_log_id>" -> packed_log_bytes
    "chunks/<stream_id>/<chunk_seq>" -> roaring_chunk_bytes
```

Conceptually, the code will treat those persisted records through cleaner views:

```python
shared views:
    FinalizedHeadState
    BlockIdentity

log views:
    LogSequencingState
    LogBlockWindow
```

## Top-Level Service Boundary

```python
class FinalizedLogQueryService:
    async def query_logs(self, request: QueryLogsRequest, budget: ExecutionBudget) -> QueryPage[Log]:
        ...


class FinalizedHistoryWriter:
    async def ingest_finalized_block(self, block: Block) -> IngestOutcome:
        ...
```

This service boundary is transport-free:

- the RPC crate parses JSON-RPC into `QueryLogsRequest`
- this crate executes the query and returns `QueryPage[Log]`
- the RPC crate formats the final response envelope

## Query Flow

```python
async def query_logs(request, budget):
    if request.limit == 0:
        raise InvalidParams("limit must be at least 1")

    effective_limit = min(request.limit, budget.max_results or request.limit)

    block_window = range_resolver.resolve(
        from_block=request.from_block,
        to_block=request.to_block,
        order=request.order,
    )
    if block_window.is_empty():
        return QueryPage(
            items=[],
            meta=QueryPageMeta(
                resolved_from_block=block_window.resolved_from_ref,
                resolved_to_block=block_window.resolved_to_ref,
                cursor_block=block_window.examined_endpoint_ref,
                has_more=False,
                next_resume_log_id=None,
            ),
        )

    log_window = logs.window_resolver.resolve(block_window)
    if log_window.is_empty():
        return QueryPage(
            items=[],
            meta=QueryPageMeta(
                resolved_from_block=block_window.resolved_from_ref,
                resolved_to_block=block_window.resolved_to_ref,
                cursor_block=block_window.examined_endpoint_ref,
                has_more=False,
                next_resume_log_id=None,
            ),
        )

    if request.resume_log_id is not None:
        if not log_window.contains(request.resume_log_id):
            raise InvalidParams("resume_log_id outside resolved block window")

        log_window = log_window.resume_strictly_after(request.resume_log_id)
        if log_window.is_empty():
            return QueryPage(
                items=[],
                meta=QueryPageMeta(
                    resolved_from_block=block_window.resolved_from_ref,
                    resolved_to_block=block_window.resolved_to_ref,
                    cursor_block=block_window.examined_endpoint_ref,
                    has_more=False,
                    next_resume_log_id=None,
                ),
            )

    if logs.should_use_block_scan(request.filter):
        matching = await logs.block_scan.execute(
            block_window=block_window,
            log_window=log_window,
            filter=request.filter,
            take=effective_limit + 1,
        )
    else:
        clause_plan = streams.planner.plan(
            filter=request.filter,
            id_window=log_window,
        )

        matching = await shared_candidate_executor.execute(
            clause_plan=clause_plan,
            id_window=log_window,
            filter=request.filter,
            materializer=logs.materializer,
            take=effective_limit + 1,
        )

    has_more = len(matching) > effective_limit
    page_matches = matching[:effective_limit]

    items = [match.item for match in page_matches]
    next_resume_log_id = page_matches[-1].id if has_more and page_matches else None

    if page_matches:
        cursor_block = page_matches[-1].block_ref
    else:
        cursor_block = block_window.examined_endpoint_ref

    return QueryPage(
        items=items,
        meta=QueryPageMeta(
            resolved_from_block=block_window.resolved_from_ref,
            resolved_to_block=block_window.resolved_to_ref,
            cursor_block=cursor_block,
            has_more=has_more,
            next_resume_log_id=next_resume_log_id,
        ),
    )
```

## What The Shared Candidate Executor Does

The shared executor should operate on primary IDs, not directly on `Log`.

Conceptually:

```python
async def execute(clause_plan, id_window, filter, materializer, take):
    candidate_ids = intersect_stream_candidates(clause_plan, id_window)

    out = []
    for id in candidate_ids:
        item = await materializer.load_by_id(id)
        if item is None:
            continue
        if not materializer.exact_match(item, filter):
            continue

        out.append(
            MatchedPrimary(
                id=id,
                item=item,
                block_ref=extract_block_ref(item),
            )
        )

        if len(out) >= take:
            break

    return out
```

Why this matters:

- the executor must preserve the primary ID long enough to emit `next_resume_log_id`
- `has_more` must be exact, so the executor needs one-item lookahead or an equivalent mechanism

## Broad-Query Block Scan In Wave 1

The broad-query block-scan path stays in the logs family because it still depends on log-specific block metadata.

Conceptually:

```python
async def log_block_scan(block_window, log_window, filter, take):
    out = []

    for block_num in block_window.iter_ascending():
        block_meta = load_block_meta(block_num)
        for log_id in block_meta.log_ids():
            if log_id not in log_window:
                continue

            log = await logs.materializer.load_by_id(log_id)
            if log is None:
                continue
            if not logs.materializer.exact_match(log, filter):
                continue

            out.append(
                MatchedPrimary(
                    id=log_id,
                    item=log,
                    block_ref=BlockRef(
                        number=log.block_num,
                        hash=log.block_hash,
                        parent_hash=block_meta.parent_hash,
                    ),
                )
            )

            if len(out) >= take:
                return out

    return out
```

That path must use the same pagination semantics as the indexed path:

- one-item lookahead
- exact `has_more`
- item-level resume token
- block-scoped `cursor_block`

## Ingest Flow Under The New Boundary

The ingest path remains log-only in wave 1, but it should read more like:

```python
async def ingest_finalized_block(block, writer_epoch):
    shared_state = load_finalized_head_state()
    log_state = load_log_sequencing_state()

    validate_block_sequence_and_parent(block, shared_state)

    first_log_id = log_state.next_log_id
    write_log_pack_and_locator_pages(block.logs, first_log_id)
    write_block_identity_and_log_window(block, first_log_id)

    for each (log_id, log) in enumerate_logs_with_ids(block.logs, first_log_id):
        appends = logs.indexer.collect_stream_appends(log, log_id)
        streams.writer.apply(appends)

    store_updated_head_and_log_sequencing_state(block, first_log_id, writer_epoch)
```

The important boundary is:

- stream append and seal lifecycle moves into shared stream infrastructure
- log-specific stream fanout rules stay in the logs family

## Deferred Work

Wave 1 intentionally does not do:

- relation hydration helpers
- canonical block / transaction / receipt / trace artifact stores
- descending traversal
- block-hash mode in this crate
- generic relation frameworks

Those are later layers built on top of the cleaner method / family / shared-substrate boundaries above.
