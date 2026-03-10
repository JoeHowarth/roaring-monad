# Finalized History Query High-Level Pseudocode

This is intentionally not real code. It is a compact sketch of the current implementation so the control flow is easy to understand before reading the Rust modules.

## Core Terms

```python
block_window = inclusive finalized block range after validation and clipping
primary_id = family-specific monotonic ID
log_id = the primary_id for logs
resume_log_id = declarative log-ID lower bound in the request
next_resume_log_id = token returned for the next request
cursor_block = block containing the last returned item
effective_limit = min(request.limit, budget.max_results or request.limit)
```

## Main Types

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


class BlockIdentity:
    number: int
    hash: bytes32
    parent_hash: bytes32


class LogSequencingState:
    next_log_id: int


class LogBlockWindow:
    first_log_id: int
    count: int
```

## Persisted Model

```python
meta_store:
    "meta/state" -> MetaState
    "block_meta/<block_num>" -> BlockMeta
    "block_hash_to_num/<block_hash>" -> block_num
    "log_dir/<bucket_start_log_id>" -> LogDirectoryBucket covering all block ranges that touch the bucket
    "block_log_headers/<block_num>" -> BlockLogHeader
    "manifests/<stream_id>" -> Manifest
    "tails/<stream_id>" -> roaring_bitmap

blob_store:
    "block_logs/<block_num>" -> concatenated encoded logs
    "chunks/<stream_id>/<chunk_seq>" -> roaring_chunk_bytes
```

The current code treats those bytes through shared and family-local helpers:

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
class FinalizedHistoryService:
    async def query_logs(self, request: QueryLogsRequest, budget: ExecutionBudget) -> QueryPage[Log]:
        ...

    async def ingest_finalized_block(self, block: Block) -> IngestOutcome:
        ...
```

This boundary is transport-free:

- the RPC crate parses and validates transport requests
- this crate executes queries and ingest
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
        return empty_page(block_window)

    log_window = logs.window_resolver.resolve(block_window)
    if log_window.is_empty():
        return empty_page(block_window)

    if request.resume_log_id is not None:
        if not log_window.contains(request.resume_log_id):
            raise InvalidParams("resume_log_id outside resolved block window")

        log_window = log_window.resume_strictly_after(request.resume_log_id)
        if log_window.is_empty():
            return empty_page(block_window)

    if logs.should_use_block_scan(request.filter):
        matching = await logs.block_scan.execute(
            block_window=block_window,
            log_window=log_window,
            filter=request.filter,
            take=effective_limit + 1,
        )
    else:
        clause_plan = logs.build_clause_plan(
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

    return QueryPage(
        items=[match.item for match in page_matches],
        meta=QueryPageMeta(
            resolved_from_block=block_window.resolved_from_ref,
            resolved_to_block=block_window.resolved_to_ref,
            cursor_block=page_matches[-1].block_ref if page_matches else block_window.examined_endpoint_ref,
            has_more=has_more,
            next_resume_log_id=page_matches[-1].id if has_more and page_matches else None,
        ),
    )
```

## Shared Candidate Execution

The shared executor works on primary IDs, not directly on `Log`.

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
                block_ref=await materializer.block_ref_for(item),
            )
        )

        if len(out) >= take:
            break

    return out
```

This preserves the primary ID long enough to emit:

- exact `has_more`
- exact `next_resume_log_id`
- exact `cursor_block`

## Broad-Query Block Scan

The block-scan path stays in the logs family because it depends on log-specific block metadata.

```python
async def log_block_scan(block_window, log_window, filter, take):
    out = []

    for block_num in range(block_window.from_block, block_window.to_block + 1):
        block_ref = load_shared_block_ref(block_num)
        log_block_window = load_log_block_window(block_num)
        if log_block_window is None or log_block_window.count == 0:
            continue

        start = max(log_block_window.first_log_id, log_window.start)
        end = min(
            log_block_window.first_log_id + log_block_window.count - 1,
            log_window.end_inclusive,
        )
        if start > end:
            continue

        header = await logs.storage.load_block_header(block_num)
        payload = await logs.storage.read_full_block_blob(block_num)
        if header is None or payload is None:
            continue

        for log_id in range(start, end + 1):
            local_ordinal = log_id - log_block_window.first_log_id
            log = decode_one_log(payload, header.offsets, local_ordinal)
            if not logs.materializer.exact_match(log, filter):
                continue

            out.append(MatchedPrimary(id=log_id, item=log, block_ref=block_ref))
            if len(out) >= take:
                return out

    return out
```

That path uses the same page semantics as the indexed path.

## Ingest Flow

```python
async def ingest_finalized_block(block, writer_epoch):
    shared_state = load_meta_state()
    validate_block_sequence_and_parent(block, shared_state)

    first_log_id = shared_state.next_log_id

    await logs.persist_log_artifacts(block.logs, first_log_id)
    await logs.persist_log_block_metadata(block, first_log_id)

    appends = logs.collect_stream_appends(block, first_log_id)
    await streams.writer.apply(appends)

    next_state = MetaState(
        indexed_finalized_head=block.block_num,
        next_log_id=first_log_id + len(block.logs),
        writer_epoch=writer_epoch,
    )
    await store_meta_state(next_state)
```

Important boundary:

- `ingest/engine.rs` validates finalized sequencing and orchestrates work
- `logs/ingest.rs` owns log-family write layout and stream fanout
- `streams/writer.rs` owns append / seal mechanics

## Current Non-Goals

The current crate does not yet implement:

- descending traversal
- non-log families
- relation hydration helpers
- canonical block / transaction / trace artifact stores
- transport-level `block_hash` query mode
