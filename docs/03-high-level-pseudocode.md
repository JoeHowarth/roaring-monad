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
    if not logs.has_indexed_clause(request.filter):
        raise InvalidParams("query must include at least one indexed address or topic clause")

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

    matching = await logs.shard_streaming_executor.execute(
        filter=request.filter,
        id_window=log_window,
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

## Shard-Streaming Indexed Execution

The indexed executor works on one shard at a time and preserves primary IDs long enough to emit exact pagination metadata.

```python
async def execute(filter, id_window, take):
    out = []

    for shard in overlapping_shards(id_window):
        local_range = local_range_for_shard(id_window, shard)
        clauses = await prepare_shard_clauses(filter, shard, local_range)
        clauses.sort_by(sealed_estimate)

        shard_accumulator = None
        for clause in clauses:
            bitmap = await load_clause_bitmap_for_one_shard(clause, local_range)
            if shard_accumulator is None:
                shard_accumulator = bitmap
            else:
                shard_accumulator &= bitmap
            if shard_accumulator.is_empty():
                break

        for local_id in shard_accumulator:
            id = compose_global_log_id(shard, local_id)
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
                return out

    return out
```

This preserves the primary ID long enough to emit:

- exact `has_more`
- exact `next_resume_log_id`
- exact `cursor_block`

## Unsupported Query Shapes

Queries without at least one indexed address/topic clause are rejected at the boundary.

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
