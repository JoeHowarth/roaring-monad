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
    publication_epoch: int


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
    "publication_state" -> PublicationState
    "block_meta/<block_num>" -> BlockMeta
    "block_hash_to_num/<block_hash>" -> block_num
    "log_dir_frag/<sub_bucket_start>/<block_num>" -> LogDirFragment
    "log_dir_sub/<sub_bucket_start>" -> LogDirectoryBucket
    "log_dir/<bucket_start_log_id>" -> optional compacted LogDirectoryBucket
    "block_log_headers/<block_num>" -> BlockLogHeader
    "open_stream_page/<shard>/<page_start>/<stream_id>" -> marker
    "stream_frag_meta/<stream_id>/<page_start>/<block_num>" -> StreamBitmapMeta
    "stream_page_meta/<stream_id>/<page_start>" -> StreamBitmapMeta

blob_store:
    "block_logs/<block_num>" -> concatenated encoded logs
    "stream_frag_blob/<stream_id>/<page_start>/<block_num>" -> roaring_bitmap_blob
    "stream_page_blob/<stream_id>/<page_start>" -> roaring_bitmap_blob
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
    async def startup(self) -> RecoveryPlan:
        ...

    async def query_logs(self, request: QueryLogsRequest, budget: ExecutionBudget) -> QueryPage[Log]:
        ...

    async def ingest_finalized_block(self, block: Block) -> IngestOutcome:
        ...

    async def ingest_finalized_blocks(self, blocks: list[Block]) -> IngestOutcome:
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
        clauses.sort_by(estimated_count)

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
async def startup(writer_id):
    lease = acquire_publication(writer_id)
    cleanup_unpublished_suffix(lease.indexed_finalized_head)
    repair_open_stream_page_markers(derive_next_log_id_from_block_meta(lease.indexed_finalized_head))
    return lease

async def ingest_finalized_blocks(blocks, lease):
    validate_contiguous_finalized_sequence_and_parent(blocks, lease.indexed_finalized_head)

    first_log_id = derive_next_log_id_from_block_meta(lease.indexed_finalized_head)
    next_log_id = first_log_id
    opened_during = []

    for block in blocks:
        await logs.persist_log_artifacts(block.logs, next_log_id)
        await logs.persist_log_block_metadata(block, next_log_id)
        await logs.persist_log_directory_fragments(block, next_log_id)
        opened_during.extend(await logs.persist_stream_fragments(block, next_log_id))
        next_log_id += len(block.logs)

    await mark_open_stream_pages_that_remain_open(opened_during, next_log_id)

    await logs.compact_newly_sealed_directory_boundaries(first_log_id, next_log_id)
    await seal_newly_sealed_stream_pages(first_log_id, next_log_id, opened_during)

    await compare_and_set_publication_state(
        expected=lease,
        next_head=blocks[-1].block_num,
    )
```

Important boundary:

- `api/service.rs` startup acquires publication ownership, runs cleanup-first recovery, and caches the lease for ingest
- `ingest/engine.rs` validates finalized sequencing and orchestrates publication for `H -> T`
- `logs/ingest.rs` owns immutable directory/stream fragment publication plus eager compaction
- `publication_state.indexed_finalized_head` is published last and is the only reader-visible watermark

## Current Non-Goals

The current crate does not yet implement:

- descending traversal
- non-log families
- relation hydration helpers
- canonical block / transaction / trace artifact stores
- transport-level `block_hash` query mode
