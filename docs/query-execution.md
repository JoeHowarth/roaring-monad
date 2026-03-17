# Query Execution

This document describes the query execution model: shard-streaming, LogId types, pagination, clause filtering, and materialization.

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

## LogId Type System

A `LogId` is a 64-bit value split into two components:

| Component | Bits | Type | Range |
|-----------|------|------|-------|
| Shard | Upper 40 bits | `LogShard` | 0 to 2^40 - 1 |
| Local ID | Lower 24 bits | `LogLocalId` | 0 to 16,777,215 |

Composition: `global_log_id = (shard << 24) | local_id`

The shard determines which stream pages and bitmaps are relevant. The local ID is the position within a shard's bitmap space.

Stream pages span 4,096 local IDs within a shard (see [storage-model.md](storage-model.md) for the stream layout).

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

Key steps:

1. **Range resolution** — `RangeResolver` clips the request to the indexed finalized head
2. **Window resolution** — `LogWindowResolver` maps the block range to a log-ID range
3. **Resume handling** — if `resume_log_id` is set, narrow the window to strictly after it
4. **Shard-streaming execution** — fetch `effective_limit + 1` matches to determine `has_more`
5. **Page assembly** — preserve primary IDs through assembly for exact pagination metadata

## Shard-Streaming Executor

The indexed executor works on one shard at a time in ascending `log_id` order, preserving primary IDs for exact pagination metadata.

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

## Clause Filtering and Bitmap Intersection

Clauses are sorted by estimated cardinality before intersection. The smallest clause loads first, and each subsequent intersection can only shrink the accumulator. If the accumulator empties, the shard is skipped immediately.

Stream scans prefer compacted `stream_page_*` blobs and fall back to `stream_frag_*` blobs for the bounded frontier or compaction lag.

## Materialization

After bitmap intersection identifies candidate `log_id`s, each is materialized:

1. **Directory lookup** — resolve `log_id -> block_num` using the 3-tier directory (bucket → sub-bucket → fragment fallback). See [storage-model.md](storage-model.md) for the lookup flow and worked example.
2. **Range blob read** — load the byte range from `block_logs/<block_num>` using the block header's offset table
3. **Coalesced runs** — contiguous same-block log payload reads may be coalesced into bounded prefix range reads, stopping once exact filtering and `limit + 1` pagination have enough items

Materialized logs are checked against the filter for exact match (the bitmap index is an approximation within the page span).

## Pagination

- `resume_log_id` is a declarative lower bound — the query resumes strictly after this value
- `next_resume_log_id` is the ID of the last returned item when `has_more` is true
- `cursor_block` is the `BlockRef` of the block containing the last returned item
- `has_more` is exact because the executor fetches `limit + 1` candidates

## Non-Indexed Query Rejection

Queries without at least one indexed address/topic clause are rejected at the boundary. There is no fallback to a full scan.
