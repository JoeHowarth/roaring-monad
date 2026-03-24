# Query Execution

This document describes the shared query execution model used by logs and traces: normalized block/ID windows, shard-streaming planning, bitmap intersection, materialization, and pagination.

## Core Terms

```python
block_window = inclusive finalized block range after validation and clipping
primary_id = family-specific monotonic ID
log_id = the primary_id for logs
resume_id = declarative primary-ID lower bound in the request
next_resume_id = token returned for the next request
cursor_block = block containing the last returned item
effective_limit = min(request.limit, budget.max_results or request.limit)
```

## Family ID Type System

Each family ID (`LogId`, `TraceId`, and future peers like transaction IDs) wraps the same shared 64-bit `FamilyId` layout:

| Component | Bits | Type | Range |
|-----------|------|------|-------|
| Shard | Upper 40 bits | family shard newtype | 0 to 2^40 - 1 |
| Local ID | Lower 24 bits | family local-id newtype | 0 to 16,777,215 |

Composition: `family_id = (shard << 24) | local_id`

The shared `FamilyId` core owns shard/local extraction, composition, and generic range helpers. The public wrappers preserve family type safety at API boundaries.

The shard determines which stream pages and bitmaps are relevant. The local ID is the position within a shard's bitmap space.

Stream pages span 4,096 local IDs within a shard (see [storage-model.md](storage-model.md) for the stream layout).

## Query Layers

The execution stack is split into shared substrate plus family-owned boundaries:

1. `query::normalized` computes the effective limit, validates resume IDs, and narrows the primary-ID window.
2. `query::window` resolves the first and last non-empty primary IDs inside the resolved block range.
3. `query::planner` turns family clause vocabularies into shared logical stream selectors and shard-local prepared clauses.
4. `query::bitmap` loads the prepared clause bitmaps from compacted pages or frontier fragments.
5. `core::directory` and `core::directory_resolver` provide the shared directory payloads and `primary_id -> (block_num, local_ordinal)` resolution.
6. `query::runner` executes the shard loop, intersects bitmaps, batches contiguous same-block candidates, materializes outputs, and assembles the page.

Logs and traces still own request validation, indexed clause vocabularies, exact-match semantics, physical key derivation, and output materialization.

## Query Flow

```python
async def query_logs(request, budget):
    if request.limit == 0:
        raise InvalidParams("limit must be at least 1")
    if not logs.has_indexed_clause(request.filter):
        raise InvalidParams("query must include at least one indexed address or topic clause")

    effective_limit = min(request.limit, budget.max_results or request.limit)

    block_window = resolve_block_range(
        from_block=request.from_block,
        to_block=request.to_block,
        order=request.order,
    )
    if block_window.is_empty():
        return empty_page(block_window)

    log_window = resolve_log_window(block_window)
    if log_window.is_empty():
        return empty_page(block_window)

    if request.resume_id is not None:
        if not log_window.contains(request.resume_id):
            raise InvalidParams("resume_id outside resolved block window")

        log_window = log_window.resume_strictly_after(request.resume_id)
        if log_window.is_empty():
            return empty_page(block_window)

    matching = await query.runner.execute_indexed_query(
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
            next_resume_id=page_matches[-1].id if has_more and page_matches else None,
        ),
    )
```

Key steps:

1. **Range resolution** — `RangeResolver` clips the request to the indexed finalized head
2. **Window resolution** — `LogWindowResolver` maps the block range to a log-ID range
3. **Resume handling** — if `resume_id` is set, narrow the window to strictly after it
4. **Shard-streaming execution** — fetch `effective_limit + 1` matches to determine `has_more`
5. **Page assembly** — preserve primary IDs through assembly for exact pagination metadata

## Shared Runner

The indexed runner works on one shard at a time in ascending primary-ID order, preserving IDs for exact pagination metadata.

```python
async def execute_indexed_query(filter, id_window, take):
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
            id = compose_primary_id(shard, local_id)
            location = await materializer.resolve_id(id)
            if location is None:
                continue

            run = collect_contiguous_same_block_prefix(
                shard, local_id, location, remaining_needed(take, out)
            )

            for id, item in await materializer.load_run(run):
                if not materializer.exact_match(item, filter):
                    continue

                out.append(
                    MatchedQueryItem(
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
- exact `next_resume_id`
- exact `cursor_block`

## Clause Filtering and Bitmap Intersection

Clauses are sorted by estimated cardinality before intersection. The smallest clause loads first, and each subsequent intersection can only shrink the accumulator. If the accumulator empties, the shard is skipped immediately.

Stream scans prefer compacted `stream_page_*` blobs and fall back to `stream_frag_*` blobs for the bounded frontier or compaction lag.

## Materialization

After bitmap intersection identifies candidate primary IDs, each family materializer resolves and hydrates them:

1. **Directory lookup** — resolve `primary_id -> (block_num, local_ordinal)` through one shared three-tier algorithm over shared `PrimaryDirBucket` and `PrimaryDirFragment` payloads.
2. **Coalesced runs** — contiguous same-block candidates are discovered and materialized in bounded prefixes so one block-range read or a tight trace loop can serve several matches.
3. **Shared block refs** — `query::runner::cached_block_ref_with_fallback(...)` owns the cache + canonical block-ref lookup path, while each family only supplies its fallback parent-hash source when the canonical block-ref row is absent.

The directory tables are exposed as family-owned bundles on `Tables` (`log_dir()` and `trace_dir()`), but both bundles use the same shared bucket/sub-bucket/fragment table types.

Materialized items are checked against the family filter for exact match because the bitmap index remains a candidate filter rather than a full predicate evaluation.

## Public Runner API

`query::runner` is also the public execution seam for low-level benchmarks and experiments:

- `QueryId` and `QueryIdRange<I>` model generic monotonic query IDs such as `LogId` and `TraceId`
- `QueryMaterializer` provides the generic resolve/load/exact-match/materialize hooks
- `ShardBitmapSet` is the generic shard-to-bitmap input shape for candidate execution
- `execute_candidates(...)` runs bitmap intersection and exact filtering for precomputed shard bitmaps

The higher-level family query engines build on the same module through the internal descriptor-based indexed runner.

## Pagination

- `resume_id` is a declarative lower bound — the query resumes strictly after this value
- `next_resume_id` is the ID of the last returned item when `has_more` is true
- `cursor_block` is the `BlockRef` of the block containing the last returned item
- `has_more` is exact because the executor fetches `limit + 1` candidates

## Non-Indexed Query Rejection

Queries without at least one indexed address/topic clause are rejected at the boundary. There is no fallback to a full scan.
