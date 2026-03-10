# Finalized Log Index High-Level Pseudocode

This is intentionally not real code. It is a high-level sketch of the current control flow.

## Main Data Shapes

```python
class QueryLogsRequest:
    from_block: int
    to_block: int
    order: "ascending"
    resume_log_id: int | None
    limit: int
    filter: LogFilter


class ExecutionBudget:
    max_results: int | None


class QueryPageMeta:
    resolved_from_block: BlockRef
    resolved_to_block: BlockRef
    cursor_block: BlockRef
    has_more: bool
    next_resume_log_id: int | None


class QueryPage[T]:
    items: list[T]
    meta: QueryPageMeta
```

## Top-Level Service

```python
class FinalizedHistoryService:
    async def ingest_finalized_block(self, block):
        reject_if_degraded_or_throttled()
        return ingest_engine.ingest_finalized_block(block, writer_epoch)

    async def query_logs(self, request, budget):
        reject_if_degraded()
        return logs_query_engine.query_logs(meta_store, blob_store, request, budget)
```

## Query Flow

```python
async def query_logs(meta_store, blob_store, request, budget):
    if request.limit == 0:
        raise InvalidParams("limit must be at least 1")

    effective_limit = min(request.limit, budget.max_results or request.limit)

    block_range = range_resolver.resolve(
        from_block=request.from_block,
        to_block=request.to_block,
        order=request.order,
    )
    if block_range.is_empty():
        return empty_page(block_range)

    log_window = logs.window_resolver.resolve(block_range)
    if log_window.is_empty():
        return empty_page(block_range)

    if request.resume_log_id is not None:
        if not log_window.contains(request.resume_log_id):
            raise InvalidParams("resume_log_id outside resolved block window")
        log_window = log_window.resume_strictly_after(request.resume_log_id)
        if log_window.is_empty():
            return empty_page(block_range)

    if logs.should_force_block_scan(request.filter):
        matched = logs.block_scan.execute(
            block_range=block_range,
            log_window=log_window,
            filter=request.filter,
            take=effective_limit + 1,
        )
    else:
        clause_order = logs.index_spec.build_clause_order(
            filter=request.filter,
            from_log_id=log_window.start,
            to_log_id=log_window.end_inclusive,
        )
        clause_sets = load_stream_candidates(clause_order, log_window)
        matched = shared_candidate_executor.execute(
            clause_sets=clause_sets,
            id_range=log_window,
            filter=request.filter,
            materializer=logs.materializer,
            take=effective_limit + 1,
        )

    has_more = len(matched) > effective_limit
    page_items = matched[:effective_limit]

    return QueryPage(
        items=[m.item for m in page_items],
        meta=QueryPageMeta(
            resolved_from_block=block_range.resolved_from_ref,
            resolved_to_block=block_range.resolved_to_ref,
            cursor_block=page_items[-1].block_ref if page_items else block_range.examined_endpoint_ref,
            has_more=has_more,
            next_resume_log_id=page_items[-1].id if has_more and page_items else None,
        ),
    )
```

## Ingest Flow

```python
async def ingest_finalized_block(block, writer_epoch):
    state = load_meta_state()
    validate_sequence_and_parent(block, state)

    first_log_id = state.next_log_id
    write_packed_logs_and_locator_pages(block.logs, first_log_id)
    write_block_meta(block, first_log_id)
    write_block_hash_lookup(block)

    appends = logs.collect_stream_appends(block, first_log_id)
    for stream_id, local_values in appends:
        streams.apply_stream_appends(stream_id, local_values)

    store_next_meta_state(
        indexed_finalized_head=block.block_num,
        next_log_id=first_log_id + len(block.logs),
        writer_epoch=writer_epoch,
    )
```

## Storage Model

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
