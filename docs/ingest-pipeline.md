# Ingest Pipeline

This document describes the block ingestion flow, artifact writes, compaction triggers, and startup behavior.

## Block Ingestion Flow

```python
async def ingest_finalized_blocks(blocks, lease):
    validate_contiguous_finalized_sequence_and_parent(blocks, lease.indexed_finalized_head)

    first_log_id = derive_next_log_id_from_block_record(lease.indexed_finalized_head)
    next_log_id = first_log_id
    opened_during = []

    for block in blocks:
        await logs.persist_log_artifacts(block.logs, next_log_id)
        await logs.persist_log_block_record(block, next_log_id)
        await logs.persist_log_dir_by_block(block, next_log_id)
        opened_during.extend(await logs.persist_stream_fragments(block, next_log_id))
        next_log_id += len(block.logs)

    await mark_open_bitmap_pages_that_remain_open(opened_during, next_log_id)

    await logs.compact_newly_sealed_directory_boundaries(first_log_id, next_log_id)
    await seal_newly_sealed_stream_pages(first_log_id, next_log_id, opened_during)

    await compare_and_set_publication_state(
        expected=lease,
        next_head=blocks[-1].block_num,
    )
```

The `IngestEngine` orchestrates this flow. It validates finalized sequencing and parent hashes, then delegates artifact writes to the logs family.

## Artifact Write Order

For each block in the batch:

1. **Log blob** — `block_log_blob` blob table, key `<block_num>`: concatenated encoded log bytes
2. **Block log header** — `block_log_header` table, key `<block_num>`: byte offset table for local ordinals
3. **Block meta** — `block_record` table, key `<block_num>`: `{ block_hash, parent_hash, first_log_id, count }`
4. **Block hash index** — `block_hash_index` table, key `<block_hash>`: reverse lookup
5. **Directory fragments** — one `log_dir_by_block` row per covered sub-bucket, keyed by partition `<sub_bucket_start>` and clustering `<block_num>`
6. **Stream fragments** — `bitmap_by_block` rows per stream per page touched

All artifacts must be durable before the head advance — see the publication ordering invariant in [storage-model.md](storage-model.md). Artifact writes are unconditional; publication remains the only visibility boundary.

## Directory Compaction

After all blocks in the batch are persisted, sealed directory boundaries are compacted:

- **Sub-bucket compaction**: when `next_log_id` crosses a sub-bucket boundary (10,000 `log_id` range), the sealed sub-bucket's fragments are compacted into `log_dir_sub_bucket`, key `<sub_bucket_start>`
- **Bucket compaction**: when `next_log_id` crosses a 1M-bucket boundary, the sealed sub-buckets are optionally compacted into `log_dir_bucket`, key `<bucket_start>`

See [storage-model.md](storage-model.md) for directory layout details.

## Stream Page Compaction

When `next_log_id` crosses a stream page boundary (see [storage-model.md](storage-model.md) for page span), the sealed page's fragments are compacted:

1. load all `bitmap_by_block` entries for the page
2. merge the roaring bitmaps
3. write compacted `bitmap_page_meta` and `bitmap_page_blob`
4. delete the `open_bitmap_page` marker for the sealed page

## Open-Page Markers

`open_bitmap_page` rows with partition `<shard>` and clustering `<page_start_local>/<stream_id>` serve two purposes:

### During ingest

When a stream fragment is written to a page for the first time in a batch, an open-page marker is created. After the batch completes, markers for pages that remain open (not sealed by the batch) are retained.

## Important Boundaries

- `api.rs` startup: re-authorizes cached writer or acquires ownership, then derives the next write position from the published head
- `family.rs`: generic startup/ingest boundary between the concrete API and family-owned logic
- `ingest/engine.rs`: generic publication orchestration from current head to new tail for a selected family
- `logs/family.rs`: logs-specific startup recovery and finalized ingest sequencing
- `logs/ingest.rs`: owns directory/stream fragment publication plus eager compaction
- `publication_state.indexed_finalized_head` is published last

See [write-authority.md](write-authority.md) for the lease and publication model.
