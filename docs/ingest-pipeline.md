# Ingest Pipeline

This document describes the block ingestion flow, artifact writes, compaction triggers, and recovery behavior.

## Block Ingestion Flow

```python
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

The `IngestEngine` orchestrates this flow. It validates finalized sequencing and parent hashes, then delegates artifact writes to the logs family.

## Artifact Write Order

For each block in the batch:

1. **Log blob** — `block_logs/<block_num>`: concatenated encoded log bytes
2. **Block log header** — `block_log_headers/<block_num>`: byte offset table for local ordinals
3. **Block meta** — `block_meta/<block_num>`: `{ block_hash, parent_hash, first_log_id, count }`
4. **Block hash index** — `block_hash_to_num/<block_hash>`: reverse lookup
5. **Directory fragments** — one immutable `log_dir_frag/<sub_bucket_start>/<block_num>` per covered sub-bucket
6. **Stream fragments** — `stream_frag_meta/...` and `stream_frag_blob/...` per stream per page touched

All artifacts must be durable before the head advance — see the publication ordering invariant in [storage-model.md](storage-model.md).

## Directory Compaction

After all blocks in the batch are persisted, sealed directory boundaries are compacted:

- **Sub-bucket compaction**: when `next_log_id` crosses a sub-bucket boundary (10,000 `log_id` range), the sealed sub-bucket's fragments are compacted into `log_dir_sub/<sub_bucket_start>`
- **Bucket compaction**: when `next_log_id` crosses a 1M-bucket boundary, the sealed sub-buckets are optionally compacted into `log_dir/<bucket_start>`

See [storage-model.md](storage-model.md) for directory layout details.

## Stream Page Compaction

When `next_log_id` crosses a stream page boundary (see [storage-model.md](storage-model.md) for page span), the sealed page's fragments are compacted:

1. load all `stream_frag_meta` and `stream_frag_blob` entries for the page
2. merge the roaring bitmaps
3. write compacted `stream_page_meta` and `stream_page_blob`
4. delete the `open_stream_page` marker for the sealed page

## Open-Page Markers

`open_stream_page/<shard>/<page_start>/<stream_id>` markers serve two purposes:

### During ingest

When a stream fragment is written to a page for the first time in a batch, an open-page marker is created. After the batch completes, markers for pages that remain open (not sealed by the batch) are retained.

### During startup repair

At startup, stale open-page markers may exist from interrupted ingest. The repair process:

1. list all `open_stream_page/*` markers
2. for each marker, check whether the page is actually sealed (the current `next_log_id` is past the page boundary)
3. if sealed, compact the page and remove the marker
4. if not sealed, the marker is valid and kept

## Recovery: cleanup_unpublished_suffix

When a writer acquires ownership (fresh or after takeover), it cleans any artifacts above the published head that may have been written by a previous writer that failed before publishing:

- deletes `block_meta/<block_num>` for blocks above `indexed_finalized_head`
- deletes `block_log_headers/<block_num>` for those blocks
- deletes `block_logs/<block_num>` blobs for those blocks
- deletes `block_hash_to_num/<hash>` entries for those blocks
- deletes directory fragments (`log_dir_frag/...`) for those blocks
- deletes stream fragments (`stream_frag_meta/...`, `stream_frag_blob/...`) for those blocks

This runs before any new ingest, ensuring the write position is clean.

## Important Boundaries

- `api/service.rs` startup: re-authorizes cached writer or acquires ownership, then runs cleanup-first recovery
- `ingest/engine.rs`: validates finalized sequencing, orchestrates publication from current head to new tail
- `logs/ingest.rs`: owns directory/stream fragment publication plus eager compaction
- `publication_state.indexed_finalized_head` is published last

See [write-authority.md](write-authority.md) for the lease and fencing model.
