# Ingest Pipeline

This document describes the block ingestion flow, artifact writes, compaction triggers, and ingest-time recovery behavior.

## Block Ingestion Flow

```python
async def ingest_finalized_blocks(blocks, lease):
    prepared = preflight_writer_state(lease)
    validate_contiguous_finalized_sequence_and_parent(blocks, prepared.indexed_finalized_head)
    for block in blocks:
        await logs.ingest_block(block, prepared.family_states.logs)
        await txs.ingest_block(block, prepared.family_states.txs)
        await traces.ingest_block(block, prepared.family_states.traces)

    await compare_and_set_publication_state(
        expected=lease,
        next_head=blocks[-1].block_num,
    )
```

The `IngestEngine` orchestrates this flow. It owns write-session acquisition, writer preflight, finalized sequencing validation, recovery-only repair of stale sealed open-page markers, publication, and the shared block loop. The service owns one concrete `Families { logs, txs, traces }` registry, and those family handlers derive sequencing state from the published head before ingesting one shared `FinalizedBlock` at a time.

Shared ingest helpers under `src/ingest/` now own the generic primary-directory and bitmap-page mechanics. Family adapters supply payload-specific block artifacts, stream fanout values, and any family-only behavior such as logs open-page markers.

The current family set is:

- logs: fully implemented and persists artifacts
- txs: state scaffold plus ingest slot, currently rejects non-empty tx payloads
- traces: fully implemented and persists trace artifacts plus trace indexes

## Artifact Write Order

For each block in the batch:

1. **Logs family ingest** — logs artifacts are written through the shared runtime
2. **Txs family ingest** — reserved slot in the coordinator; currently accepts only empty tx payloads
3. **Traces family ingest** — trace artifacts, directory fragments, and stream fragments are written through the shared runtime

Within the logs family step, artifact writes remain:

1. **Log blob** — `block_log_blob` blob table, key `<block_num>`: concatenated encoded log bytes
2. **Block log header** — `block_log_header` table, key `<block_num>`: byte offset table for local ordinals
3. **Block meta** — `block_record` table, key `<block_num>`: merge the logs `PrimaryWindowRecord { first_primary_id, count }` into the shared `{ block_hash, parent_hash, logs, traces }` record
4. **Block hash index** — `block_hash_index` table, key `<block_hash>`: reverse lookup
5. **Directory fragments** — one `log_dir_by_block` row per covered sub-bucket, keyed by partition `<sub_bucket_start>` and clustering `<block_num>`
6. **Stream fragments** — `bitmap_by_block` rows per stream per page touched

Within the traces family step, artifact writes are:

1. **Trace blob** — `block_trace_blob` blob table, key `<block_num>`: raw per-block `trace_rlp`
2. **Block trace header** — `block_trace_header` table, key `<block_num>`: compact trace header with offsets and tx starts
3. **Block meta** — `block_record` table, key `<block_num>`: merge the traces `PrimaryWindowRecord { first_primary_id, count }` into the shared `{ block_hash, parent_hash, logs, traces }` record
4. **Directory fragments** — one `trace_dir_by_block` row per covered sub-bucket, keyed by partition `<trace_sub_bucket_start>` and clustering `<block_num>`
5. **Stream fragments** — `trace_bitmap_by_block` rows per stream per page touched

All family artifacts for the block batch must be durable before the head advance — see the publication ordering invariant in [storage-model.md](storage-model.md). Artifact writes are unconditional; publication remains the only visibility boundary. Writes flow through the service-owned typed `Tables` runtime so ingest also warms the same per-table caches that queries read.

## Directory Compaction

After all blocks in the batch are persisted, sealed directory boundaries are compacted:

- **Sub-bucket compaction**: when `next_log_id` crosses a sub-bucket boundary (10,000 `log_id` range), the sealed sub-bucket's fragments are compacted into `log_dir_sub_bucket`, key `<sub_bucket_start>`
- **Bucket compaction**: when `next_log_id` crosses a 1M-bucket boundary, the sealed sub-buckets are optionally compacted into `log_dir_bucket`, key `<bucket_start>`
- **Trace sub-bucket compaction**: when `next_trace_id` crosses a trace sub-bucket boundary, the sealed trace fragments are compacted into `trace_dir_sub_bucket`, key `<trace_sub_bucket_start>`
- **Trace bucket compaction**: when `next_trace_id` crosses a trace bucket boundary, the sealed trace sub-buckets are optionally compacted into `trace_dir_bucket`, key `<trace_bucket_start>`

The sealing walk and the compaction from fragments/sub-buckets are shared substrate logic. Logs and traces provide only the family directory layout (bucket spans, alignment functions, and table bindings).

See [storage-model.md](storage-model.md) for directory layout details.

## Stream Page Compaction

When `next_log_id` or `next_trace_id` crosses a stream page boundary (see [storage-model.md](storage-model.md) for page span), the sealed page's fragments are compacted:

1. load all `bitmap_by_block` entries for the page
2. merge the roaring bitmaps
3. write compacted `bitmap_page_meta` and `bitmap_page_blob`
4. delete the `open_bitmap_page` marker for the sealed page

The traces family follows the same pattern with `trace_bitmap_by_block`, `trace_bitmap_page_meta`, and `trace_bitmap_page_blob`.

The generic page-grouping, bitmap merge, and compacted-page write flow lives in shared ingest helpers. Family adapters are responsible for producing `(stream_id, local_id)` pairs and selecting the family-owned tables.

## Open-Page Markers

`open_bitmap_page` rows with partition `<shard>` and clustering `<page_start_local>/<stream_id>` serve two purposes:

### During ingest

When a stream fragment is written to a page for the first time in a batch, an open-page marker is created. After the batch completes, markers for pages that remain open (not sealed by the batch) are retained.

## Important Boundaries

- `api.rs`: transport-free query and ingest entrypoints
- `block.rs`: shared finalized block envelope carrying block identity plus family payloads
- `family.rs`: concrete `Families { logs, txs, traces }` registry plus shared state derivation and per-family ingest aggregation
- `runtime.rs`: shared store-handle + typed-table runtime used by query/status/ingest
- `ingest/engine.rs`: generic writer preflight and publication orchestration from current head to new tail for the shared finalized block envelope
- `ingest/primary_dir.rs`: shared primary-directory fragment persistence and sealed-boundary compaction
- `ingest/bitmap_pages.rs`: shared stream-page fragment persistence and compacted-page writes
- `logs/family.rs`: logs-specific sequencing-state derivation and per-block ingest handler
- `txs/family.rs`: tx-family scaffold that currently rejects non-empty tx payloads
- `traces/mod.rs`: trace-family sequencing-state derivation and per-block ingest handler
- `logs/ingest/`: logs payload encoding, logs stream fanout, and open-page marker handling
- `traces/ingest/`: trace payload encoding and trace stream fanout
- `publication_state.indexed_finalized_head` is published last

See [write-authority.md](write-authority.md) for the lease and publication model.
