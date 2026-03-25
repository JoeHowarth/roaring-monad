# Caching

Read this after the main ingest/query docs.

This doc describes the immutable artifact-table and bytes-cache design.

The shared cache implementation, cache metrics types, and generic cache-backed
table helpers live under `src/kernel/*`. `src/tables.rs` assembles those shared
primitives into the crate's typed family-facing storage surface.

## Design Principle

Query, status, and ingest share one long-lived `Runtime<M, B>` that owns typed
table readers over cheap-clone store handles. Each table reader owns:

- backend access for that artifact table
- its own bytes cache instance
- key construction
- decode/load behavior

The underlying cache still stores raw `Bytes` values, not decoded structs. This keeps the cache storage type-unaware while allowing zero-copy ref types to validate and read directly from cached bytes. `Bytes::clone()` is an Arc increment, making cache hits cheap.

Ingest writes are write-through for the typed artifact surfaces owned by `Tables`.
When a block is ingested, the just-written immutable artifacts are immediately
seeded into the corresponding per-table cache so recency-biased reads do not
need to miss once before warming.

## Tables

```rust
pub struct Tables<M, B> {
    // accessor methods omitted
}
```

Hot-path callers do not manually select a cache table or backend. They call
the typed table reader for the artifact they need, such as
`tables.block_log_headers().get(block_num)`. See [config.md](config.md) for
`BytesCacheConfig`.

## Per-Table Byte Budgets

Each typed table has an independent byte budget configured via `BytesCacheConfig`:


| Table                    | What it caches                                                        |
| ------------------------ | --------------------------------------------------------------------- |
| `BlockRecords`           | `block_record` table, key `<block_num>` — immutable indexed-family primary windows plus shared block identity |
| `BlockLogHeaders`        | `block_log_header` table, key `<block_num>` — byte offset tables |
| `DirBuckets`             | `log_dir_bucket` table, key `<bucket_start>` — 1M compacted directory buckets |
| `LogDirSubBuckets`       | `log_dir_sub_bucket` table, key `<sub_bucket_start>` — 10K compacted sub-buckets |
| `BlockLogBlobs`          | Per-log byte slices derived from `block_log_blob` blob-table range reads keyed by `<block_num>` |
| `BlockTxBlobs`           | Per-tx envelope byte slices derived from `block_tx_blob` blob-table range reads keyed by `<block_num>` |
| `BlockTraceBlobs`        | Per-trace frame byte slices derived from `block_trace_blob` blob-table range reads keyed by `<block_num>` |
| `BitmapPageMeta`         | `bitmap_page_meta` table, key `<stream_id>/<page_start>` |
| `BitmapPageBlobs`        | `bitmap_page_blob` blob table, key `<stream_id>/<page_start>` |


A `max_bytes = 0` budget disables that table's cache entirely. The typed table reader still works, but it reads directly from the backing store with no cache lookup/insert overhead. See [storage-model.md](storage-model.md) for the artifact key layout.

## Zero-Copy Ref Types

Internal query execution uses zero-copy views to avoid allocation on the hot path:

- `LogRef` — reference view over cached log payload bytes
- `TxRef` — reference view over cached tx envelope bytes
- `TraceRef` — reference view over cached trace frame bytes
- `BlockLogHeaderRef` — reference view over cached block header bytes
- `DirBucketRef` — reference view over cached directory bucket bytes

The public query boundary is also zero-copy for indexed families:

- `QueryPage<LogRef>`
- `QueryPage<TxRef>`
- `QueryPage<TraceRef>`

## Exclusions

### `publication_state`

Mutable, correctness-critical, tiny. Not part of the immutable artifact cache. Loaded directly on each query to determine the visible finalized head.

### `open_bitmap_page`

Write/recovery inventory only. The query path does not read these rows.

## Eviction

Each enabled table uses an independent `quick_cache` instance with weighted,
scan-resistant eviction rather than exact LRU. Per-table metrics track hits,
misses, inserts, evictions, and resident bytes.

## Deferred Scope

- miss deduplication (concurrent fetches for the same key)
- compression or alternate backing allocation inside the cache
- payload-only stored log encoding (removing duplicated `block_num`/`block_hash`
  from stored log bytes)
