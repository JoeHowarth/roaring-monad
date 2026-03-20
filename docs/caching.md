# Caching

This document describes the immutable artifact-table and bytes-cache design.

## Design Principle

Query code reads immutable artifacts through typed table readers. Each table reader owns:

- backend access for that artifact family
- its own bytes cache instance
- key construction
- decode/load behavior

The underlying cache still stores raw `Bytes` values, not decoded structs. This keeps the cache storage type-unaware while allowing zero-copy ref types to validate and read directly from cached bytes. `Bytes::clone()` is an Arc increment, making cache hits cheap.

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
| `BlockLogHeaders`        | `block_log_header/<block_num>` — byte offset tables                  |
| `DirBuckets`    | `log_dir_bucket/<bucket_start>` — 1M compacted directory buckets             |
| `LogDirSubBuckets` | `log_dir_sub_bucket/<sub_bucket_start>` — 10K compacted sub-buckets          |
| `PointLogPayloads`       | Per-log byte slices derived from `block_log_blob/<block_num>` range reads |
| `BitmapPageMeta`         | Sealed `bitmap_page_meta/<stream_id>/<page_start>`                    |
| `BitmapPageBlobs`        | Sealed `bitmap_page_blob/<stream_id>/<page_start>`                    |


A `max_bytes = 0` budget disables that table's cache entirely. The typed table reader still works, but it reads directly from the backing store with no cache lookup/insert overhead. See [storage-model.md](storage-model.md) for the artifact key layout.

## Zero-Copy Ref Types

Internal query execution uses zero-copy views to avoid allocation on the hot path:

- `LogRef` — reference view over cached log payload bytes
- `BlockLogHeaderRef` — reference view over cached block header bytes
- `DirBucketRef` — reference view over cached directory bucket bytes

The public query boundary remains `QueryPage<Log>` (owned). The `LogRef -> Log` conversion happens at the API boundary.

## Exclusions

### `publication_state`

Mutable, correctness-critical, tiny. Not part of the immutable artifact cache. Loaded directly on each query to determine the visible finalized head.

### `open_bitmap_page/*`

Write/recovery inventory only. The query path does not read these markers.

## Eviction

Each enabled table uses an independent `quick_cache` instance with weighted,
scan-resistant eviction rather than exact LRU. Per-table metrics track hits,
misses, inserts, evictions, and resident bytes.

## Deferred Scope

- miss deduplication (concurrent fetches for the same key)
- eager cache population on ingest writes
- compression or alternate backing allocation inside the cache
- payload-only stored log encoding (removing duplicated `block_num`/`block_hash`
  from stored log bytes)
