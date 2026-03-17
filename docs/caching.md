# Caching

This document describes the immutable bytes cache design.

## Design Principle

The cache stores raw `Bytes` values, not decoded structs. This keeps the cache type-unaware while allowing zero-copy ref types to validate and read directly from cached bytes. `Bytes::clone()` is an Arc increment, making cache hits cheap.

## Cache Trait

```rust
pub trait BytesCache: Send + Sync {
    fn is_enabled(&self, table: TableId) -> bool;
    fn get(&self, table: TableId, key: &[u8]) -> Option<Bytes>;
    fn put(&self, table: TableId, key: &[u8], value: Bytes, weight: usize);
}
```

The cache sits below the query API — the executor and service do not expose cache-specific behavior.

## Per-Table Byte Budgets

Each table has an independent byte budget configured via `BytesCacheConfig`:

| Table | What it caches |
|-------|---------------|
| `BlockLogHeaders` | `block_log_headers/<block_num>` — byte offset tables |
| `LogDirectoryBuckets` | `log_dir/<bucket_start>` — 1M compacted directory buckets |
| `LogDirectorySubBuckets` | `log_dir_sub/<sub_bucket_start>` — 10K compacted sub-buckets |
| `PointLogPayloads` | Per-log byte slices derived from `block_logs/<block_num>` range reads |
| `StreamPageMeta` | Sealed `stream_page_meta/<stream_id>/<page_start>` |
| `StreamPageBlobs` | Sealed `stream_page_blob/<stream_id>/<page_start>` |

A `max_bytes = 0` budget disables the table entirely — the cache is bypassed with no lookup/insert overhead.

## Zero-Copy Ref Types

Internal query execution uses zero-copy views to avoid allocation on the hot path:

- `LogRef` — reference view over cached log payload bytes
- `BlockLogHeaderRef` — reference view over cached block header bytes
- `LogDirectoryBucketRef` — reference view over cached directory bucket bytes

The public query boundary remains `QueryPage<Log>` (owned). The `LogRef -> Log` conversion happens at the API boundary.

## Exclusions

### `publication_state`

Mutable, correctness-critical, tiny. Not part of the immutable artifact cache. Loaded directly on each query to determine the visible finalized head.

### `open_stream_page/*`

Write/recovery inventory only. The query path does not read these markers.

## Eviction

LRU eviction within each enabled table. Per-table metrics track hits, misses, inserts, evictions, and resident bytes.

## Not Yet Implemented

- miss deduplication (concurrent fetches for the same key)
- eager cache population on ingest writes
- compression or alternate backing allocation inside the cache
- payload-only stored log encoding (removing duplicated `block_num`/`block_hash` from stored log bytes)
