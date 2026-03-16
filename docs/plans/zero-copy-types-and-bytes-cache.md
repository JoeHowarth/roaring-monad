# Zero-Copy Immutable Bytes Cache

## Summary

This document is the active caching plan for `crates/finalized-history-query`.

The intended end state is:

- cache only immutable read-path artifacts
- keep `publication_state` and `open_stream_page/*` out of the cache
- store cached values as raw `Bytes`, not typed decoded objects
- construct zero-copy reference views (`LogRef`, `BlockLogHeaderRef`, `LogDirectoryBucketRef`)
  on demand from cached bytes
- keep the public query boundary as `QueryPage<Log>` while using zero-copy types internally
- treat miss deduplication, eager population, compression, and arena allocation as follow-up work,
  not requirements for the base design

This plan supersedes
`docs/plans/superceded/metadata-caching-architecture.md`.

## Why This Is The Right Scope

The current architecture has a very small amount of shared mutable state:

- `publication_state`
- `open_stream_page/*`
- backend fence state where applicable

The read path depends on `publication_state` to clip queries to the published head, but it does not
read `open_stream_page/*`.

That leads to a simple caching rule:

- cache immutable artifacts that are safe to reuse indefinitely until eviction
- do not cache mutable control-plane or inventory state

This avoids the entire invalidation/refresh problem that dominated the earlier metadata-caching
design.

## Design Decisions

### 1. Cache only immutable artifacts

The cache is for immutable read artifacts only.

Initial cache candidates:

- `block_log_headers/<block_num>`
- `log_dir_sub/<sub_bucket_start>`
- optional `log_dir/<bucket_start>`
- `block_logs/<block_num>` or per-log slices derived from them
- optionally sealed `stream_page_meta/*` and `stream_page_blob/*`

Explicit non-candidates:

- `publication_state`
- `open_stream_page/*`
- any future mutable writer-local or control-plane state

### 2. Keep the cache below the query API

The query surface remains cache-transparent.

The executor and service should not expose cache-specific behavior. The cache belongs at the
typed materialization / storage boundary, where callers already know:

- which key to read
- which table the object belongs to
- how to validate bytes into a zero-copy view

### 3. Cache raw `Bytes`, not decoded owned structs

The cache stores raw `Bytes` values keyed by `(table, key)`.

Why:

- `Bytes::clone()` is cheap
- the cache stays type-unaware
- zero-copy ref types can validate and read directly from cached bytes
- compression and alternative backing allocation strategies can remain cache-internal details

The cache should not store deep-cloned owned values like `BlockLogHeader` or `LogDirectoryBucket`.

### 4. Use zero-copy ref types internally, not at the public API boundary

Internal query execution should use zero-copy types where they remove avoidable allocation:

- `LogRef`
- `BlockLogHeaderRef`
- `LogDirectoryBucketRef`

The public query result should remain `QueryPage<Log>`.

That keeps the external boundary stable while still removing most allocation from the internal read
path. Converting `LogRef -> Log` at the final API boundary is an acceptable and explicit cost.

### 5. Start simple

The base cache should provide:

- `get`
- `put`
- logical table IDs
- caller-supplied weight

The base design does not require:

- miss deduplication
- eager population on ingest writes
- cross-request invalidation
- mutable cache semantics

Those can be added later if profiling shows they are worth the complexity.

## Current State

The current branch has already landed part of this plan:

- `BytesCache` exists
- `HashMapBytesCache` exists as the first implementation
- `LogRef`, `BlockLogHeaderRef`, and `LogDirectoryBucketRef` exist
- `LogMaterializer` already uses `BytesCache` and zero-copy ref types
- the public query boundary still returns owned `Log`, which is the intended design

So this document should be read as:

- the coherent architecture we are carrying forward
- a description of what has already landed
- the remaining follow-up work needed to finish and harden it

## Cache Shape

Conceptually:

```rust
pub trait BytesCache: Send + Sync {
    fn get(&self, table: TableId, key: &[u8]) -> Option<Bytes>;
    fn put(&self, table: TableId, key: &[u8], value: Bytes, weight: usize);
}
```

Recommended logical tables:

- `BlockLogHeaders`
- `LogDirectorySubBuckets`
- `LogDirectoryBuckets`
- `BlockLogBlobs`
- future: sealed `StreamPages`

The cache implementation remains free to choose its own internal structure.

For the carried-forward design, the important properties are:

- table-aware capacity policy
- exact byte weight accounting
- immutable-entry-only semantics

## Zero-Copy Types

The internal read path should prefer zero-copy views for high-traffic, variable-sized data.

### Keep zero-copy

- `LogRef`
- `BlockLogHeaderRef`
- `LogDirectoryBucketRef`

### Keep owned

These should remain normal owned decoded structs:

- `PublicationState`
- `BlockMeta`
- `LogDirFragment`
- `StreamBitmapMeta`

Reason:

- they are fixed-size or cheap to decode
- they are not where clone/allocation pressure is concentrated
- in the case of `PublicationState`, we do not want it in the cache at all

## Explicit Exclusions

### `publication_state`

Readers load `publication_state` on the query path to determine the visible finalized head.

This record is:

- mutable
- correctness-critical
- tiny and cheap to decode

It should not be part of the normal cross-request cache.

If a future optimization wants to memoize it, that should be treated as a special freshness-aware
optimization, not part of the immutable artifact cache.

### `open_stream_page/*`

These markers are write/recovery inventory only.

The query path does not read them. They exist to:

- discover newly sealed pages
- repair stale markers at startup

They should not be cache targets for the query cache.

## Follow-Up Work

### Phase 1: finish the immutable bytes cache rollout

- keep using zero-copy ref types internally
- finish wiring cache usage through the relevant immutable readers
- align docs and tests with the current `QueryPage<Log>` public boundary

### Phase 2: add bounded cache policy

The current `HashMapBytesCache` is only a simple starting point.

The next practical step is:

- per-table capacity budgets
- eviction
- metrics

This is more important than miss deduplication.

### Phase 3: expand coverage selectively

After profiling, consider adding cache coverage for:

- `block_logs/*`
- sealed `stream_page_*`

Only add these where measured reuse justifies the memory pressure.

### Phase 4: optional miss deduplication

Only pursue this if redundant concurrent fetches show up in profiling.

It is a useful optimization, but it should not be treated as foundational.

### Phase 5: optional compression / alternate backing allocation

Compression and arena allocation are both compatible with the bytes-only cache shape, but they are
follow-up optimizations.

They should be justified by profiling before they become committed architectural requirements.

In particular, per-log compression of `block_logs/*` is not part of the base caching plan. It is a
separate storage optimization decision and should remain optional until measured evidence says it is
worth the format churn.

## Non-Goals

- caching mutable control-plane state
- changing query semantics or pagination semantics
- changing the public transport-free query boundary to `QueryPage<LogRef>`
- requiring miss deduplication in the first usable cache
- coupling correctness to cache residency

## Relationship To Other Plans

- This plan supersedes
  `docs/plans/superceded/metadata-caching-architecture.md`.
- It aligns with the current immutable-frontier architecture in
  `docs/plans/completed/immutable-frontier-index-architecture.md`.
- It is compatible with the current zero-copy materialization work already landed in
  `crates/finalized-history-query/src/cache/` and
  `crates/finalized-history-query/src/codec/log_ref.rs`.
