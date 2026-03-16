# Zero-Copy Immutable Bytes Cache

## Summary

This document is the active caching plan for `crates/finalized-history-query`.

The intended end state is:

- cache only immutable read-path artifacts
- keep `publication_state` and `open_stream_page/*` out of the cache
- store cached values as raw `Bytes`, not typed decoded objects
- configure each cache table independently with a byte budget
- treat `size = 0` for a table as "disabled" and bypass the cache for that table entirely
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

Immutable tables that should participate from the first rollout:

- `block_log_headers/<block_num>`
- `log_dir_sub/<sub_bucket_start>`
- optional `log_dir/<bucket_start>`
- `block_logs/<block_num>` or per-log slices derived from them
- sealed `stream_page_meta/*` and `stream_page_blob/*`

These all get cache plumbing from the start.

Whether a given table is active at runtime is controlled only by its configured size budget:

- non-zero budget: table is enabled and participates in LRU caching
- zero budget: table is bypassed entirely

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

### 3a. Every table gets an explicit size budget

Each cache table, whether it fronts metadata or blob-backed reads, should have an explicit size
budget configured in bytes.

Operationally these values will typically be set in MB or GB, but the cache should normalize them
to byte budgets internally.

Required behavior:

- each table has its own byte budget
- eviction is LRU within that table
- `size = 0` means the table is disabled
- a disabled table should be bypassed entirely rather than paying lookup/insert overhead

This applies equally to:

- metadata-backed tables such as block headers and directory summaries
- blob-backed tables such as block-log payload bytes or future sealed stream-page blobs

### 4a. Fold payload dedup into the same read-path optimization track

The next storage-level optimization in this same area is to remove duplicated block context from the
persisted per-log payload bytes.

That means moving from:

- stored log bytes containing `block_num` and `block_hash`

to:

- stored payload bytes containing only per-log fields
- materialization injecting `block_num` from the resolved block location
- materialization injecting `block_hash` from block metadata

This work belongs under the same carried-forward plan because it serves the same read-path goals:

- smaller stored payloads
- fewer bytes read per point materialization
- cleaner separation between block-scoped context and per-log payload

It should keep the public `Log` type unchanged.

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
- per-table byte-budget configuration
- per-table LRU eviction
- a fast disabled-table path when configured size is `0`

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
pub struct TableCacheConfig {
    pub max_bytes: u64,
}

pub trait BytesCache: Send + Sync {
    fn get(&self, table: TableId, key: &[u8]) -> Option<Bytes>;
    fn put(&self, table: TableId, key: &[u8], value: Bytes, weight: usize);
}
```

`max_bytes == 0` means the table is disabled.

Recommended logical tables:

- `BlockLogHeaders`
- `LogDirectorySubBuckets`
- `LogDirectoryBuckets`
- `PointLogPayloads`
- `StreamPageMeta`
- `StreamPageBlobs`

All immutable read tables should be represented in the cache configuration and implementation from
the start, even if some are configured with `max_bytes = 0` initially.

The cache implementation remains free to choose its own internal structure.

For the carried-forward design, the important properties are:

- table-aware capacity policy
- per-table enable/disable via byte budget
- LRU eviction within each enabled table
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
- `size = 0` bypass behavior
- LRU eviction
- metrics

This is more important than miss deduplication.

### Phase 3: tune per-table budgets

After profiling, tune the per-table budgets rather than adding cache coverage to new immutable
tables later.

The intended rollout already includes all immutable read tables. The operational question is:

- which tables should have non-zero budgets in a given deployment
- how large those budgets should be relative to one another

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

### Phase 6: remove duplicated block context from stored log payloads

After the cache/materialization direction is settled, the next clean storage optimization is:

- introduce a payload-only stored log encoding
- stop persisting `block_num` and `block_hash` inside each encoded log
- inject block context during materialization

That is a local storage-format change with no intended query-semantics change.

## Non-Goals

- caching mutable control-plane state
- changing query semantics or pagination semantics
- changing the public transport-free query boundary to `QueryPage<LogRef>`
- requiring miss deduplication in the first usable cache
- coupling correctness to cache residency

## Relationship To Other Plans

- This plan supersedes
  `docs/plans/superceded/metadata-caching-architecture.md`.
- It also absorbs the forward-looking work from
  `docs/plans/superceded/log-payload-block-context-dedup-plan.md`.
- It aligns with the current immutable-frontier architecture in
  `docs/plans/completed/immutable-frontier-index-architecture.md`.
- It is compatible with the current zero-copy materialization work already landed in
  `crates/finalized-history-query/src/cache/` and
  `crates/finalized-history-query/src/codec/log_ref.rs`.

## Implementation Status

This section reflects the current state of the codebase.

### Implemented

- internal zero-copy read types are in use for:
  - `LogRef`
  - `BlockLogHeaderRef`
  - `LogDirectoryBucketRef`
- the public query boundary still returns `QueryPage<Log>`
- the immutable bytes cache is bytes-only rather than storing decoded owned structs
- cache tables have explicit per-table byte budgets
- `max_bytes = 0` disables a table and bypasses cache lookup/insert work
- cache eviction is LRU within each enabled table
- cache metrics are available per table for hits, misses, inserts, evictions, and resident bytes
- the normal `FinalizedHistoryService` query path uses a service-owned bytes cache
- immutable read-path cache coverage currently includes:
  - `block_log_headers/<block_num>`
  - `log_dir_sub/<sub_bucket_start>`
  - optional `log_dir/<bucket_start>`
  - point log payload bytes derived from `block_logs/<block_num>` range reads
  - sealed `stream_page_meta/*`
  - sealed `stream_page_blob/*`
- `publication_state` is not part of the normal immutable query cache
- `open_stream_page/*` is not part of the query cache

### Not Yet Implemented

- miss deduplication
- eager cache population on ingest writes
- compression or alternate backing allocation inside the cache
- payload-only stored log encoding
- removal of persisted per-log `block_num` and `block_hash`
- materialization that injects block context into payload-only log bytes
- operational tuning guidance for real deployed cache budgets beyond the current basic benchmark notes

### Notes

- The bounded cache rollout and main immutable-reader wiring are implemented.
- The document still includes forward work that has not landed yet, especially metrics and the
  payload-only log storage follow-up.
