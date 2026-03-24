# Typed Artifact Tables

## Goal

Replace the current raw `meta_store` + `blob_store` + cache plumbing with a
typed artifact-table layer that owns:

- backend access for that artifact family
- per-table cache policy and metrics
- key construction
- decode/load behavior

The main objective is to make cache usage automatic and local to the artifact
being loaded, rather than something query code must remember to wire manually.

## Problem

The current cache system is poorly encapsulated.

Today, query and materialization code frequently receives some combination of:

- `meta_store`
- `blob_store`
- `cache`

and is responsible for deciding:

- which backend to read from
- whether the artifact should be cached
- which cache table to use
- how to decode the artifact after load

This creates two problems:

1. cache usage is easy to forget at call sites
2. cache and storage policy leak into query logic that should only care about
   domain operations

The current single cache object also places unrelated tables behind a shared
mutex even though their budgets and eviction domains are already separate in
concept.

## Intended End State

The crate exposes a typed `Tables` facade for immutable query artifacts.

Each artifact family gets its own typed reader with its own cache instance.
Call sites depend on those typed readers instead of threading raw stores and a
generic cache through the stack.

Representative shape:

```rust
pub struct Tables<M, B> {
    pub block_log_headers: BlockLogHeaderTable<M>,
    pub dir_buckets: DirBucketTable<M>,
    pub log_dir_sub_buckets: LogDirSubBucketTable<M>,
    pub point_log_payloads: PointLogPayloadTable<M, B>,
    pub bitmap_page_meta: BitmapPageMetaTable<M>,
    pub bitmap_page_blobs: BitmapPageBlobTable<B>,
}
```

Representative call sites:

- `tables.block_log_headers.get(block_num)`
- `tables.dir_buckets.get(bucket_start)`
- `tables.point_log_payloads.load_contiguous_run(block_num, start, end)`
- `tables.bitmap_page_meta.get(stream, page_start)`

The table type, not the caller, owns the questions of:

- store kind
- cacheability
- cache key layout
- decode shape
- per-table metrics

## Scope

This plan covers immutable artifact reads used by query execution and
materialization.

Initial artifact families:

- block log headers
- directory buckets
- log directory sub-buckets
- point log payloads
- bitmap page metadata
- bitmap page blobs

This plan does not try to turn every local memoization into a shared table.
Request-local assembled caches remain local when they are not a single stored
artifact.

Examples that should remain request-local:

- directory fragment assembly caches
- block-ref memoization

## Design Principles

### 1. Encapsulate Store And Cache Together

If an artifact family is cacheable, its cache should live with the code that
knows how to load and decode that artifact.

The caller should not need to know:

- whether the data comes from meta or blob storage
- whether it is cached
- how keys are formed

### 2. Use One Cache Instance Per Artifact Family

Each typed table should own an independent cache instance and lock.

This preserves:

- per-table byte budgets
- per-table metrics
- per-table eviction domains

while removing cross-table lock contention.

### 3. Prefer Typed APIs Over Generic Table Selectors

The main abstraction boundary should be typed table readers, not a more generic
`TableId`-driven API.

`TableId` may still remain useful for:

- config naming
- metrics labeling
- transitional internal wiring

but query code should not select cache behavior by `TableId`.

### 4. Keep Request-Local Assembly Separate

Not all repeated work is a shared immutable artifact.

If a value is assembled from multiple backend operations and only useful within
one request, it should stay as request-local memoization rather than being
forced into the shared tables layer.

## Work Packages

### 1. Introduce The `Tables` Facade

Add a new module that defines:

- `Tables<M, B>`
- one typed reader per immutable artifact family
- a cache-metrics snapshot API that preserves current observability shape

During this phase, existing query code may keep using adapters while the new
types are introduced.

### 2. Split The Shared Cache Into Per-Table Caches

Replace the current single cache object with one cache instance per typed table.

Keep the existing config shape stable:

- `block_log_header`
- `log_dir_buckets`
- `log_dir_sub_buckets`
- `point_log_payloads`
- `bitmap_page_meta`
- `bitmap_page_blobs`

Keep the existing metrics field names stable as well so tests and observability
do not need a simultaneous semantic migration.

### 3. Move Single-Key Immutable Reads Behind Typed Tables

Move the following read logic out of query/materialization call sites and into
typed table readers:

- block log header load + cache + decode
- directory bucket load + cache + decode
- log directory sub-bucket load + cache + decode
- bitmap page meta load + cache + decode
- bitmap page blob load + cache + decode

After this step, these call sites should no longer manually perform:

- `cache.get(...)`
- backend read
- `cache.put(...)`
- decode

for those artifact families.

### 4. Move Point Log Payload Loading Behind A Typed Table

Point log payloads need a slightly richer abstraction than single-key metadata.

The typed table should own:

- the payload cache
- block header usage required to compute ranges
- contiguous-run range reads from the blob store
- per-log slicing and insertion into the payload cache

This table should expose a domain-level API such as:

- `load_contiguous_run(block_num, start_local_ordinal, end_local_ordinal_inclusive)`

so materialization code no longer manually coordinates headers, range reads, and
payload cache inserts.

### 5. Refactor Query And Materialization To Depend On Typed Tables

Change query execution and materialization code to accept the `Tables` facade or
the specific typed table readers they need.

Priority migration targets:

- log materialization
- directory resolution
- bitmap-page loading
- clause preparation that currently depends on bitmap metadata caching
- service query wiring

The desired outcome is that raw cache objects are no longer threaded through the
query stack.

### 6. Preserve And Clarify Request-Local Memoization

Keep request-local caches where they are still appropriate, but make the
boundary explicit in code comments and constructors.

Examples:

- directory fragments remain per-request because they are assembled from
  `list_prefix` plus multiple record loads
- block refs remain per-request because they are small derived values rather
  than direct immutable stored bytes

### 7. Remove Obsolete Raw Cache Plumbing

After the call sites are migrated:

- remove generic `query_logs_with_cache(...)`-style entry points
- remove `NoopBytesCache`-based fallback plumbing from query code
- remove `TableId` selection from hot-path callers
- collapse any temporary adapters introduced during the transition

At the end of this phase, typed tables are the only supported path for cached
immutable artifact reads.

## Rollout Strategy

Implement this refactor in two major commits.

### Commit 1: Infrastructure

Add the typed tables layer and per-table caches without fully deleting the old
call paths.

This commit should:

- introduce `Tables<M, B>`
- create per-table cache instances
- preserve current config and metrics shapes
- add transitional adapters where needed

### Commit 2: Call-Site Migration

Move query and materialization code onto the typed tables layer, update tests,
and remove obsolete raw cache plumbing.

This commit should:

- migrate log materialization
- migrate bitmap metadata/blob loading
- migrate directory bucket loading
- update service construction and query entry points
- remove the old raw cache threading

## Risks And Guardrails

### Risk: Mixing Old And New Access Paths

If one artifact family is available through both the new typed table and the old
raw cache/store path for too long, usage may diverge and the refactor will lose
its architectural value.

Guardrail:

- once a typed table exists for an artifact family, new code should not add more
  raw store-plus-cache accesses for that family

### Risk: Forcing Non-Artifact Memoization Into Shared Tables

Some current local caches are not actually reusable immutable tables.

Guardrail:

- keep request-local assembled caches local unless they clearly represent one
  persisted artifact with a stable key and decode shape

### Risk: Metrics Churn During Refactor

Changing metrics shape at the same time as the architecture will make it harder
to compare behavior before and after the migration.

Guardrail:

- preserve current cache metrics names and semantics through the transition

## Exit Criteria

- query and materialization code no longer thread raw cache objects through the
  stack
- immutable artifact reads are performed through typed table readers
- each artifact family owns its backend access, decode behavior, and per-table
  cache
- cross-table cache lock contention is removed by independent per-table caches
- request-local assembled caches remain separate from shared immutable table
  readers
- current-state docs can describe caching in terms of typed artifact tables
  rather than ad hoc call-site cache usage
