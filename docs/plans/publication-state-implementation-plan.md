# Publication State Implementation Plan

This document is the concrete implementation companion to
`publication-state-architecture.md`.

The architecture document defines the invariants. This document translates those invariants into:

- exact code changes in `crates/finalized-history-query`
- the staged rollout order
- recovery and restart behavior
- the test matrix required before the protocol is considered complete

The intended end state is:

- one correctness-critical persisted publication record
- immutable authoritative artifacts
- immutable sealed summaries
- a durable `open_stream_page` marker inventory for stream-page sealing discovery
- no persisted mutable summary builders
- any future in-memory builder treated as cache only

## Goals

- implement `publication_state` as the sole correctness authority for ownership and visibility
- remove `writer_lease` from correctness and eventually from the read path
- make authoritative artifacts and sealed summaries safe under crash, retry, and takeover
- keep the stream-page sealing protocol bounded without persisting mutable builders
- preserve current query semantics

## Non-Goals

- redesigning query semantics
- redesigning the log storage model
- introducing generic multi-key transactions
- introducing persisted mutable summary builders in the first implementation

## Recommended Delivery Strategy

Implement this in six phases:

1. add the publication-state control plane and read-path compatibility
2. add immutable-create primitives for blobs and create-if-absent behavior for immutable metadata
3. refactor ingest to publish through `publication_state`
4. implement cleanup-first unpublished-suffix recovery
5. add durable `open_stream_page` markers and final stream-page summary sealing
6. remove legacy `writer_lease` handling and tighten docs/tests

Phases 1 through 4 establish the safe baseline. Phase 5 adds efficient stream-page sealing without
introducing persisted builders.

## Current Code Surface

The current implementation centers on:

- `src/ingest/engine.rs`
- `src/logs/ingest.rs`
- `src/core/state.rs`
- `src/store/traits.rs`
- `src/store/{meta,fs,scylla}.rs`
- `src/api/service.rs`
- `src/recovery/startup.rs`

The current correctness split is:

- `indexed_head` is the visible watermark
- `writer_lease` is written separately
- most artifact writes are unconditional

The new implementation replaces that with:

- `publication_state` for ownership plus visibility
- immutable block-scoped artifacts
- immutable sealed summaries
- durable open-page markers for stream-page discovery

## End-State Types

## Domain types

Add this struct to `src/domain/types.rs`:

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct PublicationState {
    pub owner_id: u64,
    pub epoch: u64,
    pub indexed_finalized_head: u64,
}
```

If a non-empty marker payload is preferred, add:

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct OpenStreamPageMarker;
```

The marker payload may also be empty. The key carries the real identity.

## Keys

Add new key helpers/constants in `src/domain/keys.rs`:

- `PUBLICATION_STATE_KEY`
- `open_stream_page_key(shard, page_start, stream_id)`
- `open_stream_page_prefix()`
- `open_stream_page_shard_prefix(shard)`
- optionally `open_stream_page_shard_page_prefix(shard, page_start)`

Recommended key layout:

- `open_stream_page/<shard>/<page_start>/<stream_id>`

The ordering matters:

- shard must come before page
- page must come before stream id
- the keyspace must support prefix scans by shard and by `(shard, page)`

Do not remove `INDEXED_HEAD_KEY` or `WRITER_LEASE_KEY` until Phase 6.

## Codecs

Add codecs in `src/codec/finalized_state.rs` for:

- `PublicationState`
- optional `OpenStreamPageMarker`

Requirements:

- include an explicit version byte for every new non-empty record
- keep decode strict about shape and length
- if marker values are empty, keep that choice consistent across backends

## New Storage Traits

## Publication store

Add a dedicated store trait in a new file such as `src/store/publication.rs`:

```rust
pub enum CasOutcome<T> {
    Applied(T),
    Failed { current: Option<T> },
}

#[allow(async_fn_in_trait)]
pub trait PublicationStore: Send + Sync {
    async fn load(&self) -> Result<Option<PublicationState>>;

    async fn create_if_absent(
        &self,
        initial: &PublicationState,
    ) -> Result<CasOutcome<PublicationState>>;

    async fn compare_and_set(
        &self,
        expected: &PublicationState,
        next: &PublicationState,
    ) -> Result<CasOutcome<PublicationState>>;
}
```

Implementation notes:

- `InMemoryMetaStore` can back this with an internal mutex and exact-value compare
- `FsMetaStore` can implement this via lock-file or single-file replace logic
- `ScyllaMetaStore` should implement this with one-row LWT
- do not try to express this through `MetaStore::put` alone

## Blob immutability capability

Extend `BlobStore` in `src/store/traits.rs`:

```rust
pub enum CreateOutcome {
    Created,
    AlreadyExists,
}

#[allow(async_fn_in_trait)]
pub trait BlobStore: Send + Sync {
    async fn put_blob(&self, key: &[u8], value: Bytes) -> Result<()>;
    async fn put_blob_if_absent(&self, key: &[u8], value: Bytes) -> Result<CreateOutcome>;
    ...
}
```

Required usage after migration:

- `block_logs/<block_num>` uses `put_blob_if_absent`
- `stream_frag_blob/*` uses `put_blob_if_absent`
- `stream_page_blob/*` uses `put_blob_if_absent`

If a backend cannot provide safe immutable create, it should return `Error::Unsupported(...)`
rather than silently falling back to overwrite.

## Open-page marker helpers

The first implementation does not need a separate `OpenSummaryStore`.

Use the existing `MetaStore` plus a helper module such as `src/ingest/open_pages.rs` or
`src/store/open_pages.rs` with functions like:

- `mark_open_stream_page_if_absent(meta_store, shard, page_start, stream_id)`
- `delete_open_stream_page(meta_store, shard, page_start, stream_id)`
- `list_open_stream_pages_for_shard(meta_store, shard, cursor, limit)`
- `list_all_open_stream_pages(meta_store, cursor, limit)`

Important design rule:

- open-page markers are durable inventory only
- they are not ownership proof
- a stale or duplicated marker may cause repair work, but must not be able to create a query-visible
  gap because sealed summaries remain authoritative

## Reader Changes

## `load_finalized_head_state`

Update `src/core/state.rs` so readers load visibility from:

1. `publication_state`, if present
2. otherwise `indexed_head`

Compatibility rules:

- during Phases 1 through 5, readers must understand both formats
- `writer_epoch` should be populated from `publication_state.epoch` when present
- `writer_lease` becomes advisory-only immediately
- if `publication_state` exists, ignore `writer_lease` for correctness

## Service surface

`FinalizedHistoryService` in `src/api/service.rs` should continue to expose:

- `query_logs`
- `ingest_finalized_block`
- `indexed_finalized_head`

No public API change is required.

## Ingest Engine Refactor

## New helper modules

Split `src/ingest/engine.rs` into explicit steps. The current file can remain the entry point, but
add helper modules to avoid one monolithic function:

- `src/ingest/publication.rs`
- `src/ingest/recovery.rs`
- `src/ingest/open_pages.rs`

The engine should evolve toward:

```rust
pub struct IngestEngine<M, B, P> {
    pub config: Config,
    pub meta_store: M,
    pub blob_store: B,
    pub publication_store: P,
}
```

## New ingest flow

The final ingest flow for one block should be:

1. load current `PublicationState`
2. verify current owner/epoch matches the writer token
3. if unpublished suffix exists above current head, run recovery before proceeding
4. validate block sequence and parent against the published head
5. derive `first_log_id` from the published head
6. write block-scoped authoritative artifacts with immutable semantics
7. mark any touched open stream pages
8. compute newly sealed directory summaries from `F_H` and `F_T`
9. compute newly sealed stream pages from durable open markers plus pages opened during this batch
10. materialize newly sealed summaries with create-if-absent plus validation
11. delete the corresponding open-page markers only after summary creation/validation succeeds
12. CAS `publication_state` from `H` to `H + 1`

For backfill:

- steps 4 through 11 repeat for `H + 1 ..= T`
- step 12 CASes directly from `H` to `T`

## Ownership acquisition

Add explicit ownership helpers:

- `bootstrap_publication_state(owner_id)`
- `acquire_publication(owner_id)`
- `assert_current_owner(owner_id, epoch)`

Recommended shape:

```rust
pub struct PublicationLease {
    pub owner_id: u64,
    pub epoch: u64,
    pub indexed_finalized_head: u64,
}
```

This replaces relying on `writer_epoch` as a hidden convention.

## Unpublished-Suffix Recovery

Implement cleanup-first recovery before any validate-and-reuse optimization.

Add `src/ingest/recovery.rs` with:

- `discover_unpublished_suffix(meta_store, published_head) -> Vec<BlockMetaAnchor>`
- `delete_unpublished_block(meta_store, blob_store, block_meta_anchor)`
- `cleanup_unpublished_suffix(meta_store, blob_store, published_head)`

`BlockMetaAnchor` should include:

- block number
- decoded `BlockMeta`

Deletion ordering per block:

1. delete stream fragment blobs
2. delete stream fragment metadata
3. delete block log blob
4. delete block log header
5. delete directory fragments
6. delete block-hash mapping
7. delete `block_meta/<block_num>` last

Implementation detail:

- discovery is by point lookup of `block_meta/<H+1>`, `block_meta/<H+2>`, ...
- stop at first gap
- do not use a global prefix scan for ordinary recovery

## Artifact Write Changes

## Class A block-scoped artifacts

Change `src/logs/ingest.rs` so these use immutable semantics:

- `block_logs/<block_num>` via `put_blob_if_absent`
- `block_log_headers/<block_num>` via `PutCond::IfAbsent`
- `block_meta/<block_num>` via `PutCond::IfAbsent`
- `block_hash_to_num/<hash>` via `PutCond::IfAbsent`
- `log_dir_frag/*` via `PutCond::IfAbsent`
- `stream_frag_meta/*` via `PutCond::IfAbsent`
- `stream_frag_blob/*` via `put_blob_if_absent`

On conflict:

- load the existing bytes
- validate exact equality against the expected value
- continue only if bytes match exactly
- otherwise return a typed conflict error

Recommended new errors:

- `ArtifactConflict`
- `SummaryConflict`
- `PublicationConflict`

Keep those distinct from plain `CasConflict`.

## Class B sealed summaries

For:

- `log_dir_sub/<sub_bucket>`
- `log_dir/<bucket>`
- `stream_page_meta/<stream>/<page>`
- `stream_page_blob/<stream>/<page>`

use:

- create-if-absent
- if already present, byte-validate exact expected contents before reuse

Do not overwrite sealed summaries in place.

## Stream-page marker updates

When ingest writes stream fragments for a block:

1. determine touched `(stream, shard, page_start)` tuples
2. for each touched page:
   - if the page is already sealed at `F_T`, do not create a marker
   - otherwise create `open_stream_page/<shard>/<page_start>/<stream_id>` if absent

Important rule:

- markers are created conservatively
- markers are deleted only after the corresponding sealed `stream_page_*` summary exists and has
  been validated

## Summary Materialization

Directory summaries should be computed directly from frontier arithmetic:

- `compute_new_dir_sub(H, T, F_H, F_T)`
- `compute_new_dir_bucket(H, T, F_H, F_T)`

Stream-page summaries should be computed from:

- durable `open_stream_page` markers already present below `H`
- `OpenedDuring(H -> T)` from the current batch

The implementation should:

1. union durable open markers with pages opened during the batch
2. determine which of those pages newly seal at `F_T`
3. reconstruct each sealed page summary from immutable per-page fragments
4. create-or-validate the sealed `stream_page_*` objects
5. delete the corresponding open markers

No persisted mutable builder is required.

Any future in-memory builder must obey:

- cache only
- rebuildable from fragments plus open markers
- safe to drop on crash without affecting correctness

## Startup and Restart Behavior

Update `src/recovery/startup.rs` so startup does all of the following:

1. load `publication_state` if present
2. derive the current published head
3. detect unpublished suffix above the head
4. clean or report any unpublished suffix
5. scan `open_stream_page` markers and repair any stale sealed entries if desired
6. derive `next_log_id` from the published head after recovery, not from any unpublished block

Recommended first cut:

- cleanup unpublished suffix synchronously on startup
- leave open-page marker repair to the first maintenance or ingest cycle if that keeps startup
  simpler

The key rule is:

- startup must not silently trust unpublished artifacts from a previous owner

## Migration Plan

## Phase 1 compatibility

Readers:

- read `publication_state` if present
- otherwise read `indexed_head`

Writers:

- still write `indexed_head` only until `PublicationStore` is wired

## Phase 3 dual-write transition

Once publication CAS exists:

- write `publication_state`
- optionally continue writing `indexed_head` for temporary compatibility if any external tools
  still inspect it

If dual-writing is kept temporarily:

- readers trust `publication_state`
- `indexed_head` is best-effort compatibility only

## Phase 6 cleanup

Remove:

- `writer_lease` writes from `src/ingest/engine.rs`
- `writer_lease` decode/use from `src/core/state.rs`
- legacy docs describing `writer_lease` as meaningful

Optionally remove legacy `indexed_head` reads only after all repo consumers have migrated.

## Backend-Specific Work

## In-memory store

Add:

- exact-value CAS for `PublicationStore`
- `put_blob_if_absent`
- open-page marker helpers backed by the existing metadata map
- fault-injection hooks around publication CAS and immutable-create failures

## Filesystem store

Implement:

- `put_blob_if_absent` using exclusive file creation
- `PublicationStore` using a single file plus compare-and-replace under a process-local lock
- open-page marker helper functions on top of metadata files and prefix listing

## Scylla store

Implement:

- `PublicationStore::create_if_absent` and `compare_and_set` with one-row LWT
- immutable metadata create using existing `put_if_absent` primitives where available
- blob immutability separately at the blob layer
- open-page marker helpers backed by ordinary metadata rows in the dedicated marker keyspace

The existing min-epoch fence remains stale-writer suppression only, not correctness authority.

## Error and Metrics Changes

## Errors

Add typed errors for:

- publication ownership mismatch
- publication head mismatch under same owner
- immutable artifact mismatch
- immutable summary mismatch
- optional marker inventory inconsistency, if surfaced explicitly

Keep `LeaseLost` for the case where publication owner/epoch changes before publish completes.

## Metrics

Add counters for:

- publication CAS success
- publication CAS failure due to owner mismatch
- publication CAS failure due to head mismatch
- artifact create-if-absent created
- artifact create-if-absent reused
- artifact mismatch
- summary created
- summary reused after validation
- unpublished suffix blocks cleaned
- open-page marker created
- open-page marker deleted
- stale open-page marker repaired

These metrics matter because they will later tell us whether an in-memory builder cache is worth
adding.

## Test Plan

The protocol is not complete until the test matrix exists.

## Unit tests

Add codec and store tests for:

- `PublicationState` round-trip
- `PublicationStore` CAS success/failure
- blob `put_blob_if_absent`
- immutable metadata create and exact-byte validation behavior
- marker key encoding / decoding helpers
- optional marker payload codec if a non-empty payload is used

## Ingest tests

Extend `tests/finalized_index.rs` and add new test files where needed.

Required tests:

1. bootstrap creates `publication_state` when absent
2. takeover increments epoch without changing head
3. publish one block succeeds only when expected owner/epoch/head match
4. publish batch succeeds only for contiguous validated suffix
5. stale owner gets `LeaseLost` when publish CAS fails after takeover
6. block artifact duplicate create with identical bytes is accepted
7. block artifact duplicate create with mismatched bytes fails
8. sealed summary duplicate create with identical bytes is accepted
9. sealed summary duplicate create with mismatched bytes fails
10. readers use `publication_state` in preference to legacy `indexed_head`

## Crash injection tests

Extend `tests/crash_injection_matrix.rs` with cases for:

1. crash after Class A artifacts but before publish CAS
2. crash after sealed summary creation but before publish CAS
3. crash after publish CAS but before client acknowledgment
4. takeover after partial unpublished suffix, followed by cleanup-first recovery
5. stale writer attempting immutable artifact creation after takeover
6. stale writer attempting publish after takeover

## Open-page marker tests

When Phase 5 begins, add dedicated tests for:

1. `open_stream_page` markers are created for newly touched `(stream, page)` pairs
2. newly sealed stream pages are computed from `OpenPages_H union OpenedDuring(H -> T)`
3. sealed summaries are removed from marker inventory only after successful creation/validation
4. restart/takeover can reload marker inventory and continue sealing correctly
5. stale marker recreation causes at most repair work and never a query-visible gap

## Differential query tests

Extend the differential suite so mixed crash/retry scenarios still match the naive query model once
publication succeeds.

Required scenarios:

- cleanup-first recovery after partial suffix
- pre-existing sealed summaries reused after validation
- batched publish across multiple blocks

## Suggested Commit Plan

Keep commits small and reviewable.

Recommended sequence:

1. add `PublicationState` types, codecs, and `PublicationStore`
2. teach readers to prefer `publication_state`
3. add blob immutable-create capability
4. convert Class A and Class B artifact writes to immutable semantics with byte validation
5. refactor ingest publish path to use `publication_state`
6. implement cleanup-first recovery and crash tests
7. add `open_stream_page` markers and final stream-page summary-sealing logic
8. remove legacy `writer_lease` usage and update docs

Each commit that changes crate code should leave `scripts/verify.sh finalized-history-query`
passing.

## Decisions To Lock Before Coding

These should be made explicitly before Phase 3 starts:

1. whether `indexed_head` will be dual-written during migration or dropped immediately
2. whether `PublicationStore` will be wired directly into `FinalizedHistoryService::new` or hidden
   behind a composite storage adapter
3. whether cleanup-first recovery runs automatically on startup or lazily on first ingest
4. exact `open_stream_page` key layout for efficient shard/page prefix scans
5. exact error taxonomy for artifact mismatch vs ownership loss vs publish conflict

## Practical Recommendation

The safest concrete path is:

1. land Phases 1 through 4 first
2. prove crash/retry/takeover behavior with tests
3. only then land Phase 5 `open_stream_page` marker inventory

That keeps the first protocol understandable:

- one publication CAS record
- immutable artifacts
- cleanup-first recovery
- durable page-sealing hints, not persisted builders

Only after that baseline is stable should the implementation consider an in-memory builder cache as
a purely optional optimization.
