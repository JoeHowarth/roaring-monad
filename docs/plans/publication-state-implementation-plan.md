# Publication State Implementation Plan

This document is the concrete implementation companion to `publication-state-architecture.md`.

The architecture document defines the protocol and invariants. This document translates those
invariants into:

- exact code changes in the current crate
- data shapes and new storage capabilities
- rollout order
- recovery behavior
- tests required before each stage is considered complete

The intended audience is the person implementing the change in
`crates/finalized-history-query`.

## Goals

- implement `publication_state` as the single correctness-critical control-plane record
- remove `writer_lease` from correctness and eventually from the read path
- make authoritative artifacts and sealed summaries safe under crash, retry, and takeover
- add the minimum new storage traits needed for the protocol
- preserve current reader semantics except for reading the new publication record
- land the change in stages with a clear intermediate state at each step

## Non-Goals

- redesigning query semantics
- redesigning the log storage model
- collapsing all storage access behind one generic transaction API
- optimizing away conservative recovery in the first pass

## Recommended Delivery Strategy

Implement this in six phases:

1. add the publication-state control plane and read-path compatibility
2. add immutable-create primitives for blobs and use create-if-absent for summary and block artifacts
3. refactor ingest to publish through `publication_state`
4. implement cleanup-first unpublished-suffix recovery
5. add durable open-summary state and switch summary finalization to use it
6. remove legacy `writer_lease` handling and tighten tests/docs

Phases 1 through 4 give a safe baseline. Phase 5 is where the full proposal becomes real. Do not
skip directly to a simplified steady state before phases 1 through 4 have passing tests.

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
- artifact writes are mostly unconditional

This implementation plan replaces that split with one publication record plus immutable artifact
semantics.

## End-State Types

## Domain types

Add these structs to `src/domain/types.rs`:

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct PublicationState {
    pub owner_id: u64,
    pub epoch: u64,
    pub indexed_finalized_head: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OwnedState<T> {
    pub owner_id: u64,
    pub epoch: u64,
    pub version: u64,
    pub value: T,
}
```

Add open-summary payload types only when Phase 5 begins. Keep them out of earlier phases so the
first protocol cut remains smaller:

- `OpenDirectoryState`
- `OpenDirectorySubBucket`
- `OpenDirectoryBucket`
- `OpenStreamPage`

## Keys

Add new key builders/constants in `src/domain/keys.rs`:

- `PUBLICATION_STATE_KEY`
- `OPEN_DIRECTORY_STATE_KEY`
- `open_stream_page_key(...)`
- `open_stream_page_prefix()`

Do not remove `INDEXED_HEAD_KEY` or `WRITER_LEASE_KEY` until Phase 6.

## Codecs

Add fixed codecs in `src/codec/finalized_state.rs` for:

- `PublicationState`
- `OwnedState<OpenDirectoryState>`
- `OwnedState<OpenStreamPage>`

Requirements:

- include an explicit version byte for all newly introduced records
- keep decode strict about shape and length
- keep the version field inside the serialized `OwnedState` value separate from the storage-layer
  version returned by `MetaStore`

The storage-layer version is for CAS. The payload `version` field is part of the durable logical
state and must round-trip across stores that expose different native CAS tokens.

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
- `FsMetaStore` can implement this via lock-file or file-replace logic scoped to the single
  publication record
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

Required usage after the migration:

- `block_logs/<block_num>` uses `put_blob_if_absent`
- `stream_frag_blob/*` uses `put_blob_if_absent`
- `stream_page_blob/*` uses `put_blob_if_absent`

Backend requirements:

- in-memory and filesystem stores can implement create-if-absent directly
- MinIO/S3-backed stores may need a conditional object-create mechanism or an adapter strategy
- if a backend cannot provide safe immutable create, the implementation should return
  `Error::Unsupported(...)` rather than silently falling back to unsafe overwrite

## Open-summary store

Add this only in Phase 5 in a new file such as `src/store/open_summary.rs`:

```rust
#[allow(async_fn_in_trait)]
pub trait OpenSummaryStore: Send + Sync {
    async fn load_open_directory_state(&self)
        -> Result<Option<OwnedState<OpenDirectoryState>>>;

    async fn compare_and_set_open_directory_state(
        &self,
        expected: Option<&OwnedState<OpenDirectoryState>>,
        next: &OwnedState<OpenDirectoryState>,
    ) -> Result<CasOutcome<OwnedState<OpenDirectoryState>>>;

    async fn load_open_stream_pages(&self)
        -> Result<Vec<OwnedState<OpenStreamPage>>>;

    async fn compare_and_set_open_stream_page(
        &self,
        expected: Option<&OwnedState<OpenStreamPage>>,
        next: &OwnedState<OpenStreamPage>,
    ) -> Result<CasOutcome<OwnedState<OpenStreamPage>>>;

    async fn delete_open_stream_page(
        &self,
        expected: &OwnedState<OpenStreamPage>,
    ) -> Result<CasOutcome<OwnedState<OpenStreamPage>>>;
}
```

This trait is intentionally separate from `PublicationStore` because:

- publication state is one hot singleton record
- open-summary state is a small mutable inventory with owner-scoped CAS

## Reader Changes

## `load_finalized_head_state`

Update `src/core/state.rs` so readers load visibility from:

1. `publication_state`, if present
2. otherwise fall back to `indexed_head`

Compatibility plan:

- during Phases 1 through 5, readers must understand both formats
- `writer_epoch` should be populated from `publication_state.epoch` when present
- `writer_lease` should become advisory-only immediately and should not override
  `publication_state`

Concrete rule:

- if `publication_state` exists, ignore `writer_lease` for correctness
- if only legacy rows exist, preserve current behavior

## Service surface

`FinalizedHistoryService` in `src/api/service.rs` should continue to expose:

- `query_logs`
- `ingest_finalized_block`
- `indexed_finalized_head`

No public API change is required. Only the internal source of truth changes.

## Ingest Engine Refactor

## New helper modules

Split `src/ingest/engine.rs` into explicit steps. The current file can remain the entry point, but
add helper modules to avoid one monolithic function:

- `src/ingest/publication.rs`
- `src/ingest/recovery.rs`
- `src/ingest/summary_state.rs` in Phase 5

The engine should evolve toward:

```rust
pub struct IngestEngine<M, B, P, O> {
    pub config: Config,
    pub meta_store: M,
    pub blob_store: B,
    pub publication_store: P,
    pub open_summary_store: O, // Phase 5
}
```

If wiring four generics becomes noisy, use small wrapper structs or trait objects at the engine
boundary. The important part is that `PublicationStore` is explicit.

## New ingest flow

The final ingest flow for one block should be:

1. load current `PublicationState`
2. verify current owner/epoch matches the writer token
3. if unpublished suffix exists above current head, run recovery before proceeding
4. validate block sequence and parent against the published head
5. derive `first_log_id` from the published head
6. write block-scoped authoritative artifacts with immutable semantics
7. update open-summary state while processing the block
8. determine newly sealed summaries for the candidate head advance
9. materialize newly sealed summaries with create-if-absent
10. validate any pre-existing summary keys before reuse
11. remove sealed entries from open-summary state
12. CAS `publication_state` from `H` to `H + 1`

For batched backfill, steps 4 through 11 repeat for `H + 1 ..= T` and step 12 CASes directly to
`T`.

## Ownership acquisition

Add explicit ownership methods on the ingest side:

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

This replaces using `writer_epoch` as a hidden shared convention. The service can still be
constructed with a configured writer identity, but the engine should operate on an explicit current
publication token.

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

Delete only blocks strictly above the published head.

Important implementation detail:

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

- if the key already exists, load and byte-validate it against the expected value
- continue only if bytes match exactly
- otherwise return a typed conflict error

Add new errors if needed:

- `ArtifactConflict`
- `SummaryConflict`
- `PublicationConflict`

These should be separate from plain `CasConflict`, which is too generic once the new protocol
lands.

## Class B sealed summaries

For:

- `log_dir_sub/<sub_bucket>`
- `log_dir/<bucket>`
- `stream_page_meta/<stream>/<page>`
- `stream_page_blob/<stream>/<page>`

change writes to:

- create-if-absent
- if already exists, byte-validate exact expected contents before reuse

Do not overwrite sealed summaries in place.

## Summary Materialization

## Phase 2 interim behavior

Before open-summary state exists, keep the current compaction helpers but make them safe:

- still compute newly sealed directory summaries directly from `first_log_id`, `count`, and
  `next_log_id`
- still compute newly sealed stream pages from pages touched during the current block
- do not claim that this fully satisfies the final proposal

This phase is acceptable only while recovery always cleans unpublished suffixes before continuing.
That guarantees a writer does not inherit unknown open pages from a previous owner.

## Phase 5 final behavior

Once `OpenSummaryStore` exists:

- `persist_stream_fragments` returns both `OpenedDuring(H -> T)` and per-page mutations
- directory ingest updates the current open sub-bucket and open bucket state
- stream ingest updates owned open-page entries

Then implement:

- `compute_new_dir_sub(H, T, F_H, F_T)`
- `compute_new_dir_bucket(H, T, F_H, F_T)`
- `compute_new_stream_pages(open_pages_h, opened_during, F_H, F_T)`

Only after those sets are computed from durable open state should sealed summaries be written.

## Startup Recovery

Update `src/recovery/startup.rs` so startup does all of the following:

1. load `publication_state` if present
2. derive current published head from it
3. detect unpublished suffix above the head
4. report whether cleanup is required before ingest can proceed
5. derive `next_log_id` from the published head after recovery, not from any unpublished block

Recommended behavior:

- startup should not silently trust unpublished artifacts from a previous owner
- if unpublished suffix exists, either:
  - clean it synchronously during startup, or
  - surface a startup state that requires cleanup before first ingest

The simpler first cut is synchronous cleanup.

## Migration Plan

## Phase 1 compatibility

Readers:

- read `publication_state` if present
- otherwise read `indexed_head`

Writers:

- still write `indexed_head` only until `PublicationStore` is wired

This phase is purely preparatory.

## Phase 3 dual-write transition

Once publication CAS exists:

- write `publication_state`
- optionally continue writing `indexed_head` for temporary compatibility if any external tools
  still inspect it

If dual-writing is kept temporarily, the rule must be:

- readers trust `publication_state`
- `indexed_head` is best-effort compatibility only

Do not allow any correctness logic to depend on legacy `indexed_head` once `publication_state`
publishing is active.

## Phase 6 cleanup

Remove:

- `writer_lease` writes from `src/ingest/engine.rs`
- `writer_lease` decode/use from `src/core/state.rs`
- legacy docs describing `writer_lease` as meaningful

Optionally remove legacy `indexed_head` reads only after the entire repo and any operational tools
have migrated.

## Backend-Specific Work

## In-memory store

Add:

- exact-value CAS for `PublicationStore`
- `put_blob_if_absent`
- helper methods for test fault injection around publication CAS and immutable-create failures

This backend is the reference implementation for unit and integration tests.

## Filesystem store

Implement:

- `put_blob_if_absent` using exclusive file creation
- `PublicationStore` using a single file plus compare-and-replace under a process-local lock

Correctness matters more than performance here.

## Scylla store

Implement:

- `PublicationStore::create_if_absent` and `compare_and_set` with one-row LWT
- immutable metadata create using existing `put_if_absent` primitives where available
- blob immutability separately at the blob layer; do not assume the metadata fence is sufficient

The existing min-epoch fence remains useful only as stale-writer suppression, not as the ownership
authority.

## Error and Metrics Changes

## Errors

Add typed errors for:

- publication ownership mismatch
- publication head mismatch under same owner
- immutable artifact mismatch
- immutable summary mismatch
- open-summary ownership mismatch

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
- open-summary CAS conflicts

These metrics will be essential when deciding later whether any protocol rules can be relaxed.

## Test Plan

The protocol is not complete until the test matrix exists.

## Unit tests

Add codec and store tests for:

- `PublicationState` round-trip
- `OwnedState<T>` round-trip
- `PublicationStore` CAS success/failure
- blob `put_blob_if_absent`
- immutable metadata create and exact-byte validation behavior

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

## Open-summary tests

When Phase 5 begins, add dedicated tests for:

1. open directory state is owned by `(owner_id, epoch)`
2. open stream page CAS rejects stale owner/version
3. newly sealed stream pages are computed from `OpenPages_H union OpenedDuring(H -> T)`
4. takeover reclaims or rebuilds open state before continuing
5. sealed summaries are removed from open state only after successful creation/validation

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
7. add open-summary state and final summary-finalization logic
8. remove legacy `writer_lease` usage and update docs

Each commit that changes crate code should leave `scripts/verify.sh finalized-history-query`
passing.

## Decisions To Lock Before Coding

These should be made explicitly before Phase 3 starts:

1. whether `indexed_head` will be dual-written during migration or dropped immediately
2. whether `PublicationStore` will be wired directly into `FinalizedHistoryService::new` or hidden
   behind a composite storage adapter
3. whether cleanup-first recovery runs automatically on startup or lazily on first ingest
4. whether open-summary logical versions live inside payload bytes, storage CAS versions, or both
5. exact error taxonomy for artifact mismatch vs ownership loss vs publish conflict

## Practical Recommendation

The safest concrete path is:

1. land Phases 1 through 4 first
2. prove crash/retry/takeover behavior with tests
3. only then land Phase 5 open-summary state

That order keeps the early protocol understandable:

- one publication CAS record
- immutable artifacts
- cleanup-first recovery

Only after that baseline is stable should the implementation take on the additional complexity of
durable open-summary ownership and sealing logic.
