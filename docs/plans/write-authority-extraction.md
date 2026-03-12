# Write Authority Extraction

## Summary

Extract the multi-writer leadership protocol (lease acquisition, renewal, takeover,
fencing, recovery) out of the ingest engine into a `WriteAuthority` trait with two
implementations:

- `LeaseAuthority` — the current publication-state CAS protocol with all its complexity
- `SingleWriterAuthority` — a trivial always-ok implementation for single-writer deployments

The ingest engine becomes generic over `WriteAuthority` and loses all direct knowledge
of `PublicationState`, `PublicationLease`, sessions, epochs, lease expiry, and renewal.

## Goals

- make the ingest engine independently testable without the lease protocol
- make the lease protocol independently testable without constructing an ingest engine
- give single-writer deployments a code path with zero lease ceremony overhead
- create a clean module boundary that makes it obvious what the leadership protocol is
  responsible for vs what the artifact-writing pipeline is responsible for
- preserve all current correctness properties

## Non-Goals

- changing the publication-state CAS protocol itself
- fixing the open protocol gaps from `publication-state-review-followup.md` or
  `publication-state-operational-followup.md` (those are follow-up work that becomes
  easier after this extraction)
- introducing a separate crate (the trait implementations share store types; a crate
  split would force trait-crate layering that is not justified yet)
- changing reader-side behavior (readers only observe `indexed_finalized_head` and
  are not affected by this refactor)

## Why Module, Not Crate

`PublicationStore` and `FenceStore` are implemented by the same concrete types that
implement `MetaStore` (`InMemoryMetaStore`, `ScyllaMetaStore`, `FsMetaStore`).

A crate boundary would either:

- force those traits into a shared traits crate, creating circular layering
- require store implementations to live in one crate and implement traits from another

The coupling surface between the authority and the engine is small enough that a
module boundary within the existing crate is the right size.

## Current Coupling Points

The ingest engine currently depends on the lease protocol in these specific places:

### `ingest/engine.rs`

1. **Import**: `PublicationLease`, `renew_publication_if_needed`
2. **Type bound**: `M: MetaStore + PublicationStore`
3. **Method signature**: `ingest_finalized_blocks(..., lease: PublicationLease)`
4. **Renewal bookends**: calls `renew_publication_if_needed` at lines 50-57 and 141-148
5. **Fence derivation**: `lease.fence_token()` at line 65
6. **Epoch threading**: `lease.epoch` passed to every artifact-write helper
7. **Publish CAS**: direct `compare_and_set` on `PublicationState` at lines 165-185
8. **Lease loss detection**: pattern-matches on `owner_id`/`session_id`/`epoch` at 174-179

### `api/service.rs`

1. **Type bound**: `M: MetaStore + PublicationStore + FenceStore`
2. **Startup**: calls `startup_with_writer` which acquires publication
3. **Lease caching**: `startup_state: Arc<Mutex<Option<PublicationLease>>>`
4. **Lease threading**: passes cached lease into `ingest.ingest_finalized_blocks`
5. **Lease update**: updates cached lease from ingest result

### `recovery/startup.rs`

1. `startup_with_writer`: acquires publication, runs cleanup, repairs markers
2. `startup_plan`: read-only, loads head state

### `ingest/recovery.rs` and `ingest/open_pages.rs`

These take `FenceToken` directly and do not depend on publication or lease types.
They are already cleanly separated.

## Proposed Design

### New trait: `WriteAuthority`

```rust
/// Opaque write-permission token returned by the authority.
/// The engine treats this as a black box and threads it through calls.
#[derive(Debug, Clone, Copy)]
pub struct WriteToken {
    pub epoch: u64,
    pub indexed_finalized_head: u64,
}

/// Abstracts writer leadership away from the ingest engine.
///
/// The engine calls `authorize` before writing, `publish` after writing,
/// and `fence` to get the token for artifact-write fencing.
#[allow(async_fn_in_trait)]
pub trait WriteAuthority: Send + Sync {
    /// Validate that this process may still write, and renew permission
    /// if needed. Called at the start of ingest and after compaction.
    ///
    /// Returns the current write token, or an error if permission was lost.
    async fn authorize(&self, current: &WriteToken, now_ms: u64) -> Result<WriteToken>;

    /// Commit a head advance from `current.indexed_finalized_head` to `new_head`.
    /// Returns the updated token on success, or an error if another writer
    /// took ownership.
    async fn publish(
        &self,
        current: &WriteToken,
        new_head: u64,
    ) -> Result<WriteToken>;

    /// Derive the fence token for artifact writes under this authority.
    fn fence(&self, token: &WriteToken) -> FenceToken;

    /// Acquire initial write permission. Called during startup.
    /// For lease-based authorities this acquires the publication lease.
    /// For single-writer authorities this is a no-op that reads current head.
    async fn acquire(&self, now_ms: u64) -> Result<WriteToken>;

    /// Run startup recovery (cleanup unpublished suffix, repair markers)
    /// under the authority of the given token.
    async fn recover<M: MetaStore, B: BlobStore>(
        &self,
        token: &WriteToken,
        meta_store: &M,
        blob_store: &B,
    ) -> Result<()>;
}
```

### `LeaseAuthority`

Wraps the current protocol. Owns `owner_id`, `session_id`, and all publication-state
CAS logic.

```rust
pub struct LeaseAuthority<P> {
    publication_store: P,       // P: PublicationStore + FenceStore
    owner_id: u64,
    session_id: SessionId,
    lease_duration_ms: u64,
    renew_skew_ms: u64,
    // Internal mutable lease state protected by async mutex
    lease: futures::lock::Mutex<Option<PublicationLease>>,
}
```

Implementation sketch:

- `acquire`: calls `acquire_publication_with_session`, stores lease internally,
  returns `WriteToken { epoch, indexed_finalized_head }`
- `authorize`: calls `renew_publication_if_needed` if needed, validates
  owner/session/epoch match, returns updated token
- `publish`: builds expected + next `PublicationState`, calls `compare_and_set`,
  handles `LeaseLost` / `PublicationConflict`
- `fence`: returns `FenceToken(token.epoch)`
- `recover`: calls `cleanup_unpublished_suffix` and `repair_open_stream_page_markers`
  under `FenceToken(token.epoch)`

The existing functions in `ingest/publication.rs` (`acquire_publication_with_session`,
`renew_publication_if_needed`, `bootstrap_publication_state`) become internal helpers
of `LeaseAuthority`, not public API of the crate.

### `SingleWriterAuthority`

Trivial implementation for single-writer deployments.

```rust
pub struct SingleWriterAuthority<M> {
    meta_store: M,  // M: MetaStore
    epoch: u64,     // fixed at construction, typically 1
}
```

Implementation sketch:

- `acquire`: reads current head from store (or starts at 0), returns token with
  fixed epoch
- `authorize`: always returns the token unchanged (no renewal needed)
- `publish`: unconditionally writes head to store (no CAS), returns updated token.
  The head record uses a simple key like `single_writer_head` with `PutCond::Any`.
- `fence`: returns `FenceToken(self.epoch)`
- `recover`: runs the same `cleanup_unpublished_suffix` and
  `repair_open_stream_page_markers` with the fixed fence token

### Refactored `IngestEngine`

```rust
pub struct IngestEngine<A: WriteAuthority, M: MetaStore, B: BlobStore> {
    pub config: Config,
    pub authority: A,
    pub meta_store: M,
    pub blob_store: B,
}
```

The engine no longer imports `PublicationLease`, `PublicationStore`, `PublicationState`,
or `renew_publication_if_needed`.

The ingest method becomes:

```rust
pub async fn ingest_finalized_blocks(
    &self,
    blocks: &[Block],
    token: WriteToken,
) -> Result<(IngestOutcome, WriteToken)> {
    let token = self.authority.authorize(&token, now_ms).await?;
    validate_block_sequence(&self.meta_store, blocks, &token).await?;

    let fence = self.authority.fence(&token);
    // ... artifact writes using fence.0 as epoch ...

    let token = self.authority.authorize(&token, now_ms).await?;
    let token = self.authority.publish(&token, last_block.block_num).await?;
    Ok((outcome, token))
}
```

### Refactored `FinalizedHistoryService`

```rust
pub struct FinalizedHistoryService<A: WriteAuthority, M: MetaStore, B: BlobStore> {
    pub ingest: IngestEngine<A, M, B>,
    query: LogsQueryEngine,
    config: Config,
    state: Arc<RuntimeState>,
    token: Arc<futures::lock::Mutex<Option<WriteToken>>>,
}
```

The service no longer knows about `writer_id`, `session_id`, or `PublicationLease`.
Those concepts live inside `LeaseAuthority`.

Startup becomes:

```rust
pub async fn startup(&self) -> Result<RecoveryPlan> {
    let token = self.ingest.authority.acquire(now_ms).await?;
    self.ingest.authority.recover(&token, &self.ingest.meta_store, &self.ingest.blob_store).await?;
    *self.token.lock().await = Some(token);
    // ... build recovery plan from token.indexed_finalized_head ...
}
```

## Module Layout

```
ingest/
    mod.rs
    engine.rs               -- IngestEngine<A, M, B>, generic over WriteAuthority
    authority.rs             -- WriteAuthority trait, WriteToken
    authority/
        mod.rs
        lease.rs             -- LeaseAuthority (current full protocol)
        single_writer.rs     -- SingleWriterAuthority (trivial)
    open_pages.rs            -- unchanged, takes FenceToken
    recovery.rs              -- unchanged, takes FenceToken
    planner.rs               -- unchanged
```

The `store/publication.rs` traits (`PublicationStore`, `FenceStore`) remain in `store/`
because they describe storage capabilities. `LeaseAuthority` imports them;
`SingleWriterAuthority` does not.

The `lease/` module (`LeaseManager`) can be deleted — it is vestigial. The in-memory
atomic epoch tracking was the predecessor to `PublicationLease` and is no longer used
in the main path.

## What Moves Where

| Current location | Destination | Notes |
|---|---|---|
| `ingest/publication.rs` (all of it) | `ingest/authority/lease.rs` | Becomes internal to `LeaseAuthority` |
| `PublicationLease` struct | `ingest/authority/lease.rs` | Internal to `LeaseAuthority`, not public |
| `WriteToken` (new) | `ingest/authority.rs` | Public, used by engine |
| `WriteAuthority` trait (new) | `ingest/authority.rs` | Public |
| `SingleWriterAuthority` (new) | `ingest/authority/single_writer.rs` | Public |
| `lease/manager.rs` | deleted | Vestigial |
| `store/publication.rs` | stays | Still needed by `LeaseAuthority` |
| `domain/types.rs::PublicationState` | stays | Still needed by store impls and `LeaseAuthority` |
| `recovery/startup.rs::startup_with_writer` | `ingest/authority/lease.rs` | Part of `LeaseAuthority::acquire` + `recover` |
| `recovery/startup.rs::startup_plan` | `recovery/startup.rs` | Stays as read-only helper for RO nodes |

## Impact On Readers

None. Reader-side code (`core/range.rs`, `logs/query.rs`, `core/state.rs`) reads
`indexed_finalized_head` via `PublicationStore::load()` or the `load_finalized_head_state`
helper. This path does not go through `WriteAuthority` and is unaffected.

For `SingleWriterAuthority`, readers still need to discover the current head.
`SingleWriterAuthority` should implement `PublicationStore::load()` or use a compatible
head record so that readers can resolve the visibility watermark through the same
`load_finalized_head_state` path. The simplest approach:

- `SingleWriterAuthority` writes its head to the same `publication_state` key using
  `PutCond::Any` (no CAS), with a fixed epoch and dummy session
- readers use the same `load_finalized_head_state` path unchanged

This avoids a reader-side branch.

## Implementation Order

### Phase 1: Introduce `WriteAuthority` and `WriteToken`

1. Create `ingest/authority.rs` with the `WriteAuthority` trait and `WriteToken` struct.
2. Create `ingest/authority/mod.rs` re-exporting the trait and both implementations.

### Phase 2: Implement `LeaseAuthority`

1. Create `ingest/authority/lease.rs`.
2. Move all logic from `ingest/publication.rs` into `LeaseAuthority` methods.
3. Keep `bootstrap_publication_state` and `acquire_publication_with_session` as
   private helpers inside the module.
4. Move lease-related tests from `ingest/publication.rs` into `authority/lease.rs`.
5. Move the ownership-acquisition and recovery logic from `recovery/startup.rs::
   startup_with_writer` into `LeaseAuthority::acquire` and `LeaseAuthority::recover`.

### Phase 3: Implement `SingleWriterAuthority`

1. Create `ingest/authority/single_writer.rs`.
2. Implement with fixed epoch, no CAS, `PutCond::Any` head writes.
3. Write tests proving single-writer ingest works end-to-end without any publication
   state ceremony.

### Phase 4: Make `IngestEngine` generic over `WriteAuthority`

1. Add type parameter `A: WriteAuthority` to `IngestEngine`.
2. Replace all `PublicationLease` usage with `WriteToken`.
3. Replace `renew_publication_if_needed` calls with `authority.authorize`.
4. Replace the publish CAS block with `authority.publish`.
5. Replace `lease.fence_token()` with `authority.fence(&token)`.
6. Replace `lease.epoch` threading with `authority.fence(&token).0` where an epoch
   value is needed for artifact writes.
7. Remove `PublicationStore` from the `M` type bound on `IngestEngine`.

### Phase 5: Update `FinalizedHistoryService`

1. Add `A: WriteAuthority` type parameter.
2. Replace `startup_state: Option<PublicationLease>` with `Option<WriteToken>`.
3. Remove `writer_id` and `session_id` fields (owned by `LeaseAuthority`).
4. Update `startup()` to use `authority.acquire` + `authority.recover`.
5. Update `ingest_blocks_with_startup` to thread `WriteToken`.

### Phase 6: Cleanup

1. Delete `ingest/publication.rs` (contents moved to `authority/lease.rs`).
2. Delete `lease/manager.rs` and the `lease/` module.
3. Remove `IngestMode` from `Config` (the choice is now made at construction time
   by choosing which `WriteAuthority` implementation to use).
4. Update `recovery/startup.rs`: keep only `startup_plan` (read-only) and
   `build_recovery_plan` helper.

### Phase 7: Tests

#### Authority-level tests (in `authority/lease.rs`)

These already exist and move with the code:

- bootstrap race does not accept foreign owner
- fresh lease rejects takeover until expiry
- same-owner restart bumps epoch and session
- (future) fence advancement during takeover

New tests:

- `LeaseAuthority::authorize` returns `LeaseLost` after external takeover
- `LeaseAuthority::publish` returns `LeaseLost` on epoch mismatch
- `LeaseAuthority::publish` returns `PublicationConflict` on head mismatch

#### Authority-level tests (in `authority/single_writer.rs`)

- `acquire` returns head 0 on empty store
- `acquire` returns current head on non-empty store
- `publish` advances head unconditionally
- `fence` returns fixed epoch
- readers can load the head written by `SingleWriterAuthority`

#### Engine-level tests

- ingest with `SingleWriterAuthority` writes correct artifacts and advances head
- ingest with `LeaseAuthority` against `InMemoryMetaStore` works end-to-end
- engine does not import or mention `PublicationLease`, `PublicationState`, or
  `PublicationStore` (compile-time guarantee from removed imports)

## Risks and Mitigations

### Risk: `WriteToken` becomes a god struct

`WriteToken` intentionally carries only `epoch` and `indexed_finalized_head`. If the
engine needs more authority-specific state in the future, that should be handled by
the authority internally (via its own mutex or internal state), not by expanding
`WriteToken`.

### Risk: `SingleWriterAuthority` publish is not atomic with readers

Because it uses `PutCond::Any` instead of CAS, two processes using
`SingleWriterAuthority` concurrently would corrupt state. This is by design — the
single-writer variant assumes exclusive access. The type name and documentation should
make this clear.

### Risk: `recover` method on the trait is awkward

Recovery needs `MetaStore` + `BlobStore` references, which the authority may not own.
The proposed signature passes them in. An alternative is to have recovery live outside
the trait entirely (the engine calls `cleanup_unpublished_suffix` directly with the
fence from `authority.fence`). This is a reasonable simplification if the trait surface
feels too broad.

**Recommendation**: start with recovery outside the trait. Both `cleanup_unpublished_suffix`
and `repair_open_stream_page_markers` already take `FenceToken` directly. The startup
path can call `authority.acquire()` to get the token, then call recovery helpers directly
with `authority.fence(&token)`. This keeps the trait surface minimal:

```rust
pub trait WriteAuthority: Send + Sync {
    async fn acquire(&self, now_ms: u64) -> Result<WriteToken>;
    async fn authorize(&self, current: &WriteToken, now_ms: u64) -> Result<WriteToken>;
    async fn publish(&self, current: &WriteToken, new_head: u64) -> Result<WriteToken>;
    fn fence(&self, token: &WriteToken) -> FenceToken;
}
```

Recovery stays as a direct call in startup:

```rust
let token = authority.acquire(now_ms).await?;
let fence = authority.fence(&token);
cleanup_unpublished_suffix(meta_store, blob_store, token.indexed_finalized_head, fence).await?;
repair_open_stream_page_markers(meta_store, blob_store, next_log_id, fence).await?;
```

This is simpler and avoids threading store references through the trait.

## Relationship To Open Protocol Gaps

This refactor does not fix the open gaps documented in the operational follow-up.
It makes them easier to fix because:

- fence advancement during takeover is now localized to `LeaseAuthority::acquire`
- Class B summary recovery is called from startup, which is now a clear sequence of
  `acquire` → `fence` → `recover`
- bounded marker scans are in `open_pages.rs` which is already independent of the
  authority
- filesystem CAS is in `store/fs.rs` which is independent of this refactor
- legacy type cleanup is easier because the engine no longer references
  `PublicationState` directly

## Bottom Line

The ingest engine should not know how writer leadership works. It should receive
permission to write, write artifacts, and commit the result. The `WriteAuthority`
trait makes that boundary explicit and gives single-writer deployments a zero-overhead
path that is independently testable.
