# Publication State Architecture

## Summary

This document defines the ownership and publication protocol for finalized-history ingest.

The core decision is:

- replace the current correctness split across `writer_lease` and `indexed_head`
- use one authoritative publication record for both ownership proof and reader visibility
- keep artifact writes outside that record and publish them only by advancing the publication head
- preserve a separate low-level write fence only as a stale-writer suppression mechanism, not as the publication authority

The intended end state is:

- one correctness-critical publication record
- one conditional publish operation per visible head advance
- no cross-row lease/publication race
- a backend abstraction that maps cleanly to both Scylla and DynamoDB
- support for single-block publication and batched backfill publication under the same protocol

## Goals

- define one authoritative ownership and visibility model
- eliminate the current race between `writer_lease` and `indexed_head`
- support Scylla using its strongest practical primitive for this case
- support DynamoDB without requiring a broad transaction abstraction
- keep per-block overhead acceptable for steady-state ingest
- allow batched head advancement for backfill indexing
- specify exact behavior for crash, takeover, retry, and duplicate-publication cases
- make reader visibility rules explicit

## Non-Goals

- changing query semantics or pagination semantics
- making unpublished artifacts above the published head reader-visible
- introducing generic multi-key or cross-partition transactions into the main storage abstraction
- requiring heartbeat-based timed leases in the first implementation
- preserving compatibility with the current `writer_lease` row as a correctness mechanism

## Problem Statement

Today the crate writes:

- `writer_lease` as advisory metadata
- authoritative block and index artifacts
- `indexed_head` as the visible watermark

That shape has a correctness gap:

- ownership is described in one row
- visibility is committed in another row
- the final publication step does not prove that the writer still owns the advertised lease

With only ordinary single-row conditional writes in the generic metadata abstraction, no protocol that checks lease row `A` while publishing head row `B` can avoid a time-of-check/time-of-use race.

That means one of these must be true:

1. `writer_lease` is not part of correctness and should be treated as advisory only, or
2. ownership proof and head publication must be merged into one atomic conditional mutation

This plan chooses the second option.

## Core Decision

Introduce one authoritative record:

- `publication_state`

This record is the only persisted control-plane state that determines:

- who currently owns publication rights
- which epoch that ownership corresponds to
- how far finalized history is visible to readers

Conceptually:

```rust
struct PublicationState {
    owner_id: u64,
    epoch: u64,
    indexed_finalized_head: u64,
}
```

The key invariant is:

- a publish succeeds only if the writer atomically proves that the current publication owner and epoch still match its own ownership token

Readers must treat:

- `publication_state.indexed_finalized_head`

as the only visibility watermark.

## Why One Record Is Required

The correctness argument is simple:

- artifact writes can happen before visibility
- visibility must be one atomic state transition
- ownership proof must be part of that same state transition

If ownership and visibility are stored in separate records, the protocol must:

1. read ownership
2. write visibility

Another writer can take over between those steps.

A generic multi-row transaction abstraction is not the right fix here because:

- Scylla is best used with LWT on one row or one partition, not as a generic transaction engine
- DynamoDB can do atomic conditional single-item updates directly and cheaply
- the control-plane state for this crate naturally fits one logical record

## Persisted Model

### Publication record

Persist one logical record:

- `publication_state -> PublicationState`

Required fields:

- `owner_id`
- `epoch`
- `indexed_finalized_head`

Optional future fields:

- `lease_deadline_ms`
- `last_heartbeat_ms`
- `writer_metadata`

The first implementation should not require timed fields for correctness.

### Artifact records

The existing artifact layout remains authoritative below the publication boundary:

- `block_meta/<block_num>`
- `block_hash_to_num/<block_hash>`
- `block_log_headers/<block_num>`
- `block_logs/<block_num>`
- `log_dir_frag/*`
- `log_dir_sub/*`
- optional `log_dir/*`
- `stream_frag_meta/*`
- `stream_frag_blob/*`
- `stream_page_meta/*`
- `stream_page_blob/*`

Artifacts for blocks above `publication_state.indexed_finalized_head` are:

- allowed to exist
- not reader-visible committed history
- safe to overwrite deterministically during retry or takeover replay

## Ownership Token

The writer identity used by ingest is:

- `(owner_id, epoch)`

Properties:

- `epoch` is monotonically increasing across takeovers
- the same `owner_id` may appear again in the future only with a higher `epoch`
- a stale writer cannot reuse an older `(owner_id, epoch)` pair after takeover

This token is embedded in `PublicationState` itself. No second lease row is required for correctness.

## Protocol

### Bootstrap

If `publication_state` is absent:

1. create `PublicationState { owner_id, epoch: 1, indexed_finalized_head: 0 }` with conditional create
2. if create loses, read the current row and continue with the normal acquire path

### Explicit acquire / takeover

Takeover is explicit, not time-based, in the base design.

To acquire ownership from current state:

1. load current `PublicationState`
2. construct:
   - `next.owner_id = new_owner_id`
   - `next.epoch = current.epoch + 1`
   - `next.indexed_finalized_head = current.indexed_finalized_head`
3. conditionally replace the current row with `next`
4. on CAS success, the new writer owns publication
5. on CAS failure, reload and retry or report contention

### Publish one block

Let the writer own:

- `(owner_id, epoch)`

and let current visible head be:

- `H`

After writing all authoritative artifacts for block `H + 1`, publish by conditional update:

- expected: `PublicationState { owner_id, epoch, indexed_finalized_head: H }`
- next: `PublicationState { owner_id, epoch, indexed_finalized_head: H + 1 }`

If the conditional update succeeds:

- block `H + 1` is visible

If it fails:

- reload the row
- if `owner_id` or `epoch` changed, return `LeaseLost`
- if the head changed unexpectedly under the same owner, return a publication conflict

### Publish a batch for backfill

The same protocol supports batched publication.

After writing authoritative artifacts for blocks `H + 1 ..= T`, publish:

- expected head: `H`
- next head: `T`
- owner and epoch unchanged

This reduces control-plane conditional writes during backfill from:

- one per block

to:

- one per published batch

The correctness rule stays unchanged:

- only blocks at or below the published head are visible

### Retry after partial failure

If a writer crashes after writing artifacts but before publish:

- those artifacts remain above the visible head
- readers must ignore them
- the same writer or a takeover writer may deterministically rewrite them during replay

If a writer crashes after publish succeeds but before receiving acknowledgment:

- retry must first reload `PublicationState`
- if the head already advanced to the intended value or beyond under the same owner/epoch, treat the publish as already committed

This makes publish idempotent with respect to ambiguous client-visible completion.

## Reader Semantics

Readers must use only:

- `publication_state.indexed_finalized_head`

to determine the visible finalized boundary.

Readers must not:

- infer visibility from `block_meta`
- infer visibility from directory or stream artifacts
- treat owner/epoch changes alone as visibility changes

This keeps visibility monotonic and easy to reason about.

## Interaction With Low-Level Write Fencing

The existing fence/min-epoch mechanism remains useful, but its role changes.

It should be treated as:

- an optimization and stale-writer suppression mechanism for ordinary artifact writes

It should not be treated as:

- the publication authority

Recommended rule:

1. publication ownership changes by CAS on `publication_state`
2. after successful takeover, the new writer raises the low-level minimum epoch for ordinary writes

Consequences:

- correctness does not depend on the fence update completing first
- until the fence is raised, a stale writer may still waste work by writing unpublished artifacts
- that wasted work is harmless because stale writers cannot publish without winning the `publication_state` CAS

This separation keeps the protocol principled:

- `publication_state` decides correctness
- low-level fencing reduces wasted writes and stale-writer churn

## Backend Abstraction

The control-plane protocol should not be expressed through the generic `MetaStore` alone.

Introduce a dedicated trait for the hot publication record.

Conceptual shape:

```rust
struct PublicationState {
    owner_id: u64,
    epoch: u64,
    indexed_finalized_head: u64,
}

enum CasOutcome {
    Applied(PublicationState),
    Failed { current: Option<PublicationState> },
}

#[allow(async_fn_in_trait)]
trait PublicationStore: Send + Sync {
    async fn load(&self) -> Result<Option<PublicationState>>;

    async fn create_if_absent(&self, initial: &PublicationState) -> Result<CasOutcome>;

    async fn compare_and_set(
        &self,
        expected: &PublicationState,
        next: &PublicationState,
    ) -> Result<CasOutcome>;
}
```

This is intentionally narrower than:

- a generic transaction trait
- a multi-key compare-and-swap API

Why this is the right level:

- Scylla can implement it with one-row LWT
- DynamoDB can implement it with one-item conditional writes
- the ingest protocol does not need a broader correctness primitive

If the implementation needs richer error classification, add typed failure reasons above this trait rather than widening the storage primitive itself.

## Scylla Mapping

Scylla should use a dedicated table with one hot logical row.

Conceptually:

```text
publication_state (
    id text primary key,
    owner_id bigint,
    epoch bigint,
    indexed_head bigint
)
```

Implementation guidance:

- use `INSERT ... IF NOT EXISTS` for bootstrap
- use `UPDATE ... IF owner_id = ? AND epoch = ? AND indexed_head = ?` for publish
- use `UPDATE ... IF owner_id = ? AND epoch = ? AND indexed_head = ?` or equivalent expected-row CAS for takeover

Important constraints:

- keep the publication protocol to one row
- do not rely on broad multi-row conditional batches as the steady-state path
- do not split ownership proof and head update across separate rows

Scylla-specific notes:

- one LWT per steady-state publish is acceptable for normal block cadence
- backfill should publish in batches to amortize the LWT cost
- the publication row is an expected hot spot by design and should remain tiny

## DynamoDB Mapping

DynamoDB should use one item for `PublicationState`.

Implementation guidance:

- bootstrap with `PutItem` and `attribute_not_exists`
- publish with `UpdateItem` plus a condition expression on:
  - `owner_id`
  - `epoch`
  - `indexed_finalized_head`
- takeover with `UpdateItem` plus a condition expression matching the full expected prior state

The base protocol should not require `TransactWriteItems` because:

- one-item conditional updates are enough
- the control-plane state naturally fits one item
- avoiding transactions keeps the hot path cheaper and simpler

## Supported Consistency Profiles

This protocol solves ownership/publication races. It does not create visibility guarantees that the underlying artifact stores do not support.

A supported deployment profile must ensure:

1. artifact writes are durably committed before publication CAS is attempted
2. readers that observe the new published head can also observe the artifact writes needed for blocks at or below that head

For the expected backends:

- Scylla metadata is strongly consistent enough for the publication row itself
- DynamoDB is strongly consistent for the item write itself
- any paired blob/object store used for artifacts must provide the necessary read-after-write visibility for published keys

If a deployment cannot provide that visibility contract, then:

- the publication protocol still preserves ownership correctness
- but reader-visible consistency at the newly published head is not guaranteed

That is a deployment contract issue, not a reason to split ownership and visibility rows again.

## Failure Matrix

The implementation must handle all of these cases explicitly.

### Case 1: stale writer writes artifacts after takeover

Allowed outcome:

- stale writer may waste work
- stale writer must not publish

### Case 2: stale writer attempts publish after takeover

Required outcome:

- publish CAS fails
- ingest returns `LeaseLost`

### Case 3: writer crashes before publish

Required outcome:

- head does not advance
- readers do not see the new block
- retry can overwrite or reuse unpublished artifacts deterministically

### Case 4: writer crashes after publish success but before ack

Required outcome:

- reload reveals advanced head
- retry treats publish as already committed, not as corruption

### Case 5: concurrent takeover attempts

Required outcome:

- exactly one takeover CAS wins
- losers reload and observe the winning state

### Case 6: duplicate publish attempt with same target head

Required outcome:

- operation is either a no-op success after reload or a classified conflict
- never double-advances the head

### Case 7: batched backfill publish

Required outcome:

- only the final published head matters for visibility
- partial artifact sets above the old head remain invisible if publish fails

### Case 8: fence update lags after takeover

Required outcome:

- old writer may still write unpublished artifacts
- old writer still cannot publish
- correctness remains intact

## Interaction With `StrictCas`

This plan makes the current `StrictCas` versus `SingleWriterFast` split unnecessary as a correctness model.

Once `PublicationStore` exists:

- production publication should always use the publication CAS protocol
- an unsafe fast path should not exist as a production correctness mode

The remaining question is only whether local/test backends also implement the same publication CAS semantics.

Recommended direction:

- remove `StrictCas` and `SingleWriterFast` as competing production ingest modes
- if a bypass is kept temporarily, mark it clearly as test-only or migration-only

## Migration Plan

Because this project does not require production backward compatibility yet, prefer a direct cutover.

### Phase 1: add publication state and tests

1. add `PublicationState` types and `PublicationStore` trait
2. add failing tests for:
   - lease loss before publish
   - concurrent takeover
   - crash before publish
   - crash after publish acknowledgment loss

### Phase 2: implement backend adapters

1. implement in-memory `PublicationStore`
2. implement Scylla `PublicationStore` using one-row LWT
3. implement DynamoDB `PublicationStore` using one-item conditional update

### Phase 3: switch ingest and shared-state reads

1. switch ingest ownership and publish flow to `PublicationStore`
2. stop treating `writer_lease` as correctness-critical
3. switch finalized-head reads to `publication_state.indexed_finalized_head`

### Phase 4: clean up legacy control-plane state

1. remove or deprecate `writer_lease`
2. replace direct `indexed_head` reads with `publication_state`
3. simplify `IngestMode`
4. update architecture and onboarding docs

## Test Plan

Minimum required automated coverage:

- bootstrap create-if-absent has one winner
- takeover CAS has one winner
- stale writer publish fails after takeover
- stale writer ordinary writes above head remain invisible
- publish advances head only after artifacts exist
- retry after pre-publish crash succeeds
- retry after ambiguous post-publish crash is idempotent
- backfill batch publish advances from `H` to `T` atomically
- readers never observe blocks above the published head
- in-memory, Scylla, and DynamoDB backends all satisfy the same observable protocol

The current failing regression for lease loss before publish should remain until the new protocol is implemented and passing.

## Rejected Alternatives

### Keep `writer_lease` and `indexed_head` as separate correctness rows

Rejected because the protocol cannot avoid a race without a stronger multi-row atomic primitive.

### Generic multi-key transaction trait

Rejected because:

- it is broader than the protocol needs
- it does not match the cheapest strong primitive in either Scylla or DynamoDB
- it would make the hot path more complex and less portable

### Timed leases as the base design

Rejected for the first implementation because they introduce:

- clock dependence
- renewal cadence concerns
- more takeover edge cases

Explicit takeover with monotonic epoch is sufficient for the base protocol.

## Open Questions

These should be resolved during implementation, but they do not change the core protocol.

1. whether `owner_id` should be process-generated random identity, deployment-assigned identity, or both
2. whether backfill publication should be size-based, time-based, or operator-configured
3. whether low-level write fencing remains global or becomes publication-store-driven per backend
4. whether an explicit publication conflict error distinct from `LeaseLost` is worth exposing internally
