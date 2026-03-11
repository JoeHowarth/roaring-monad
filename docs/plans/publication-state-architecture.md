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
- support for single-block publication and required batched backfill publication under the same protocol

## Goals

- define one authoritative ownership and visibility model
- eliminate the current race between `writer_lease` and `indexed_head`
- support Scylla using its strongest practical primitive for this case
- support DynamoDB without requiring a broad transaction abstraction
- keep per-block overhead acceptable for steady-state ingest
- require batched head advancement for backfill indexing
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

### Artifact taxonomy

The existing artifact layout falls into three different classes. The protocol and storage
requirements are different for each class.

#### Class A: block-scoped authoritative artifacts

These artifacts are keyed by block number or by `(stream, page, block_num)` and are part of
the authoritative published history for the corresponding blocks.

- `block_meta/<block_num>`
- `block_hash_to_num/<block_hash>`
- `block_log_headers/<block_num>`
- `block_logs/<block_num>`
- `log_dir_frag/*`
- `stream_frag_meta/*`
- `stream_frag_blob/*`

Required properties:

- deterministic from finalized block input
- immutable once successfully written
- safe to exist above the published head without becoming visible

Artifacts in this class for blocks above `publication_state.indexed_finalized_head` are:

- allowed to exist
- not reader-visible committed history
- never trusted as committed state until the head advances past them

For takeover and retry:

- a writer must never rely on unpublished artifacts from a prior owner as implicitly trusted state
- a writer must not overwrite existing Class A artifacts in place after they have been created
- takeover and retry must follow one of the explicit recovery procedures defined below

#### Class B: shared summary artifacts

These artifacts are acceleration structures written to shared keys that may summarize many blocks:

- `log_dir_sub/<sub_bucket>`
- optional `log_dir/<bucket>`
- `stream_page_meta/<stream>/<page>`
- `stream_page_blob/<stream>/<page>`

Required properties:

- derived only from already published Class A artifacts below the current head
- not required for correctness because readers can fall back to authoritative fragments
- immutable once created
- safe to be missing

These artifacts are not part of the unpublished "suffix above head" model. They must not be
treated as rewritable speculative state. Instead:

- they are created only after the source blocks they summarize are already published
- they are written with create-if-absent semantics
- if creation races, the first complete writer wins and later writers must treat the key as sealed

This distinction is required because shared summary keys are reader-preferred when present and do
not fit the simple "unpublished block suffix" category.

#### Class C: control-plane state

These records define ownership and visibility:

- `publication_state`

Optional implementation-local auxiliary state may exist, but only `publication_state` is allowed
to determine published visibility.

### Artifact mutability rules

To make stale-writer behavior actually safe, the storage contract must distinguish metadata writes
from blob writes.

For metadata keys:

- block-scoped authoritative metadata must be immutable once created
- block-scoped authoritative metadata may be written by create-if-absent directly, or by a protocol that guarantees absence before an unconditional write
- shared summary metadata must use create-if-absent or equivalent CAS-on-empty semantics

For blob/object keys:

- block-scoped authoritative blobs must use conditional create (`if absent`)
- shared summary blobs must use conditional create (`if absent`)

The architecture must not assume that stale blob overwrites are harmless. A stale writer that can
overwrite an already-visible blob can corrupt published state even if publication ownership is
correctly protected. Object-store conditional create is therefore part of the required safety model,
not an optional optimization.

The same immutability requirement applies to authoritative metadata. A stale writer that can mutate
already-visible authoritative metadata after takeover can corrupt published state even if the blob
layer is protected. The architecture therefore must not rely on low-level fencing lag plus mutable
authoritative metadata as a safe steady-state model.

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

Backfill-oriented batched publication is not optional tuning. It is a required operating mode for
this design because the peak sustained throughput regime would otherwise pay unnecessary control-plane
CAS/LWT overhead on every block while publishing data that is already naturally ordered and append-only.

The correctness rule stays unchanged:

- only blocks at or below the published head are visible

Additional required invariant:

- batch publish is legal only for a fully validated contiguous suffix

Concretely, before advancing the head from `H` to `T`, the writer must have established all of the following:

- every block in `[H + 1, T]` is present from authoritative finalized input
- the blocks in `[H + 1, T]` form a contiguous parent/sequence-valid chain
- every required Class A artifact for every block in `[H + 1, T]` exists
- no block in `[H + 1, T]` is relying on unvalidated unpublished artifacts inherited from another writer

Advancing the head to `T` without proving those conditions is invalid.

### Retry after partial failure

If a writer crashes after writing artifacts but before publish:

- those artifacts remain above the visible head
- readers must ignore them
- the same writer may reuse or complete them only under the explicit recovery rules below

If a writer crashes after publish succeeds but before receiving acknowledgment:

- retry must first reload `PublicationState`
- if the head already advanced to the intended value or beyond under the same owner/epoch, treat the publish as already committed
- if ownership changed, do not treat the old publish attempt as implicitly successful without reconciling against the current published state

This makes publish idempotent with respect to ambiguous client-visible completion.

### Unpublished suffix recovery

Recovery above the published head must be explicit. The base implementation should use cleanup-first
recovery because it is the simplest protocol to reason about under immutable Class A artifacts.

#### Cleanup-first recovery

Given current published head `H`:

1. enumerate any Class A artifacts for blocks above `H`
2. delete those unpublished artifacts
3. replay ingest from authoritative finalized input starting at `H + 1`

Properties:

- no trust is placed in unpublished artifacts from a prior owner
- no in-place overwrite of existing immutable Class A keys is required
- takeover semantics remain simple and testable

This is the recommended initial implementation.

#### Validate-and-reuse recovery

This is an allowed future optimization, not the base protocol.

Given current published head `H`:

1. enumerate Class A artifacts for blocks above `H`
2. for each block above `H`, verify that all required Class A artifacts exist and exactly match authoritative finalized input
3. reuse only the verified contiguous suffix
4. if any block is missing or mismatched, delete that block's unpublished suffix and rebuild from there forward

This optimization is only valid if the implementation proves exact equality against authoritative finalized input.

### Summary creation protocol

Shared summary creation is intentionally outside the correctness-critical publish step.

Required sequence:

1. write and publish Class A authoritative artifacts
2. after the head has advanced, derive any newly sealed summaries from published Class A artifacts
3. write the shared summary keys with create-if-absent semantics

Consequences:

- shared summaries may lag without affecting correctness
- a failed or stale summary writer cannot poison publication of new blocks
- readers may prefer summaries when present and fall back to authoritative fragments otherwise

This is required because create-if-absent on a shared summary key is only safe if the first writer
cannot win with an incomplete source set.

Additional required invariant:

- a shared summary may be created only from a complete sealed source range strictly below the current published head

Examples:

- `log_dir_sub/<sub_bucket>` only after the full sub-bucket range is below the published head
- `log_dir/<bucket>` only after the full bucket range is below the published head
- `stream_page_*` only after the page is sealed below the published head

If the backend cannot guarantee a complete view of the source fragments needed for the sealed range,
that backend profile must not create the summary synchronously under this protocol.

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
- until the fence is raised, a stale writer may still attempt some metadata writes against keys that are still absent
- blob/object writes still require conditional-create protection independently of metadata fencing
- stale writers may still waste work, but must be unable to mutate already-created immutable objects or metadata

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

### Blob/object immutability capability

The protocol also requires an explicit blob-side primitive for immutable object creation.

Conceptual shape:

```rust
enum CreateOutcome {
    Created,
    AlreadyExists,
}

#[allow(async_fn_in_trait)]
trait BlobStore: Send + Sync {
    async fn put_blob(&self, key: &[u8], value: Bytes) -> Result<()>;

    async fn put_blob_if_absent(
        &self,
        key: &[u8],
        value: Bytes,
    ) -> Result<CreateOutcome>;

    async fn get_blob(&self, key: &[u8]) -> Result<Option<Bytes>>;

    async fn delete_blob(&self, key: &[u8]) -> Result<()>;

    async fn list_prefix(
        &self,
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page>;
}
```

Required usage:

- Class A authoritative blobs use `put_blob_if_absent`
- Class B shared summary blobs use `put_blob_if_absent`
- `put_blob` remains available only for workflows whose keys are explicitly mutable by design

This capability is required because object-store immutability is part of the safety model, not merely an implementation detail.

## Write Frequency Characterization

This section characterizes the current write surface using the expected operating point and the
stated worst-case sustained throughput.

Assumptions:

- block time: `400 ms`
- normal expected throughput: `1,000 tx/s` with `1` log per transaction
- peak sustained throughput: `10,000 tx/s` with `10` logs per transaction
- therefore:
  - normal: `1,000 logs/s`
  - peak: `100,000 logs/s`
- current constants:
  - `LOG_DIRECTORY_SUB_BUCKET_SIZE = 10,000`
  - `LOG_DIRECTORY_BUCKET_SIZE = 1,000,000`
  - `STREAM_PAGE_LOCAL_ID_SPAN = 4,096`
  - shard size = `2^24 = 16,777,216` logs

Derived per-block rates:

- normal:
  - `400 tx/block`
  - `400 logs/block`
- peak:
  - `4,000 tx/block`
  - `40,000 logs/block`

### Control-plane writes

- `publication_state`
  - frequency:
    - steady state: `2.5/s` if publishing every block
    - backfill: operator-chosen batch cadence and required batched advancement
  - required write type:
    - CAS/LWT
  - reason:
    - this is the ownership and visibility gate

### Block-scoped authoritative metadata

- `block_meta/<block_num>`
- `block_hash_to_num/<block_hash>`
- `block_log_headers/<block_num>`
- `log_dir_frag/<sub_bucket>/<block_num>`
- `stream_frag_meta/<stream>/<page>/<block_num>`

Frequency:

- `block_meta` / `block_hash_to_num`: `1` per block
- `block_log_headers`: `0` or `1` per block depending on log count
- `log_dir_frag`: about `1` per covered 10k-log sub-bucket, minimum `1` per block today
- `stream_frag_meta`: once per touched `(stream, page, block)` pair

Required write type:

- create-if-absent directly, or unconditional write only when the implementation has already guaranteed key absence
- never mutable in place after successful creation

Reason:

- these keys are block-qualified and do not require shared-key arbitration in the hot path
- cleanup-first recovery allows the implementation to preserve immutability without requiring steady-state shared-key contention resolution

### Block-scoped authoritative blobs

- `block_logs/<block_num>`
- `stream_frag_blob/<stream>/<page>/<block_num>`

Frequency:

- `block_logs`: `0` or `1` per block
- `stream_frag_blob`: once per touched `(stream, page, block)` pair

Required write type:

- object-store conditional create (`if absent`)

Reason:

- stale overwrites of visible immutable objects are not safe

### Shared summary metadata

- `log_dir_sub/<sub_bucket>`
- optional `log_dir/<bucket>`
- `stream_page_meta/<stream>/<page>`

Frequency:

- `log_dir_sub`
  - normal: `1 every 10s` = `0.1/s`
  - peak: `10/s`
- `log_dir`
  - normal: `1 every 1000s` = about `0.001/s`
  - peak: `1 every 10s` = `0.1/s`
- `stream_page_meta`
  - one write per sealed stream page
  - worst-case if a single `(stream, shard)` receives all matching logs:
    - normal: `1000 / 4096 = 0.244/s`
    - peak: `100000 / 4096 = 24.4/s`
  - actual rate depends heavily on skew and stream fanout

Required write type:

- create-if-absent / CAS/LWT

Reason:

- these are shared keys and must become immutable once first created from a complete published source set

Performance guidance:

- `log_dir_sub` and `log_dir` are comfortably in CAS/LWT territory even at peak
- `stream_page_meta` is the only summary key family that can become materially frequent under hot-stream workloads
- even there, the write rate remains far below fragment write volume, and the operation is outside the correctness-critical publish path

### Shared summary blobs

- `stream_page_blob/<stream>/<page>`

Frequency:

- matches `stream_page_meta`

Required write type:

- object-store conditional create (`if absent`)

Reason:

- this is a shared reader-visible immutable object

### Shard progression and sealing pressure

One shard contains `16,777,216` logs.

- normal:
  - one shard fills in about `4.66 hours`
- peak:
  - one shard fills in about `2.8 minutes`

This matters because the peak regime increases the rate of page and shard sealing substantially,
especially for hot streams. It does not change the conclusion that:

- publication CAS belongs only on the control plane
- shared summary keys should be immutable post-publication acceleration artifacts
- block-scoped hot-path writes should avoid metadata CAS/LWT unless block-qualified create-if-absent is specifically needed

## Future Work: Layout Constant Evaluation

The current plan does not require changing storage-layout constants before the publication protocol
is implemented. However, the stated normal and peak throughput regimes make several constants worth
explicit follow-up evaluation.

These are future-work investigations, not part of the immediate protocol rollout.

### 1. Re-evaluate `LOCAL_ID_BITS`

Current value:

- `LOCAL_ID_BITS = 24`

At the stated throughput regimes:

- normal `1,000 logs/s`:
  - one shard lasts about `4.66 hours`
- peak `100,000 logs/s`:
  - one shard lasts about `2.8 minutes`

This may be acceptable, but it materially increases:

- shard churn
- stream-ID fanout
- broad-query shard traversal
- sealing pressure during sustained peak ingest

Recommended future work:

- benchmark `24`, `26`, and `28` local-ID widths
- compare query cost, stream fanout, compaction cadence, and cache behavior

### 2. Re-evaluate `STREAM_PAGE_LOCAL_ID_SPAN`

Current value:

- `STREAM_PAGE_LOCAL_ID_SPAN = 4,096`

For a single hot `(stream, shard)` page:

- normal `1,000 logs/s`: about `0.244` page seals/s
- peak `100,000 logs/s`: about `24.4` page seals/s

This is the summary family most likely to see meaningful write-rate pressure under hot-stream skew.

Recommended future work:

- benchmark `4,096` versus larger values such as `16,384`
- evaluate tradeoffs between:
  - lower summary-creation frequency
  - larger summary objects
  - wider bitmap reads
  - fallback behavior when summaries lag

### 3. Keep directory bucket sizes under observation

Current values:

- `LOG_DIRECTORY_SUB_BUCKET_SIZE = 10,000`
- `LOG_DIRECTORY_BUCKET_SIZE = 1,000,000`

At the stated throughput regimes:

- sub-bucket sealing:
  - normal: `0.1/s`
  - peak: `10/s`
- full-bucket sealing:
  - normal: about `0.001/s`
  - peak: `0.1/s`

These do not currently look like an immediate reason to change layout, but they should still be
revisited if future profiling shows:

- expensive summary creation
- high directory-summary contention
- poor query locality around bucket boundaries

### 4. Prefer operational adaptation before layout changes

Before changing layout constants, prefer operating-mode changes such as:

- backfill-oriented publication batching
- summary-creation concurrency tuning
- cache sizing adjustments
- optional lag tolerance for post-publication summary creation

This keeps the first implementation focused on correctness and allows measured layout changes later
based on real benchmark and profiling evidence.

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

- Scylla:
  - `publication_state` and metadata CAS operations must use LWT
  - readers must use read/query paths with consistency strong enough to observe already-committed metadata rows
  - correctness-sensitive `list_prefix` operations must use read/query behavior that provides complete prefix enumeration for committed rows in scope
- DynamoDB:
  - `publication_state` and metadata CAS operations must use conditional single-item writes
  - readers must use read modes that guarantee visibility of the committed item state they depend on
  - correctness-sensitive `list_prefix` operations must be implemented as strongly consistent base-table queries, not GSIs
- object/blob store:
  - immutable object creation must support conditional create (`if absent`)
  - the deployment must provide read-after-write visibility for newly created objects before the corresponding head advance is considered safe to publish
  - any correctness-sensitive object enumeration must also provide list-after-write visibility for the relevant prefixes

Correctness-sensitive enumeration includes:

- cleanup-first recovery over unpublished suffix artifacts
- shared-summary construction from sealed source fragments

Read-after-write alone is not sufficient for those workflows. They require complete prefix enumeration of the relevant source set.

Required backend notes:

- DynamoDB:
  - strong consistency is acceptable only on the base table or LSIs
  - correctness-sensitive prefix enumeration must not depend on GSIs
- S3-compatible object stores:
  - AWS S3 satisfies the required read-after-write and list-after-write object semantics
  - any alternate S3-compatible backend must prove equivalent semantics or be treated as unsupported for correctness-sensitive workflows

Required publication ordering:

1. write authoritative Class A metadata and blobs
2. ensure those writes have the backend visibility required by the deployment profile
3. advance `publication_state`
4. only after that, optionally create Class B shared summaries

If a deployment cannot provide that visibility contract, then:

- the publication protocol still preserves ownership correctness
- but reader-visible consistency at the newly published head is not guaranteed

That is a deployment contract issue, not a reason to split ownership and visibility rows again.

## Failure Matrix

The implementation must handle all of these cases explicitly.

### Case 1: stale writer writes artifacts after takeover

Allowed outcome:

- stale writer may waste work
- stale writer may create only still-absent immutable keys
- stale writer must not publish

### Case 2: stale writer attempts publish after takeover

Required outcome:

- publish CAS fails
- ingest returns `LeaseLost`

### Case 3: writer crashes before publish

Required outcome:

- head does not advance
- readers do not see the new block
- cleanup-first recovery can delete the unpublished suffix and replay it
- validate-and-reuse is allowed only if exact equality is proven against authoritative finalized input

### Case 4: writer crashes after publish success but before ack

Required outcome:

- reload reveals advanced head
- retry treats publish as already committed only after reconciling ownership token and published head state under the protocol

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

- old writer may still attempt some unfenced metadata writes or conditional-create object writes
- old writer still cannot publish
- already-created immutable objects remain protected from overwrite
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
3. add required backfill-oriented publication batching on top of the same publication protocol
4. switch finalized-head reads to `publication_state.indexed_finalized_head`

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
- stale writer cannot mutate already-created Class A metadata or blobs after takeover
- publish advances head only after artifacts exist
- retry after pre-publish crash succeeds
- retry after ambiguous post-publish crash is idempotent
- backfill batch publish advances from `H` to `T` atomically
- backfill batching reduces publication CAS frequency relative to per-block publish at peak ingest
- cleanup-first takeover deletes unpublished suffixes and rebuilds them safely
- shared summary creation only occurs from complete sealed ranges below the published head
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
