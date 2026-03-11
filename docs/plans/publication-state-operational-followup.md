# Publication State Operational Follow-Up

This document captures the remaining design and implementation gaps after the
`publication_state` cutover and the first review-driven remediation pass.

It is a companion to:

- `publication-state-architecture.md`
- `publication-state-implementation-plan.md`
- `publication-state-review-followup.md`

The earlier review follow-up doc is primarily an implementation-remediation record.
This document exists because the remaining issues are no longer just local code
defects. They now include operational semantics:

- what roles nodes may run
- when a writer may take ownership
- what stale-writer fencing is actually expected to guarantee
- which helpers are observational vs ownership-taking

## Why A New Doc

Yes, at this point a new doc is the right shape.

The prior follow-up document has become a mixed record of:

- deficiencies found during the first review
- implementation goals that have since been addressed
- still-open protocol work

The remaining work is qualitatively different. It is about the steady-state
ownership model and operational behavior of reader-only vs reader+writer nodes,
not just patching individual implementation defects.

That deserves a separate current planning document.

## Current Review Status

The review findings now split cleanly into two groups.

### Fixed in the current branch

These issues were found in review and have now been corrected:

- publication acquisition no longer accepts a foreign owner after losing bootstrap
- reacquiring with the same numeric owner forces an epoch bump
- ingest and startup cleanup use publication epoch as the fence token
- `list_prefix` pagination no longer repeats the final entry across pages

Those fixes materially improved the protocol implementation.

### Still open

These issues remain open after the latest review:

- backend fence state is not advanced as part of real ownership takeover
- `startup_plan` is not observational; it can acquire ownership and bump epoch
- the system still has no notion of a healthy primary that should resist takeover

Those are not polish items. They define the operational behavior of the write
topology.

## Decisions

The following decisions are now recommended as the concrete plan.

### 1. Keep two node roles

- `reader only`
  - never attempts publication ownership
  - only reads `publication_state` and reader-visible artifacts
- `reader + writer`
  - may ingest finalized blocks
  - may become primary writer under explicit takeover rules

### 2. Do not keep opportunistic takeover as the end state

The intended system behavior is not:

- any caught-up RW node may seize ownership whenever it starts

The intended behavior is:

- a healthy primary keeps ownership
- standby RW nodes may catch up and remain ready
- takeover happens only when the primary is stale or explicitly replaced

### 3. Use an in-band renewable lease

The recommended liveness model is:

- keep ownership and visibility in `publication_state`
- add explicit lease freshness to that record
- allow takeover only after lease expiry or explicit administrative action

This is preferred over:

- external leader election as the default
- operator-pinned primary as the long-term design

### 4. Add process identity separate from node identity

`owner_id` should mean node identity.

A new `session_id` should mean process identity.

That gives the system a clean distinction between:

- "same node, same process"
- "same node, restarted process"
- "different node"

### 5. Make startup APIs explicit

- `startup_plan` must be read-only or be deleted
- ownership-taking startup must be explicit
- no helper with a planning/inspection name may bump epoch or take ownership

### 6. Treat production fence advancement as required follow-up, but secondary to lease correctness

We still need real fence advancement during takeover.

But the most important behavioral guarantee is:

- a standby RW node must not take ownership while the primary lease is still fresh

That should be solved first and explicitly.

## Intended Node Roles

The intended deployment model should be:

- `reader only`
  - never attempts publication ownership
  - only reads `publication_state` and reader-visible artifacts
- `reader + writer`
  - may ingest finalized blocks
  - may become primary writer under explicit takeover rules

The important operational requirement is:

- a newly started RW node should be able to come up, catch up, and be ready
- but it should not become primary if another node is already a healthy primary

That is not what the current protocol guarantees.

## Current Behavior vs Desired Behavior

### Desired behavior

If node A is the healthy primary writer and node B starts as another RW node:

1. B may observe current ownership
2. B may catch up read-side state
3. B may be ready to take over
4. B must not steal primary ownership while A is still healthy

### Current behavior

Today, any RW node can attempt ownership CAS and take `publication_state` if it
wins that CAS.

There is no built-in concept of:

- health
- liveness freshness
- lease expiry
- heartbeat
- preferred primary
- controlled standby behavior

So the current behavior is effectively:

- any sufficiently synchronized RW node may seize ownership
- the old writer then loses by epoch/CAS and, ideally, by fencing

That is a valid single-writer mutual exclusion mechanism, but it is not the same
thing as healthy-primary preservation.

## Proposed End-State Record

The recommended shape is:

```rust
PublicationState {
    owner_id: u64,
    session_id: [u8; 16],
    epoch: u64,
    indexed_finalized_head: u64,
    lease_expires_at_ms: u64,
}
```

Semantics:

- `owner_id`
  - stable node identity
- `session_id`
  - per-process identity generated on startup
- `epoch`
  - monotonic fencing and ownership version
- `indexed_finalized_head`
  - reader-visible publication watermark
- `lease_expires_at_ms`
  - liveness boundary for takeover

## Ownership And Lease Rules

For an RW node with `(owner_id = W, session_id = S)`:

1. load `publication_state`
2. if absent, attempt bootstrap create with `(W, S, epoch = 1)`
3. if present and `(owner_id, session_id) == (W, S)`, renew lease in place by CAS
4. if present with a different session and lease is still fresh, do not take over
5. if present with a different session and lease is expired, attempt takeover by CAS to:
   - `owner_id = W`
   - `session_id = S`
   - `epoch = current.epoch + 1`
   - same `indexed_finalized_head`
   - fresh `lease_expires_at_ms`
6. retry only on CAS races, not on fresh-lease rejection

This gives the desired operational behavior:

- a healthy primary keeps ownership
- a standby RW node can come up and be ready
- a restarted node gets a fresh session and epoch
- takeover is gated by explicit lease freshness

## Remaining Deficit 1: Fence Advancement Is Not Integrated Into Acquisition

### Problem

The current code now uses publication epoch as the write fence token, which is
correct as far as it goes.

But the backend fence state itself is still not advanced by the real ownership
acquisition path.

In practice that means:

- ownership takeover bumps `publication_state.epoch`
- later writes use the new epoch as `FenceToken`
- but the backend’s minimum allowed epoch is not updated as part of takeover

So stale-writer suppression at the artifact-write layer is not actually active.
Old writers are still mostly stopped by publication CAS failure at the end of
ingest rather than being fenced before artifact publication.

That is a protocol gap, especially for distributed/fenced backends such as
Scylla.

### Observable consequence

A stale writer can still write immutable artifacts after another writer has
taken over, and only later fail publication CAS.

That means takeover does not yet provide the intended pre-publication stale
writer suppression.

### Required fix

Fence advancement must become part of real acquisition, not a test-only helper
or an optional side effect.

That means one of:

1. acquisition updates backend fence state immediately before returning the new
   lease
2. backend write authorization derives directly from the authoritative
   `publication_state.epoch`
3. the backend exposes an atomic ownership-plus-fence primitive

The third option is the most principled one for distributed backends.

### Important nuance

A naive "CAS ownership, then separately update fence row" sequence is better
than today but still leaves a race window between those two steps.

If the goal is strict stale-writer suppression, the end state should be:

- either one atomic takeover operation
- or write-time fence checks that are directly tied to the authoritative
  ownership record

## Remaining Deficit 2: `startup_plan` Has Surprising Side Effects

### Problem

`startup_plan` now acquires publication ownership and can bump epoch as a side
effect.

That is a semantic mismatch. The name reads like an observational helper, but
the implementation is now an active ownership-taking path.

That creates two problems:

- callers can accidentally steal/bump ownership while "just planning"
- the codebase no longer has a clean boundary between observational startup
  inspection and ownership-taking startup

### Required fix

Split the startup surface into two explicit categories:

- read-only startup planning
- active startup ownership/recovery

For example:

- `startup_plan(...) -> RecoveryPlanView`
- `startup_with_writer(...) -> (RecoveryPlan, PublicationLease)`

The read-only form should:

- load `publication_state`
- derive `next_log_id`
- inspect warm-state / markers if needed
- never mutate ownership

If there is no legitimate read-only caller, then the better choice is to delete
`startup_plan` entirely rather than keeping a misleading helper name.

## Remaining Deficit 3: Healthy-Primary Preservation Is Not Encoded Yet

### Problem

The current publication protocol answers:

- who currently owns the writer role
- whether a writer lost ownership

It does not answer:

- whether the current owner is healthy
- whether another RW node should refrain from takeover

Without that information, a newly started RW node cannot distinguish:

- "the primary is dead, takeover is required"
- "the primary is healthy, remain standby"

### Why this matters

The intended node model is not "any RW node may write whenever it wants."

The intended model is closer to:

- many readers may exist
- one active primary writer exists
- standby RW nodes may be hot and caught up
- takeover should occur only when the primary is stale or explicitly replaced

### Current model

The current model is effectively:

- single writer by CAS ownership
- opportunistic takeover permitted to any RW node

That may be acceptable for bring-up or a very simple deployment, but it is not a
good steady-state operational model for primary/standby writers.

## Should We Keep "Any Caught-Up RW Node Can Write"?

No. That should not be the intended end state.

It is workable, but it has undesirable operational properties:

- healthy primaries can be displaced by node restarts or rollouts
- ownership can flap under benign conditions
- startup of a standby RW node is not a safe no-op
- the system relies on fencing and CAS to clean up churn rather than preventing
  unnecessary takeover

That is the wrong default for a service that expects one healthy primary and one
or more warm standbys.

## Chosen Direction

The chosen direction for the next implementation plan should be:

- keep `publication_state` as the correctness and visibility record
- extend it with `session_id` and renewable lease expiry
- make RW standby startup observational by default
- allow takeover only when the current lease is expired or replacement is explicit
- keep epoch-based fencing and integrate real fence advancement during takeover

This preserves the current control-plane design while making the operational
behavior correct for:

- reader-only nodes
- active primary writers
- warm RW standby nodes
- safe failover

## Concrete Implementation Plan

## Phase 1: API split and observational startup

1. Make `startup_plan` read-only again, or delete it if no true read-only caller
   remains.
2. Introduce an explicit ownership-taking API, for example:
   - `activate_writer(...)`
   - or `startup_with_ownership(...)`
3. Ensure RO nodes never call ownership-taking paths.

## Phase 2: Extend `publication_state`

1. Add:
   - `session_id`
   - `lease_expires_at_ms`
2. Update:
   - codecs
   - stores
   - tests
   - current-state docs
3. Generate `session_id` on process startup for RW nodes.

## Phase 3: Lease-based ownership behavior

1. RW startup loads current state before attempting takeover.
2. If another session owns a fresh lease, remain standby.
3. If the lease is expired, attempt takeover by CAS with:
   - new `session_id`
   - incremented `epoch`
   - refreshed `lease_expires_at_ms`
4. If the local process already owns the lease, renew by CAS.

## Phase 4: Ingest and renewal behavior

1. Ingest requires an active local lease.
2. Before writing, renew if the lease is near expiry.
3. If renewal fails because another session owns the lease, stop writing.
4. Do not allow opportunistic ownership steal during ordinary ingest startup.

## Phase 5: Fence hardening

1. Add a production fence-advance path during successful takeover.
2. Prefer an atomic ownership-plus-fence primitive where the backend supports it.
3. If atomicity is not available immediately, land the best ordered update path
   and document the remaining race precisely.

## Phase 6: Tests

The following tests should exist and pass:

- read-only startup does not mutate ownership
- `startup_plan` does not bump epoch or steal ownership
- standby RW node does not take over while the primary lease is fresh
- standby RW node does take over after lease expiry
- same owner restart gets a new session and bumped epoch
- stale writer cannot continue ingest after lease loss
- takeover fences stale writer before artifact publication
- explicit failover / forced replacement path behaves as expected

## Relationship To The Earlier Follow-Up Doc

`publication-state-review-followup.md` should continue to be treated as the
implementation remediation record for the first review wave.

This document should be treated as the next planning artifact for:

- startup API semantics
- writer liveness and lease behavior
- operational topology for RO and RW nodes
- remaining fence-integration work

## Bottom Line

The cutover is now much closer to sound, but the remaining work is no longer
mainly about local correctness bugs.

It is about making the control plane mean the right thing operationally:

- who may write
- when takeover is allowed
- how healthy primaries retain ownership
- how stale writers are fenced
- which startup paths are observational vs active

The next implementation plan should therefore target:

- `session_id + renewable lease + startup split`

and treat deeper fence hardening as the immediately following step.
