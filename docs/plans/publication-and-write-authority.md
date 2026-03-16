# Publication And Write Authority

## Summary

This is the active plan for finalized-history publication and writer ownership.

The current foundation is already in place:

- `publication_state` is the sole reader-visible publication watermark
- ownership, epoch, session identity, and lease freshness live in that same record
- `WriteAuthority` separates publication protocol from ingest orchestration
- `LeaseAuthority` and `SingleWriterAuthority` are both implemented
- startup cleanup and marker repair run under acquired write authority

The remaining work is mostly about the lease model and operational behavior. The main open items
are:

- how explicitly the system should surface `reader-only` vs `reader+writer` roles
- moving lease freshness from wall-clock time to upstream-finalized-block validity
- what takeover paths should exist besides ordinary lease-expiry takeover
- how deployment guidance should distinguish lease-backed multi-writer mode from fail-closed
  single-writer mode

This document replaces the older overlapping plan set now kept under `docs/plans/superceded/`.

## Current State

The current implementation uses:

```rust
PublicationState {
    owner_id: u64,
    session_id: [u8; 16],
    epoch: u64,
    indexed_finalized_head: u64,
    lease_expires_at_ms: u64,
}
```

Current semantics:

- `indexed_finalized_head` is the only reader-visible publication head
- `epoch` is the fencing clock for write-side artifact mutation and cleanup
- `owner_id` identifies the node
- `session_id` identifies the process instance
- `lease_expires_at_ms` gates takeover in lease-backed mode

Readers must treat `publication_state.indexed_finalized_head` as the only visibility watermark.

## Intended End State

The carried-forward lease model should use the upstream finalized block number as the external
lease clock instead of wall-clock time.

Target shape:

```rust
PublicationState {
    owner_id: u64,
    session_id: [u8; 16],
    epoch: u64,
    indexed_finalized_head: u64,
    lease_valid_through_block: u64,
}
```

Target semantics:

- `indexed_finalized_head`
  - the only reader-visible publication head
- `epoch`
  - the fencing and ownership version
- `owner_id`
  - stable node identity
- `session_id`
  - process identity
- `lease_valid_through_block`
  - inclusive external-block lease validity bound

Validity rule:

- a lease is valid while `observed_upstream_finalized_block <= lease_valid_through_block`
- takeover is allowed once `observed_upstream_finalized_block > lease_valid_through_block`

This removes wall-clock skew from the lease protocol and replaces it with a shared external clock
all contenders can observe.

## Landed Design

### Write authority boundary

Leadership is behind `WriteAuthority`.

That boundary owns:

- acquisition
- renewal
- publish/head advancement
- fence-token derivation

The ingest engine owns:

- finalized-sequence validation
- artifact persistence
- compaction and recovery orchestration

### Startup behavior

Writer startup is explicit and authoritative:

1. acquire write authority
2. clean any unpublished suffix above the published head
3. repair sealed open-page markers
4. derive the next local write position

`startup_plan(...)` is observational only.

### Mutability model

Published data-path artifacts are immutable once created.

Shared mutable state is intentionally narrow:

- `publication_state`
- `open_stream_page/*` inventory
- backend fence state where applicable

## Lease Clock Model

### External clock source

The lease clock should be:

- the latest observed finalized block number from the upstream source

It should not be:

- wall clock time
- `publication_state.indexed_finalized_head`
- locally inferred "blocks we have ingested"

All contenders must make lease decisions against the same external finalized-block observation
model.

### Fail-closed rule

If a node does not have a current observed upstream finalized block number, it should fail closed.

That means it must not:

- acquire ownership
- renew ownership
- publish a new head
- continue owner-only maintenance or GC work

Using the last observed block number after observation has been lost should not be allowed.

### Config shape

The intended configuration is block-based:

- `publication_lease_blocks`
- `publication_lease_renew_threshold_blocks`

Recommended defaults:

- `publication_lease_blocks = 10`
- `publication_lease_renew_threshold_blocks = 2`

Required invariant:

- `publication_lease_renew_threshold_blocks < publication_lease_blocks`

Renewal rule:

- renew when the observed upstream finalized block enters the configured renew window
- otherwise reuse the current lease without paying unnecessary renewal cost

This lets lease checks and renewals happen less frequently than once per block while still using a
simple shared clock.

## Takeover Flow

The expected standby-writer takeover path is:

1. standby observes the upstream finalized block number continuously
2. standby loads `publication_state`
3. standby compares `observed_upstream_finalized_block` to `lease_valid_through_block`
4. if the observed block is still within the validity window, standby remains passive
5. once the observed block is past the validity bound, standby may attempt takeover by CAS
6. the winning CAS writes:
   - the standby's `owner_id`
   - a fresh `session_id`
   - `epoch = current.epoch + 1`
   - unchanged `indexed_finalized_head`
   - `lease_valid_through_block = observed_block + publication_lease_blocks`
7. after acquisition, the new primary:
   - advances backend fence state
   - cleans any unpublished suffix above the published head
   - repairs stale open-page markers
   - derives next local sequencing state

If the takeover CAS fails, the standby must reload `publication_state` and re-evaluate against the
latest observed upstream finalized block rather than retrying blindly.

## Ownership Rules

### Healthy primary retention

The intended steady-state behavior is:

- a healthy primary keeps ownership
- standby writers may observe and remain ready
- standby writers must not seize ownership while the current lease is still valid

### Same-node restart

Reacquiring with the same node identity must still bump `epoch`.

Node identity is not process identity, so a restarted process must not silently inherit an old
lease.

### Maintenance and GC

Owner-only maintenance and GC should use the same lease-authority validation path as ingest.

They should require a current observed upstream finalized block and fail closed when that
observation is unavailable.

## Remaining Active Work

### 1. Make node roles explicit in deployment shape

The intended roles are:

- `reader-only`
  - never attempts ownership
  - reads `publication_state` and published artifacts only
- `reader+writer`
  - may acquire ownership
  - may ingest finalized blocks

The code already allows these behaviors in practice, but the deployment model should be surfaced
more explicitly in APIs, constructors, and operator guidance.

### 2. Define administrative takeover and replacement paths

Ordinary lease expiry takeover exists.

What remains to define clearly is:

- operator-forced replacement of a healthy writer
- expected behavior for rolling restarts
- whether there is any preferred-primary concept beyond lease validity

The system should continue to prefer:

- healthy primary retention
- standby readiness without opportunistic ownership theft

### 3. Clarify single-writer vs lease-backed mode guidance

`SingleWriterAuthority` is intentionally fail-closed and only safe under exclusive access.

The remaining work is to make deployment guidance unambiguous:

- when to use `LeaseAuthority`
- when `SingleWriterAuthority` is acceptable
- what safety properties are deliberately absent in single-writer mode

### 4. Keep publication state out of general caching

`publication_state` is mutable and correctness-critical.

It should continue to be treated as direct control-plane state, not as part of the immutable
artifact cache.

If it is ever memoized, that should be done as a narrow freshness-aware optimization, not as part
of the main cache architecture.

## Non-Goals

- changing query semantics
- changing the immutable artifact model
- reintroducing separate lease/head rows
- using wall-clock time as the long-term lease validity model
- coupling correctness to cache residency

## Relationship To Other Plans

- The active immutable artifact cache direction is in
  `docs/plans/zero-copy-types-and-bytes-cache.md`.
- Historical publication and write-authority plans now live under
  `docs/plans/superceded/`.
