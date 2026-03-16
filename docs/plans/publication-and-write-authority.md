# Publication And Write Authority

## Summary

This is the active plan for finalized-history publication and writer ownership.

The current foundation is already in place:

- `publication_state` is the sole reader-visible publication watermark
- ownership, epoch, session identity, and lease freshness live in that same record
- `WriteAuthority` separates publication protocol from ingest orchestration
- `LeaseAuthority` and `SingleWriterAuthority` are both implemented
- startup cleanup and marker repair run under acquired write authority

The remaining work is operational, not architectural. The main open questions are:

- how explicitly the system should surface `reader-only` vs `reader+writer` roles
- what takeover paths should exist besides ordinary lease-expiry takeover
- how deployment guidance should distinguish lease-backed multi-writer mode from fail-closed
  single-writer mode

This document replaces the older overlapping plan set now kept under `docs/plans/superceded/`.

## Current State

The crate currently uses:

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
- whether there is any preferred-primary concept beyond lease freshness

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
- coupling correctness to cache residency

## Relationship To Other Plans

- The active immutable artifact cache direction is in
  `docs/plans/zero-copy-types-and-bytes-cache.md`.
- Historical publication and write-authority plans now live under
  `docs/plans/superceded/`.
