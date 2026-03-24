# Correctness Verification Matrix

## Summary

The crate has meaningful test coverage but the proof obligations are
implicit. This plan makes them explicit: which invariants are
non-negotiable, which tests prove each one, and which suites are
required before upstreaming versus before production.

Current test surfaces:

- `tests/finalized_index.rs` — broad single-process integration
- `tests/crash_injection_matrix.rs` — publication-boundary and retry
- `tests/differential_and_gc.rs` — differential query checks, recovery
- `tests/distributed_meta_cas.rs` — basic Scylla CAS contention
- `tests/distributed_stores_integration.rs` — one distributed roundtrip
- `tests/distributed_failure_chaos.rs` — one opt-in chaos scenario

## Invariant Families

### 1. Visibility and publication

Readers only observe published state.

- `publication_state.indexed_finalized_head` is the only reader-visible
  watermark
- failed publication CAS cannot expose partial artifacts
- all authoritative artifacts for a block exist before that block is
  visible

### 2. Ownership, lease, and fencing

Only the active writer may mutate authoritative state.

- takeover bumps epoch
- stale writers are fenced before metadata mutation
- lease-backed writers fail closed when observation is unavailable
- cached writers are re-authorized before reuse

### 3. Query correctness

Query semantics are exact and stable.

- block-window clipping respects the visible finalized head
- pagination is exact
- `resume_log_id` semantics are strict and correct
- materialization and exact-match filtering do not lose or duplicate
  results
- duplicate bucket boundaries are resolved correctly

### 4. Ingest and storage layout

Ingest writes and compaction produce the intended artifact model.

- block sequence and parent linkage must be contiguous
- directory fragments are written for all required ranges
- stream page and directory compaction happen at the correct boundaries
- typed `log_id` and shard boundaries behave correctly

### 5. Recovery and cleanup

Failed or partial work is repaired safely.

- startup cleanup removes unpublished suffix artifacts
- unpublished summaries are removed before retry
- stale open-page markers are repaired safely
- cleanup uses the correct fence epoch

### 6. Runtime safety

The service degrades or throttles instead of silently continuing in a
bad state.

- backend failure thresholds drive throttled/degraded state correctly
- finality or parent-linkage violations fail closed

### 7. Idempotency and replay

Replay of already-attempted work is harmless or rejected cleanly.

- replaying an immutable artifact write with identical content is safe
- replaying a conflicting immutable write is rejected
- replaying cleanup against already-clean state is safe
- repeated startup recovery does not corrupt published state
- publish retry after failure does not expose duplicate or inconsistent
  visible state

## Readiness Bars

### Upstream-ready

Must prove:

- local semantic correctness
- local authority and publication correctness
- crash and retry behavior for publication boundaries
- baseline distributed adapter correctness
- idempotent replay for immutable writes, publish retry, cleanup retry

### Production-ready

Must additionally prove:

- distributed takeover and stale-writer behavior on real backends
- partial publish and restart recovery on real backends
- outage behavior for metadata and blob dependencies
- repeated operational recovery and replay on the real backend path

## Work Packages

### 1. Define the invariant list

Extract invariants from the architecture docs into one canonical list.
Separate correctness invariants from performance or operability goals.

### 2. Map existing tests to invariants

State what each test actually proves. Identify weak coverage, false
confidence, and blind spots.

### 3. Close single-process gaps

Fill remaining local correctness holes:

- exact pagination and resume edge cases
- storage boundary edge cases
- recovery cases not yet covered by crash-injection

### 4. Expand crash-injection coverage

Exercise the write path at boundaries where correctness depends on
ordering and invisibility of partial work:

- summary creation before failed publish
- cleanup before retry
- startup recovery after interrupted progress

### 5. Expand distributed correctness coverage

- publication conflict and retry
- takeover after expiry
- stale-writer fencing
- partial distributed writes and restart
- blob or metadata outage at critical stages

Separate cheap distributed checks from slower gated suites.

### 6. Define CI and gated pipelines

- normal PR CI: compiles the full feature surface including distributed
  backends
- manual merge gate: distributed integration validation, required for
  every review PR
- scheduled/gated: chaos and destructive scenarios

## Exit Criteria

- each core invariant is backed by at least one test path
- distributed multi-writer and failure scenarios are covered
- upstream-ready vs production-ready suites are explicitly defined
- verification does not depend on someone remembering to run ad hoc
  commands

## Risks

- **Coverage quantity vs proof quality:** many tests can still leave a
  critical invariant unproven if they all hit the same happy path.
- **Broad integration tests hiding gaps:** large end-to-end tests create
  false confidence when they don't isolate which invariant they prove.
- **Distributed smoke as distributed proof:** one roundtrip test on real
  backends does not validate takeover, fencing, or retry semantics.
