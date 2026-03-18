# Correctness Verification Matrix

## Summary

This plan defines the correctness proof surface for
`crates/finalized-history-query`.

The crate already has meaningful correctness coverage:

- single-process end-to-end ingest and query tests
- authority and publication tests
- crash-injection coverage around publication boundaries
- differential query checks against a naive implementation
- initial distributed CAS and roundtrip tests

What it does not yet have is an explicit matrix that answers these
questions directly:

- which invariants are non-negotiable
- which tests prove each invariant
- which suites are required before upstreaming
- which additional suites are required before production

This plan turns the current test corpus from "useful coverage" into a
deliberate verification strategy.

## Goal

Build an explicit verification matrix that proves the crate's
correctness properties across unit, integration, crash-injection, and
distributed failure scenarios.

## Why This Matters

The current test base is strong for single-process behavior, but
production confidence requires more than happy-path coverage and a small
number of distributed races.

What matters here is not just adding more tests, but making the intended
proof surface explicit:

- what invariants are non-negotiable
- which scenarios exercise them
- which suites are required before upstreaming versus before production

Without that structure, the project risks both over-testing and
under-proving:

- over-testing, by adding many cases without knowing what they prove
- under-proving, by assuming broad integration tests cover distributed or
  crash semantics they do not actually exercise

## Current State

The current test surfaces are roughly:

- `tests/finalized_index.rs`
  - broad single-process integration coverage
- `tests/crash_injection_matrix.rs`
  - publication-boundary and retry scenarios
- `tests/differential_and_gc.rs`
  - differential query checking and recovery/GC smoke checks
- `tests/distributed_meta_cas.rs`
  - basic Scylla CAS contention checks
- `tests/distributed_stores_integration.rs`
  - one distributed happy-path roundtrip
- `tests/distributed_failure_chaos.rs`
  - one opt-in chaos scenario

This is a useful base, but the proof obligations are still implicit
rather than named and tracked.

## Scope

- define the invariant list the crate must uphold
- map each invariant to one or more test categories
- expand distributed tests for takeover, partial publish, retry,
  compaction, cleanup, and outage scenarios
- expand crash-injection coverage where state can be left partially
  written
- identify which suites should run in normal CI versus controlled
  pre-prod pipelines
- distinguish upstream-ready verification from production-ready
  verification

## Out Of Scope

- backend implementation work itself
- operational telemetry design
- performance benchmarking except where needed to make tests realistic
- replacing clear example-based tests with property or model-based tests
  unless that strengthens a specific proof obligation

## Verification Philosophy

This workstream should treat correctness as a layered proof problem.

### Layer 1. Local semantic correctness

Does the crate behave correctly in a single-process, deterministic
environment with in-memory or filesystem doubles?

### Layer 2. Crash and retry correctness

Does the crate remain correct when writes fail at specific boundaries and
then retry or recover?

### Layer 3. Distributed backend correctness

Do the real distributed adapters preserve the same publication,
fencing, and visibility semantics under contention and failure?

### Layer 4. Production-path correctness

Do the deployed-path suites cover the failure modes that matter before
real traffic is allowed?

Not every test belongs at the same layer, and the plan should make that
difference explicit.

## Invariant Families

The matrix should be organized around invariant families rather than
around implementation files.

## 1. Visibility And Publication Invariants

These assert that readers only observe published state.

Examples:

- `publication_state.indexed_finalized_head` is the only reader-visible
  watermark
- failed publication CAS cannot expose partial artifacts
- all authoritative artifacts for a block exist before that block is
  visible

## 2. Ownership, Lease, And Fencing Invariants

These assert that only the active writer may mutate authoritative state.

Examples:

- takeover bumps epoch
- stale writers are fenced before metadata mutation
- lease-backed writers fail closed when observation is unavailable
- cached writers are re-authorized before reuse

## 3. Query Correctness Invariants

These assert that query semantics are exact and stable.

Examples:

- block-window clipping respects the visible finalized head
- pagination is exact
- `resume_log_id` semantics are strict and correct
- candidate materialization and exact-match filtering do not lose or
  duplicate results
- duplicate bucket boundaries are resolved correctly

## 4. Ingest And Storage Layout Invariants

These assert that ingest writes and compaction produce the intended
artifact model.

Examples:

- block sequence and parent linkage must be contiguous
- directory fragments are written for all required ranges
- stream page and directory compaction happen at the correct boundaries
- typed `log_id` and shard boundaries behave correctly

## 5. Recovery And Cleanup Invariants

These assert that failed or partial work is repaired safely.

Examples:

- startup cleanup removes unpublished suffix artifacts
- unpublished summaries are removed before retry
- stale open-page markers are repaired safely
- cleanup uses the correct fence epoch

## 6. Runtime Safety Invariants

These assert that the service degrades or throttles instead of silently
continuing in a bad state.

Examples:

- backend failure thresholds drive throttled/degraded state correctly
- finality or parent-linkage violations fail closed
- GC or cleanup guardrails trigger the configured reaction once
  implemented

## 7. Idempotency And Replay Invariants

These assert that replay of already-attempted work is either harmless or
rejected in a well-defined way.

Examples:

- replaying an immutable artifact write with identical content is safe
- replaying a conflicting immutable write is rejected
- replaying cleanup against already-clean state is safe
- repeated startup recovery does not corrupt published state
- publish retry after an earlier failure does not expose duplicate or
  inconsistent visible state

## Matrix Structure

Each invariant family should map to one or more verification layers.

### A. Unit And Focused Module Tests

Use for:

- codecs
- typed IDs
- key encoding and decoding
- range resolution helpers
- small deterministic helper logic

### B. Single-Process Integration Tests

Use for:

- end-to-end ingest/query semantics
- startup behavior
- authority behavior with in-memory stores
- cache behavior
- compaction boundary behavior

### C. Differential Tests

Use for:

- query-result equivalence against a simpler reference implementation
- future high-confidence checks for planner or materializer behavior

### D. Crash-Injection Tests

Use for:

- write-boundary failure and retry
- publication CAS failure
- cleanup after partially written unpublished state

### E. Distributed Correctness Tests

Use for:

- real Scylla CAS conflict behavior
- real MinIO object behavior
- distributed roundtrip visibility and retry
- takeover and stale-writer rejection on real backends
- validation of native conditional object-create semantics on MinIO

### F. Chaos Or Pre-Prod Gated Tests

Use for:

- dependency outage behavior
- destructive backend restart scenarios
- slower multi-process or docker-controlled scenarios

## Upstream-Ready Versus Production-Ready Bars

The matrix should define two required bars.

## Upstream-Ready

Must prove:

- local semantic correctness
- local authority and publication correctness
- crash and retry behavior for publication boundaries
- at least baseline distributed adapter correctness for the core storage
  contract
- idempotent replay behavior for immutable writes, publish retry, and
  cleanup retry

Does not yet require:

- full outage matrix
- long-running soak
- every destructive chaos scenario

## Production-Ready

Must additionally prove:

- distributed takeover and stale-writer behavior on real backends
- partial publish and restart recovery on real backends
- outage behavior for metadata and blob dependencies
- cleanup and guardrail behavior once those features are implemented
- repeated operational recovery and replay behavior on the real backend
  path

## Work Packages

## 1. Define The Invariant List

### Intent

Name the non-negotiable correctness properties once, explicitly, and use
that list as the top-level structure for the matrix.

### Tasks

- extract invariants from the active architecture docs
- normalize them into a stable list
- separate correctness invariants from performance or operability goals

### Deliverables

- one canonical invariant list for the crate

## 2. Map Existing Tests To Invariants

### Intent

Turn the current suite into a matrix by stating what each test actually
proves.

### Tasks

- map each existing test or suite to one or more invariant families
- identify weak coverage, duplicated coverage, and false confidence
  areas
- mark tests that are only smoke checks versus real invariant proofs

### Deliverables

- an initial matrix showing covered and uncovered areas

## 3. Close Single-Process Gaps

### Intent

Fill any remaining local correctness holes before relying more heavily on
distributed testing.

### Candidate Gaps

- exact pagination and resume edge cases
- storage boundary edge cases
- recovery cases not yet covered by crash-injection
- newly split maintenance and GC behavior once implemented

### Deliverables

- strong local correctness bar with explicit rationale

## 4. Expand Crash-Injection Coverage

### Intent

Exercise the write path at the boundaries where correctness depends on
ordering and invisibility of partial work.

### Tasks

- enumerate failure points in artifact publication and publish CAS
- verify which are already covered
- add missing cases for:
  - summary creation before failed publish
  - cleanup before retry
  - startup recovery after interrupted progress
  - later maintenance or GC paths once implemented

### Deliverables

- a crash-injection matrix that matches the publication model

## 5. Expand Distributed Correctness Coverage

### Intent

Prove that the real backend path preserves the same semantics as the
local path.

### Tasks

- add distributed tests for:
  - publication conflict and retry
  - takeover after expiry
  - stale-writer fencing
  - partial distributed writes and restart
  - blob or metadata outage at critical stages
- separate cheap distributed checks from slower gated suites

### Deliverables

- a distributed verification layer that proves more than a happy path

## 6. Define The Required CI And Gated Pipelines

### Intent

Make the verification budget explicit so correctness does not depend on
someone remembering to run the right ad hoc commands.

### Tasks

- define which suites must run on every change in normal PR CI
- define which suites run as a required manual integration gate for every
  review PR
- define which suites run in slower scheduled or gated jobs
- document how opt-in chaos suites fit into the release process

### Known Baseline

- normal PR CI compiles the full feature surface, including distributed
  backends
- distributed integration and docker-backed tests are not part of normal
  PR CI
- distributed integration validation still blocks every review PR and
  therefore belongs in the required manual gate rather than in the
  optional-chaos bucket

### Deliverables

- a named verification ladder from fast CI to production gate

## Milestones

## Milestone A: Invariants Named

Complete when:

- the crate has one explicit list of correctness invariants

## Milestone B: Existing Coverage Mapped

Complete when:

- current tests are mapped to invariants
- known blind spots are visible

## Milestone C: Local And Crash Matrix Complete

Complete when:

- single-process and crash-injection gaps are closed
- upstream-ready verification is clearly defined

## Milestone D: Distributed Matrix Credible

Complete when:

- distributed correctness coverage proves the real backend path
- production-ready verification is clearly defined

## Exit Criteria

- the crate has an explicit verification matrix rather than an ad hoc set
  of tests
- each core invariant is backed by at least one test path
- distributed multi-writer and failure scenarios are covered, not only
  single-process flows
- the difference between must-pass upstream suites and must-pass
  production suites is documented
- the current tests are understood as proof artifacts, not just
  regression checks

## Dependencies

- depends on [(02-distributed-backend-completion.md)]((02-distributed-backend-completion.md))
  for full distributed-path coverage
- informs
  [(05-observability-and-operations.md)]((05-observability-and-operations.md))
  by identifying which failure modes must be observable

## Risks

### Confusing coverage quantity with proof quality

Many tests can still leave a critical invariant unproven if they all hit
the same happy path.

### Letting broad integration tests hide gaps

Large end-to-end tests are useful, but they can create false confidence
when they do not isolate which invariant failed or succeeded.

### Treating distributed smoke tests as distributed proof

A single roundtrip test on real backends is not enough to validate
takeover, fencing, retry, or visibility semantics.

## Review Checklist

When this plan is implemented, reviewers should be able to answer "yes"
to all of the following:

- is there a named invariant list for the crate
- can I point to at least one test path per invariant family
- is the difference between upstream-ready and production-ready proof
  explicit
- do distributed tests cover real authority and publication conflict
  cases rather than only basic smoke checks

## Dependencies On Other Plans

- [(02-distributed-backend-completion.md)]((02-distributed-backend-completion.md))
  defines the real backend contract this matrix must verify
- [(05-observability-and-operations.md)]((05-observability-and-operations.md))
  depends on this plan to identify the failure classes that must be
  surfaced operationally

## Follow-Up Questions

- which scenarios are cheap enough for regular CI and which belong in a
  slower gated pipeline
- whether some correctness cases should be expressed as property tests or
  model-based tests rather than example-based tests
- whether the matrix should live only in docs or also as structured test
  metadata in the repo
