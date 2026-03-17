# Recovery, GC, And Maintenance

## Summary

This plan defines the work required to complete the crate's long-lived
state management story: startup recovery, steady-state maintenance, and
background cleanup.

The current crate already has a meaningful recovery foundation:

- publication happens only after all authoritative artifacts are durable
- startup cleanup removes unpublished suffix artifacts above the visible
  head
- startup repair seals stale open stream pages left by interrupted
  ingest

Those behaviors are described in [../ingest-pipeline.md](../ingest-pipeline.md)
and [../storage-model.md](../storage-model.md).

What is still missing is the steady-state half of the story:

- `GcWorker::run_once_with_fence(...)` is still a placeholder
- `IngestEngine::run_periodic_maintenance(...)` is still a placeholder
- the GC stats and guardrails still reflect older debris categories more
  than the current storage model

Right now the crate can recover some failed writes on startup, but it
does not yet have a complete ongoing hygiene model for long-running
service operation.

## Goal

Complete the long-lived state management story so failed writes,
abandoned artifacts, and stale maintenance inventory do not accumulate
silently over time.

## Why This Matters

The crate's storage model deliberately keeps authoritative artifacts
immutable and advances visibility only through
`publication_state.indexed_finalized_head`.

That gives the system strong recovery properties, but it also creates
multiple classes of cleanup work:

- unpublished suffix artifacts written by a failed writer before publish
- summary objects created for ranges that never became visible
- stale `open_stream_page/*` markers left behind by interrupted ingest
- operator-driven pruning work such as old block-hash index cleanup
- future classes of backend debris discovered during GC

If these classes are not handled deliberately, the system risks either:

- retaining too much garbage indefinitely
- deleting acceleration artifacts unsafely
- relying on startup-only repair instead of a real steady-state model

This workstream exists to define and implement that model.

## Current State

### Startup recovery exists

The current startup path already does meaningful work:

- loads the visible finalized head
- discovers unpublished blocks above that head
- removes unpublished summaries for the same unpublished range
- deletes unpublished block artifacts in reverse order
- repairs sealed open stream pages and removes stale markers

This is a strong base.

### GC is mostly scaffolded

`GcWorker` currently exposes:

- `run_once_with_fence(...)`
- `prune_block_hash_index_below(...)`
- guardrail fields on `GcStats`

But only `prune_block_hash_index_below(...)` does real work today.

### Periodic maintenance is not implemented

`IngestEngine::run_periodic_maintenance(...)` currently returns default
stats without performing background work.

### Guardrail taxonomy needs reconciliation

The current GC stats and config talk about:

- orphan chunk bytes
- orphan manifest segments
- stale tail keys

Those names do not line up cleanly with the current storage model
described in `docs/storage-model.md`, which is centered around:

- block payload blobs
- directory fragments and summaries
- stream fragments and pages
- open-page markers

Part of this plan is deciding whether those guardrail names represent
real current cleanup classes or leftover terminology from an older
design.

## Scope

- implement real orphan and stale-artifact discovery in the GC worker
- implement real periodic maintenance behavior behind the service hooks
- verify that startup cleanup, maintenance, and GC use the same artifact
  ownership and visibility model
- define which artifacts are authoritative, which are accelerators, and
  which are safe to delete when unpublished or orphaned
- reconcile GC stats and guardrail taxonomy with the current storage
  model

## Out Of Scope

- backend transport concerns below the store contract
- dashboards and runbooks
- SLO and soak targets
- redesigning publication semantics

## Cleanup Model

This workstream should make the cleanup model explicit by separating it
into three categories.

### 1. Startup recovery

Runs when write authority is acquired or re-authorized and exists to make
the write frontier clean before new ingest begins.

Examples:

- delete unpublished suffix artifacts above the visible head
- delete unpublished summary objects for those unpublished ranges
- repair stale open-page markers that now refer to sealed pages

### 2. Periodic maintenance

Runs during healthy steady-state service operation and exists to finish
bounded maintenance tasks that are not strictly required at startup.

Examples:

- sealing or flushing maintenance work that may be deferred
- marker cleanup that does not need a full startup path
- bounded housekeeping on frontier-related metadata

### 3. GC

Runs as a background cleanup and auditing pass to detect and manage
backlog or debris that should not grow without bound.

Examples:

- orphaned acceleration artifacts
- stale metadata inventories
- operator-driven pruning targets
- guardrail measurement of cleanup debt

These categories must not be conflated. Startup recovery is correctness
critical. Periodic maintenance is service hygiene. GC is background debt
management and policy enforcement.

## Design Constraints

### Authoritative versus acceleration artifacts must stay distinct

The storage model explicitly says that compacted summaries are
acceleration artifacts and not the sole source of truth for frontier
data. Cleanup logic must preserve that distinction.

### Visibility is still defined only by publication state

Recovery and cleanup must treat `publication_state.indexed_finalized_head`
as the only visibility barrier. Cleanup must never delete published
authoritative data based on locally inferred state.

### Cleanup must remain fence-aware

Any metadata mutation or deletion performed during recovery,
maintenance, or GC must continue to obey write-authority fencing.

### Enumeration cost matters

Some cleanup tasks rely on prefix listing over potentially large key
spaces. The plan needs to consider cost, batching, and whether a task is
appropriate for startup, maintenance, or GC.

## Work Packages

## 1. Define The Cleanup Taxonomy

### Intent

Make the crate's cleanup targets explicit and aligned with the current
storage model.

### Tasks

- enumerate all artifact classes the crate may need to recover, repair,
  or garbage-collect
- classify each class as:
  - authoritative
  - acceleration
  - inventory/control-plane
- define which cleanup phase owns each class:
  - startup recovery
  - periodic maintenance
  - GC
- reconcile GC stats and config names with the chosen taxonomy

### Deliverables

- one explicit cleanup taxonomy
- updated guardrail terminology if current names are no longer accurate

## 2. Complete Startup Recovery Semantics

### Intent

Confirm that the existing startup path handles every required
publication-before-visibility failure case cleanly.

### Tasks

- verify unpublished suffix discovery and deletion against all current
  artifact classes
- verify unpublished summary cleanup for directory and stream-page
  summaries
- verify open-page repair behavior for partially completed ingest
- identify whether any frontier artifact classes are still missing from
  startup cleanup

### Deliverables

- explicit proof that startup recovery matches the current storage model
- tests for uncovered recovery cases if gaps are found

## 3. Implement Periodic Maintenance

### Intent

Turn `run_periodic_maintenance(...)` into a real steady-state hygiene
path instead of a placeholder.

### Tasks

- define which maintenance tasks should run online while the writer is
  healthy
- implement those tasks behind the service maintenance hook
- define bounded work and reporting in `MaintenanceStats`
- ensure maintenance remains safe under lease-backed and single-writer
  modes where applicable

### Candidate Responsibilities

- bounded open-page housekeeping
- deferred sealing or summary work if any remains outside ingest
- inventory cleanup that is safe to do incrementally

### Deliverables

- non-trivial maintenance behavior
- meaningful `MaintenanceStats`
- tests for maintenance-triggered state transitions

## 4. Implement Real GC

### Intent

Turn `GcWorker::run_once_with_fence(...)` into a real audit and cleanup
pass.

### Tasks

- define which artifact classes GC owns
- implement discovery for those classes
- implement deletion where appropriate
- report meaningful stats rather than placeholder values
- decide which findings should:
  - only report
  - delete immediately
  - trigger throttle
  - trigger fail-closed

### Deliverables

- meaningful `GcStats`
- real cleanup behavior for the debris classes GC claims to manage
- guardrails driven by observed state rather than defaults alone

## 5. Expand Recovery And Cleanup Tests

### Intent

Tie the recovery, maintenance, and GC behavior to explicit scenarios.

### Tasks

- expand crash-injection and restart coverage around startup cleanup
- add tests for maintenance behavior once implemented
- add tests for GC discovery and deletion behavior
- verify that cleanup under takeover uses the correct epoch and fence
  semantics

### Deliverables

- scenario coverage for startup recovery, maintenance, and GC
- clearer mapping from failure mode to cleanup path

## 6. Align Config And Docs With The Landed Model

### Intent

Make config, docs, and code talk about the same cleanup model.

### Tasks

- update `docs/config.md` if guardrail fields or meanings change
- update `docs/ingest-pipeline.md` and `docs/storage-model.md` if the
  cleanup ownership model is clarified
- ensure service-level docs say which tasks run at startup, during
  maintenance, and during GC

### Deliverables

- docs and config that match the implemented cleanup taxonomy

## Milestones

## Milestone A: Cleanup Taxonomy Settled

Complete when:

- cleanup classes are defined explicitly
- guardrail terminology matches the current model

## Milestone B: Startup Recovery Confirmed

Complete when:

- startup cleanup and marker repair are proven against the current
  artifact set
- missing cleanup cases, if any, are closed

## Milestone C: Maintenance And GC Real

Complete when:

- maintenance performs real work
- GC performs real discovery and cleanup
- stats and guardrails are meaningful

## Milestone D: Recovery Story Production-Capable

Complete when:

- startup recovery, maintenance, and GC form one coherent lifecycle
- later operations and performance work can treat cleanup behavior as
  real rather than provisional

## Exit Criteria

- GC scans real state and returns meaningful stats
- GC deletes the classes of orphaned or stale data it claims to manage
- maintenance hooks do non-trivial useful work and are test-covered
- cleanup behavior is safe under takeover, retry, and restart
- guardrail thresholds apply to real observed backlog, not placeholder
  values
- docs and config describe the same cleanup model the code implements

## Dependencies

- depends on [distributed-backend-completion.md](distributed-backend-completion.md)
  where cleanup must be valid on the real remote stores
- should evolve alongside
  [correctness-verification-matrix.md](correctness-verification-matrix.md)
- should inform
  [observability-and-operations.md](observability-and-operations.md)
  by defining which cleanup debt and failure modes need visibility

## Risks

### Mixing correctness cleanup with best-effort cleanup

If startup recovery, maintenance, and GC are not separated clearly,
future code may either under-clean correctness-critical state or
overreact to best-effort cleanup debt.

### Guardrails measuring the wrong thing

If the guardrail taxonomy stays misaligned with the actual storage model,
operators will get misleading signals and the service may degrade for
the wrong reasons.

### Unbounded listing costs

If cleanup relies on expensive wide listings without scope or batching,
the cure can become a production problem of its own.

## Review Checklist

When this plan is implemented, reviewers should be able to answer "yes"
to all of the following:

- do startup recovery, maintenance, and GC have distinct responsibilities
- do cleanup paths respect publication visibility and fencing semantics
- do reported guardrail stats correspond to real current artifact classes
- is there test coverage for cleanup after crash, retry, and takeover

## Dependencies On Other Plans

- [distributed-backend-completion.md](distributed-backend-completion.md)
  ensures cleanup semantics are valid on the real remote stores
- [correctness-verification-matrix.md](correctness-verification-matrix.md)
  turns the cleanup cases defined here into a durable proof matrix
- [observability-and-operations.md](observability-and-operations.md)
  depends on the cleanup model to know what backlog and repair signals to
  expose

## Follow-Up Questions

- which cleanup tasks must run synchronously at startup versus
  asynchronously in maintenance
- whether some cleanup classes should fail closed immediately while
  others should only throttle
- whether GC needs its own persistent checkpoints or can remain
  stateless between runs
- whether block-hash index pruning should remain a GC concern or become a
  distinct operator maintenance path
