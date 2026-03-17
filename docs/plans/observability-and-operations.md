# Observability And Operations

## Goal

Make the crate operable as a service by giving operators enough signal to
detect, understand, and respond to correctness, dependency, and backlog
issues.

## Why This Matters

The crate already exposes some internal state:

- degraded and throttled service state
- cache metrics snapshots
- Scylla telemetry snapshots

That is useful, but it is not yet a production observability story.
There is no clear exported metrics model, no alert strategy, and no
runbook set tied to the failure modes the service can enter.

## Scope

- define the metrics surface for ingest, query, publication, fencing,
  retries, cache behavior, GC backlog, and health-state transitions
- decide how those metrics are exported and consumed by a host service
- define the initial alert set and operator-facing runbooks
- make degraded and throttled transitions observable and explainable

## Out Of Scope

- distributed backend semantics implementation
- benchmark target definition and soak execution
- deployment security configuration in detail

## Exit Criteria

- the crate has a clear metrics contract for the critical correctness and
  reliability paths
- degraded and throttled states can be correlated with backend and
  workload behavior
- there is a documented alert and runbook path for the major failure
  classes
- operators would be able to tell the difference between transient
  backend trouble, lease loss, backlog growth, and correctness-triggered
  fail-closed behavior

## Dependencies

- depends on stable behavior from `distributed-backend-completion`,
  `recovery-gc-and-maintenance`, and `correctness-verification-matrix`
- should be substantially complete before
  `performance-capacity-and-deployment`, because benchmarks without
  observability are hard to trust

## Follow-Up Questions

- whether the crate should export metrics directly or surface hooks for a
  host service to bind into its own telemetry stack
- what the minimum required operational footprint is for initial
  upstreaming versus real production rollout
