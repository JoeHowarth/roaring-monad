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

Concrete implementation notes:

- `docs/plans/runtime-state-and-metrics-contract.md`

## Scope

- define the metrics surface for ingest, query, publication, fencing,
  retries, cache behavior, GC backlog, and health-state transitions
- decide how those metrics are exported and consumed by a host service
- define the initial alert set and operator-facing runbooks
- make degraded and throttled transitions observable and explainable
- define operator guidance for writer replacement, restart, and failover
  procedures in lease-backed versus single-writer deployments
- define the runtime degradation state machine and make its transitions
  observable

## Out Of Scope

- distributed backend semantics implementation
- benchmark target definition and soak execution
- deployment security configuration in detail

## Exit Criteria

- the crate has a clear metrics contract for the critical correctness and
  reliability paths
- the runtime policy for healthy, throttled, degraded, and fail-closed
  behavior is defined in one place
- degraded and throttled states can be correlated with backend and
  workload behavior
- there is a documented alert and runbook path for the major failure
  classes
- operator guidance exists for healthy-writer replacement, restart, and
  failover expectations
- operators would be able to tell the difference between transient
  backend trouble, lease loss, backlog growth, and correctness-triggered
  fail-closed behavior

## Dependencies

- depends on stable behavior from `02-distributed-backend-completion`
  and `04-correctness-verification-matrix`
- should be substantially complete before
  `06-performance-capacity-and-deployment`, because benchmarks without
  observability are hard to trust

## Runtime State Policy

This workstream owns the definition of the service-level degradation
policy.

That includes:

- which conditions trigger throttled state
- which conditions trigger degraded or fail-closed state
- how backend failures, cleanup guardrails, and correctness violations
  compose
- whether some transitions are automatic, sticky, or operator-cleared

Other plans may define the inputs to this state machine, but this plan
should define the service-level policy in one place so implementation and
runbooks stay consistent.

## Follow-Up Questions

- whether the crate should export metrics directly or surface hooks for a
  host service to bind into its own telemetry stack
- what the minimum required operational footprint is for initial
  upstreaming versus real production rollout
- how much forced replacement behavior needs explicit runbook support
  before a dedicated administrative takeover mechanism exists
- how the healthy → throttled → degraded → fail-closed state machine
  should compose when multiple triggers are active at once
