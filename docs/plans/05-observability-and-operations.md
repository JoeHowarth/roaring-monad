# Observability And Operations

## Summary

The crate exposes some internal state (degraded/throttled flags, cache
metrics, Scylla telemetry) but has no coherent metrics model, alert
strategy, or runbooks. This plan defines those.

## Runtime State Machine

### States

- **Healthy:** normal read and write work accepted.
- **Throttled:** available but limiting work or signaling elevated risk.
  Triggered by sustained backend trouble or concerning-but-not-critical
  conditions.
- **Degraded:** fail-closed for operations that could violate
  correctness. Triggered by finality/parent-linkage violations, backend
  failure past the degraded threshold, or hard correctness failures.

Fail-closed is a policy outcome, not a separate state — it manifests as
degraded mode rejecting risky operations.

### Inputs

- **Backend errors:** consecutive failures, retry-budget exhaustion.
- **Hard correctness violations:** finality violation, invalid parent
  linkage, lease/ownership misuse.

### Composition

Precedence: hard correctness violation > degraded > throttled > healthy.
Degraded overrides throttled. Hard correctness violations are sticky
until restart.

### Transitions

- **Healthy → throttled:** backend errors exceed throttle threshold.
- **Throttled → degraded:** backend errors exceed degraded threshold, or
  correctness failure.
- **Healthy → degraded:** immediate on finality/parent-linkage
  violation.
- **Throttled → healthy:** requires sustained backend success over a
  configurable window (not a single success), and no stronger trigger
  remaining.
- **Degraded → healthy:** correctness-triggered degraded requires
  restart. Backend-triggered degraded may auto-clear if deliberately
  supported and documented.

### Operation gating

- **Query:** rejected when degraded, signaled when throttled.
- **Ingest:** rejected when degraded, rejected or slowed when throttled.
- **Lease loss:** handled by the write-authority subsystem, not this
  state machine. Repeated lease-loss events surface through metrics but
  don't drive state transitions here.

## Metrics Contract

### State

- current service state
- transitions by trigger source
- time in throttled/degraded

### Backend reliability

- errors by class and operation
- retry attempts and exhaustion
- consecutive error streaks

### Correctness and ownership

- lease-lost events
- publication conflicts
- fence rejections
- finality/parent-linkage violations

### Query and ingest

- request counts and latencies
- cache hits/misses/evictions/bytes by table

## Open Questions

- **Throttling mechanism:** explicit `Throttled` errors vs internal rate
  limiting vs host-configurable policy. Recommendation: explicit errors
  or host-configurable, not silent-only.
- **Metrics export:** direct implementation vs abstract hooks for host
  telemetry. Recommendation: hooks if monorepo conventions exist.
- **Degraded auto-clear:** split policy — transient backend degraded may
  clear, correctness degraded may not.

## Exit Criteria

- runtime degradation policy defined in one place
- metrics contract covers correctness, backend reliability, and
  request paths
- operators can distinguish transient backend trouble from lease loss
  from correctness-triggered fail-closed
- alert and runbook path exists for major failure classes
