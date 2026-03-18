# Runtime State And Metrics Contract

## Purpose

This document defines the concrete service-level runtime state machine
and the corresponding metrics contract.

It is the implementation companion to
`docs/plans/observability-and-operations.md`.

## Goal

Define:

- healthy, throttled, degraded, and fail-closed behavior
- what inputs can trigger each state
- how triggers compose
- what metrics and signals must exist for operators

## Runtime States

### Healthy

The service accepts normal read and write work.

### Throttled

The service remains available but is intentionally limiting work or
signaling elevated risk.

Use for:

- cleanup debt or guardrail conditions that are concerning but not yet a
  correctness failure
- sustained backend trouble that should reduce pressure before a hard
  fail-closed transition

### Degraded

The service is in a fail-closed state for operations that could violate
correctness.

Use for:

- finality or parent-linkage violations
- backend failure thresholds that the service policy treats as
  correctness-threatening
- cleanup guardrail conditions configured to fail closed

### Fail-closed

For this crate, "fail-closed" is a policy outcome rather than a
separate implementation state. It normally manifests as degraded mode
that rejects risky operations.

## State Inputs

The state machine should accept inputs from three sources.

### Backend error policy

Examples:

- consecutive backend read/write failures
- retry-budget exhaustion

### Cleanup guardrail policy

Examples:

- GC debt above configured thresholds
- stale inventory or orphaned artifact backlog

### Hard correctness violations

Examples:

- finality violation
- invalid parent linkage
- lease or ownership misuse if surfaced as a service-level violation

## Composition Rules

Recommended precedence:

1. hard correctness violation
2. degraded/fail-closed trigger
3. throttled trigger
4. healthy

That means:

- degraded overrides throttled
- throttled may be cleared by recovery or backend success only if no
  stronger trigger remains
- hard correctness violations should be sticky until an explicit
  restart/reset path is taken

## Transition Rules

### Healthy → throttled

Trigger when:

- backend errors exceed the configured throttle threshold
- or cleanup guardrail policy says "throttle"

### Throttled → degraded

Trigger when:

- backend errors exceed the configured degraded threshold
- or cleanup guardrail policy says "fail closed"
- or a correctness failure occurs

### Healthy → degraded

Trigger immediately when:

- finality violation
- invalid parent linkage
- any other correctness violation explicitly classified as fail-closed

### Throttled → healthy

Allowed when:

- the triggering condition clears
- and no stronger trigger remains

Recommended recovery rule:

- do not clear throttled state on a single backend success
- require sustained backend success over a bounded configurable window so
  the service does not oscillate rapidly under intermittent dependency
  flakiness

### Degraded → healthy

Not automatic by default.

Recommended rule:

- degraded from hard correctness failure requires restart/operator
  intervention
- degraded from transient backend conditions may be allowed to clear only
  if the implementation deliberately supports that policy and it is
  documented

## Operation Gating

### Query path

- rejected in degraded mode
- may be allowed or rate-limited in throttled mode depending on host
  policy, but this crate should at minimum signal the state clearly

### Ingest path

- rejected in degraded mode
- rejected or slowed in throttled mode depending on whether throttling is
  implemented as backpressure or explicit errors

### Owner-only maintenance and GC

- must obey write-authority constraints
- should not proceed when degraded due to correctness or ownership risk

### Lease loss interaction

Lease loss is primarily handled by the write-authority subsystem rather
than by this runtime state machine.

Recommended rule:

- lease loss should block owner-only work immediately through authority
  validation
- repeated lease-loss or ownership-instability events should still be
  surfaced through the metrics and reason-code model here
- do not rely on service degraded state as the primary mechanism for
  enforcing loss of write authority

## Metrics Contract

The implementation should expose at least these conceptual metrics.

### State metrics

- current service state
- transitions by source trigger
- time spent in throttled/degraded state

### Backend reliability metrics

- backend errors by class and operation
- retry attempts
- retry exhaustion
- consecutive backend error streaks

### Correctness and ownership metrics

- lease-lost events
- publication conflicts
- fence rejections
- finality or parent-linkage violations

### Cleanup metrics

- GC debt by tracked class
- cleanup deletions by class
- maintenance actions performed

### Query and ingest metrics

- ingest request counts/latencies
- query request counts/latencies
- cache hits/misses/evictions/bytes by table

## Operator-Facing Signals

At minimum, operators should be able to answer:

- is the service healthy, throttled, or degraded
- why did it enter that state
- is the cause backend trouble, cleanup debt, or correctness failure
- did the state clear automatically or require intervention

## Open Questions

### 1. Should degraded-from-backend-failure clear automatically?

Options:

- A. yes, after sustained backend success
- B. no, any degraded state requires restart/operator action
- C. split policy: transient backend degraded may clear, correctness
  degraded may not

Recommendation:

- C

Reason:

- it preserves fail-closed behavior for correctness violations without
  forcing needless restarts for purely transient dependency trouble

### 2. Should throttling be explicit errors or internal rate limiting?

Options:

- A. explicit `Throttled` errors
- B. silent internal slowing/rate limiting
- C. host-configurable policy

Recommendation:

- A or C, but not B-only

Reason:

- operators and callers need clear feedback when the service is
  intentionally reducing work

### 3. Should state metrics be exported directly by the crate or via hooks?

Options:

- A. direct metrics implementation inside the crate
- B. abstract hooks/callbacks for a host service to export

Recommendation:

- B if monorepo conventions already exist
- otherwise start with A only if it does not fight the eventual host
  telemetry stack
