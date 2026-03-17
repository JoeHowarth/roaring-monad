# Runtime State And Metrics Contract

## Summary

This document defines the implementation-level contract for runtime
state transitions and the associated observability surface for
`crates/finalized-history-query`.

The current code already has a minimal runtime-state mechanism:

- `healthy`
- `throttled`
- `degraded`

Those states are currently driven by:

- consecutive backend errors
- explicit service safety transitions such as finality or parent-linkage
  violations
- GC guardrail reactions

The current mechanism is intentionally small, but it is still too
implicit for production-grade operation. The crate needs one explicit
runtime-state contract that implementation, metrics, and runbooks all
follow.

This document is the concrete follow-up to
`docs/plans/observability-and-operations.md`.

## Goal

Define:

- the runtime degradation state machine
- trigger composition and stickiness rules
- the metrics contract for those states and their inputs
- the runbook-facing signals that operators will need

## Non-Goals

- choosing an instrumentation library
- defining dashboard layouts
- defining SLO numbers
- changing the crate's correctness model

## Current State

Today:

- `RuntimeState` stores `degraded`, `throttled`, a consecutive backend
  error counter, and a single human-readable reason
- backend successes reset the backend error counter
- backend errors can move the service from healthy to throttled to
  degraded
- `run_gc_once(...)` can set throttled or degraded state depending on
  `gc_guardrail_action`
- ingest can set degraded state on finality or parent-linkage failures
- degraded blocks query, ingest, maintenance, and GC
- throttled blocks ingest but not query

That is enough for a first local implementation, but not enough as an
long-term contract because:

- multiple triggers can overlap
- the current state representation is not compositional
- there is no explicit sticky/clear policy beyond local method behavior
- there is no defined metric family for transition causes

## Runtime States

The service-level runtime state should be defined in terms of effect on
request handling.

### Healthy

Meaning:

- no active safety restriction is in force
- query, ingest, maintenance, and GC are allowed subject to their normal
  per-operation guards

### Throttled

Meaning:

- the service is still considered readable
- the service should reject or delay new write/owner-only mutation paths
  that would increase pressure
- query may remain available if correctness is not in doubt

Expected current behavior:

- ingest is blocked
- query remains allowed
- maintenance and GC depend on the precise throttle source

### Degraded

Meaning:

- correctness confidence is no longer sufficient to continue normal
  service operation
- all externally visible service methods that depend on correct state
  should fail closed

Expected current behavior:

- query blocked
- ingest blocked
- maintenance blocked
- GC blocked

### Fail-Closed

This document treats fail-closed as a degraded-state policy, not as a
separate parallel state bit.

Meaning:

- the triggering condition requires immediate rejection of normal
  operation
- the service should present as degraded with an explicit fail-closed
  reason

Examples:

- finality violation
- parent-linkage violation
- backend failure threshold exceeded when configured to fail closed
- GC guardrail configured as `FailClosed`

## Trigger Classes

The runtime-state machine should classify triggers, not just raw errors.

### 1. Backend availability triggers

Examples:

- repeated backend read or write failures
- retry-budget exhaustion

Default effect:

- `healthy -> throttled -> degraded`

### 2. Correctness violation triggers

Examples:

- finality violation
- invalid parent linkage
- stale-state condition proven locally

Default effect:

- immediate fail-closed / degraded

### 3. Cleanup debt triggers

Examples:

- GC guardrail exceeded
- excessive orphan or stale-artifact backlog

Default effect:

- policy-driven: either throttled or degraded

### 4. Observation/authority triggers

Examples:

- upstream finalized-block observation unavailable
- lease lost
- publication conflict requiring re-acquire

Default effect:

- not always a global runtime-state transition
- often a request-level or writer-cache-level failure instead

These should still be counted and exposed, even if they do not always
flip the global service state.

## State-Transition Policy

The implementation should follow these rules.

### Rule 1. Degraded dominates throttled

If any active trigger requires degraded, the service is degraded even if
other triggers would only throttle.

### Rule 2. Correctness violations are immediately sticky

Correctness-triggered degraded state should remain until explicit
operator clear or service restart, unless a future design chooses a more
targeted recovery mechanism.

Rationale:

- these failures are not just pressure signals
- silent automatic recovery would hide serious state risk

### Rule 3. Backend-pressure throttling may auto-clear

Backend-failure throttling may clear automatically after sufficient
successes, provided no degraded trigger is active.

The current code already behaves this way for backend-error throttling.

### Rule 4. GC-triggered throttling depends on policy

GC guardrail throttling may be:

- sticky until next clean GC pass
- auto-clear on a later successful below-threshold GC pass

The current code already clears throttle when a later GC pass no longer
exceeds the configured guardrail.

### Rule 5. One reason string is not enough for the long term

The implementation should move toward a model that can preserve:

- effective runtime state
- primary cause
- all active trigger classes or at least counters per trigger class

The external human-readable reason can remain singular, but the internal
representation should be able to explain state composition.

## Recommended Internal Model

The recommended end state is:

- one effective runtime mode
  - `Healthy`
  - `Throttled`
  - `Degraded`
- one primary reason code
- per-trigger-class activity/counters
- sticky bit or policy flag for correctness-triggered degradation

Conceptually:

```rust
enum RuntimeMode {
    Healthy,
    Throttled,
    Degraded,
}

enum RuntimeTriggerKind {
    BackendPressure,
    CorrectnessViolation,
    CleanupDebt,
    ObservationOrAuthority,
}

struct RuntimeStatus {
    mode: RuntimeMode,
    primary_reason: RuntimeReasonCode,
    active_triggers: BTreeSet<RuntimeTriggerKind>,
    sticky: bool,
}
```

This does not need to land immediately as code, but metrics and
runbooks should be designed as if this is the target model.

## Metrics Contract

The metrics contract should expose both state and causes.

## 1. Runtime mode metrics

Required concepts:

- current service mode
  - healthy / throttled / degraded
- transitions into each mode
- time spent in each mode

Recommended forms:

- a single mode gauge encoded as enum/state label
- transition counters by destination mode
- duration counters/histograms if a metrics stack supports them

## 2. Trigger counters

Required concepts:

- backend-error trigger activations
- correctness-trigger activations
- cleanup-debt trigger activations
- authority/observation failure events

Recommended labels:

- trigger kind
- reason code
- operation family where applicable

## 3. Backend health metrics

Required concepts:

- consecutive backend error count
- backend success resets
- retry-budget exhaustion count
- backend error counts by broad class

These metrics explain why a mode transition happened rather than only
that it happened.

## 4. GC and cleanup metrics

Required concepts:

- latest GC guardrail measurements
- latest GC decision
  - ok / throttled / degraded
- cleanup debt gauges by class
- startup recovery work performed
- maintenance work performed

The exact debt classes depend on the final cleanup taxonomy, but the
contract should already reserve space for them.

## 5. Cache metrics

The crate already exposes per-table cache snapshots.

The metrics contract should treat these as first-class read-path
observability:

- hits
- misses
- inserts
- evictions
- resident bytes

per table:

- `BlockLogHeaders`
- `LogDirectoryBuckets`
- `LogDirectorySubBuckets`
- `PointLogPayloads`
- `StreamPageMeta`
- `StreamPageBlobs`

## 6. Publication and authority metrics

Required concepts:

- lease acquisition attempts and successes
- lease-loss events
- observation-unavailable events
- publication conflicts
- startup re-authorization failures

These are runbook-critical even when they do not all map to a global
runtime-mode change.

## 7. Distributed-backend telemetry pass-through

The current Scylla adapter already has useful telemetry concepts:

- CAS attempts
- CAS conflicts
- timeout errors
- per-operation variants

The metrics contract should preserve these concepts at the service
observability layer rather than hiding them behind a generic
"backend_error_total".

## Reason Codes

Human-readable strings are useful, but implementation and runbooks need
stable reason codes.

Recommended reason-code families:

- `backend_error_threshold`
- `gc_guardrail_throttle`
- `gc_guardrail_fail_closed`
- `finality_violation`
- `invalid_parent`
- `lease_observation_unavailable`
- `lease_lost`
- `publication_conflict`
- `operator_cleared`

The service can still expose a human-readable string, but metrics and
state transitions should key off stable reason codes.

## Runbook-Facing Signals

Operators should be able to answer these questions from the exposed
signals.

### Why is the service throttled?

Need:

- primary reason code
- trigger kind
- recent backend error count or latest GC debt measurement

### Why is the service degraded?

Need:

- primary reason code
- whether the state is sticky
- triggering event timestamp or counter bump

### Is this a backend outage or a correctness issue?

Need:

- backend error counters
- correctness-violation counters
- authority/lease event counters

### Is the service recovering?

Need:

- mode transition counters
- time since last healthy state
- recent success-after-error counters
- latest GC outcome if cleanup debt was involved

## Open Questions

These are real design questions, but none block drafting the contract.

### 1. How sticky should backend-triggered degraded state be?

Options:

- `A.` auto-clear after enough backend successes
- `B.` sticky until operator clear or restart
- `C.` sticky only if degraded was reached through correctness-sensitive
  paths, auto-clear otherwise

Recommendation:

- `C`

Reason:

- correctness-triggered degraded state should remain sticky
- backend-pressure degraded state can be allowed to recover, but only if
  it is clearly not masking a correctness-triggered condition

### 2. Should authority/observation failures affect global runtime mode?

Options:

- `A.` never; keep them request-local only
- `B.` always; any lease or observation failure degrades the service
- `C.` only for owner-only service paths or repeated observation failure
  while in writer role

Recommendation:

- `C`

Reason:

- transient writer-cache issues should not globally degrade a read-only
  path
- repeated inability to observe finalized head on a writer-capable node
  is operationally important and should be visible

### 3. Should throttled block maintenance and GC?

Options:

- `A.` yes, always
- `B.` no, maintenance and GC may still run if they are part of recovery
- `C.` policy by trigger source

Recommendation:

- `C`

Reason:

- backend-pressure throttling may warrant reducing maintenance load
- cleanup-debt throttling may still need GC to run in order to recover

### 4. Should metrics be defined as exact names now or as concept groups?

Options:

- `A.` exact names in this doc now
- `B.` concept groups here, exact names when the instrumentation stack is
  chosen
- `C.` no metrics contract until implementation

Recommendation:

- `B`

Reason:

- the crate still needs a stable observability contract
- exact metric names should follow the eventual instrumentation style of
  the host environment

## Implementation Notes

- the first implementation can keep the current `RuntimeState` shape if
  it also starts emitting the right reason codes and transition metrics
- the internal state representation can evolve later toward a richer
  trigger-aware model
- this doc should be used as the source of truth when expanding
  `docs/plans/observability-and-operations.md`

