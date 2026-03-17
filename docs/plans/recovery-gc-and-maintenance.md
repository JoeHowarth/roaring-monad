# Recovery, GC, And Maintenance

## Goal

Complete the long-lived state management story so failed writes,
abandoned artifacts, and stale maintenance inventory do not accumulate
silently over time.

## Why This Matters

The crate already has recovery and cleanup structure, but the background
maintenance side is not finished:

- startup cleanup and open-page repair exist
- GC guardrails are configured
- `GcWorker::run_once_with_fence(...)` is still a placeholder
- `IngestEngine::run_periodic_maintenance(...)` is still a placeholder

Without this work, the service can recover some failures at startup, but
it does not yet have a complete steady-state hygiene model.

## Scope

- implement real orphan and stale-artifact discovery in the GC worker
- implement real periodic maintenance behavior behind the service hooks
- verify that startup cleanup, maintenance, and GC use the same artifact
  ownership and visibility model
- define which artifacts are authoritative, which are accelerators, and
  which are safe to delete when unpublished or orphaned

## Out Of Scope

- backend transport concerns below the store contract
- dashboards and runbooks
- SLO and soak targets

## Exit Criteria

- GC scans real state and returns meaningful stats
- GC deletes the classes of orphaned or stale data it claims to manage
- maintenance hooks do non-trivial useful work and are test-covered
- cleanup behavior is safe under takeover, retry, and restart
- guardrail thresholds apply to real observed backlog, not placeholder
  values

## Dependencies

- depends on `distributed-backend-completion` where cleanup must be valid
  on the real remote stores
- should evolve alongside `correctness-verification-matrix`

## Follow-Up Questions

- which cleanup tasks must run synchronously at startup versus
  asynchronously in maintenance
- whether some cleanup classes should fail closed immediately while
  others should only throttle
- whether GC needs its own persistent checkpoints or can remain
  stateless between runs
