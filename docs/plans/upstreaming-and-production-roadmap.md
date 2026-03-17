# Upstreaming And Production Roadmap

## Summary

This document defines the active workstream structure for getting
`crates/finalized-history-query` into good upstream shape and then
closing the remaining production-readiness gaps.

The current crate is coherent and has strong single-process correctness
coverage, but it is not yet production-ready. The remaining work falls
into six workstreams:

1. core productization and upstreaming
2. distributed backend completion
3. recovery, GC, and maintenance
4. correctness verification matrix
5. observability and operations
6. performance, capacity, and deployment

Each workstream has its own plan stub under `docs/plans/`.

## Workstream Docs

- [core-productization-and-upstreaming.md](core-productization-and-upstreaming.md)
- [distributed-backend-completion.md](distributed-backend-completion.md)
- [recovery-gc-and-maintenance.md](recovery-gc-and-maintenance.md)
- [correctness-verification-matrix.md](correctness-verification-matrix.md)
- [observability-and-operations.md](observability-and-operations.md)
- [performance-capacity-and-deployment.md](performance-capacity-and-deployment.md)

## Why This Grouping

The workstreams are intentionally separated by the kind of risk they
reduce:

- productization reduces review and integration risk
- backend completion reduces distributed correctness risk
- recovery and maintenance reduce long-lived state drift risk
- the verification matrix reduces unknown-behavior risk
- observability and operations reduce operator blind-spot risk
- performance and deployment reduce rollout and steady-state risk

This avoids treating "production readiness" as one undifferentiated
task.

## Sequencing

Recommended sequence:

1. core productization and upstreaming
2. distributed backend completion
3. recovery, GC, and maintenance
4. correctness verification matrix
5. observability and operations
6. performance, capacity, and deployment

This order keeps the early work focused on code shape and correctness
blocking issues before moving into heavier operational work.

## Exit Model

The roadmap uses two bars:

### Upstream-ready

Good enough to send for sustained code review and staged merge into a
larger codebase.

Expected properties:

- coherent docs
- reviewable module and test shape
- core local correctness story is easy to audit
- optional or unfinished production-only features are clearly isolated

### Production-ready

Good enough to run as a real service against distributed backends with
explicit operational confidence.

Expected properties:

- distributed write/read semantics are complete
- recovery and cleanup are implemented and proven
- metrics, alerts, and runbooks exist
- SLO and soak evidence exist

## Dependency Summary

- `distributed-backend-completion` depends on `core-productization-and-upstreaming`
  for a stable review surface.
- `recovery-gc-and-maintenance` depends on `distributed-backend-completion`
  where cleanup semantics must match the real backend behavior.
- `correctness-verification-matrix` depends on
  `distributed-backend-completion` and should grow alongside
  `recovery-gc-and-maintenance`.
- `observability-and-operations` depends on stable service and backend
  behavior from the earlier workstreams.
- `performance-capacity-and-deployment` depends on all earlier
  workstreams, because benchmarking incomplete or weakly observable
  behavior gives misleading evidence.
