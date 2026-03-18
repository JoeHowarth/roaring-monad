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

- [(01-core-productization-and-upstreaming.md)]((01-core-productization-and-upstreaming.md))
- [(02-distributed-backend-completion.md)]((02-distributed-backend-completion.md))
- [(04-correctness-verification-matrix.md)]((04-correctness-verification-matrix.md))
- [(05-observability-and-operations.md)]((05-observability-and-operations.md))
- [(06-performance-capacity-and-deployment.md)]((06-performance-capacity-and-deployment.md))

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

## Shared Assumptions

The active plans assume the following shared model.

### Review-stack model

- the crate will land into a monorepo as a new code area rather than as
  a deep integration into an existing subsystem
- the primary upstreaming problem is reviewer cognitive load, not API
  adaptation
- this repo should reach its intended end state before the design-review
  docs for the landing stack are circulated for review
- the review stack should land the full crate surface over time,
  including distributed backends and supporting benchmarking/tooling
  crates, but not as one monolithic PR
- each review PR should be approximately one commit
- the reviewed stack should preserve the intended end state of this repo
  as closely as possible
- temporary compatibility layers or transitional abstractions should be
  avoided unless strictly necessary to keep an intermediate PR compiling
  and reviewable
- design-review docs are reviewed out-of-band from this repo before PR
  cutting
- current-state docs should be updated in the same implementation PR
  whenever that PR changes architecture, terminology, storage layout, or
  user-visible behavior

### Verification model

- normal PR CI will compile the full feature surface, including the
  distributed-store feature
- distributed integration tests are not part of normal PR CI
- distributed integration validation is still required and will run as a
  manual merge gate for every PR in the review stack
- benchmark evidence should be attached to relevant review PRs when they
  introduce or wire performance-sensitive features such as caching

### Concurrency model

- the intended steady-state model is one active writer at a time
- lease-backed mode enforces active-writer ownership through
  `publication_state`, lease validity, and epoch fencing
- readers use `publication_state.indexed_finalized_head` as the only
  visibility barrier
- stale writers must be rejected by epoch fencing before they can mutate
  authoritative metadata
- single-writer mode is an explicit deployment choice that relies on the
  deployment preventing concurrent writers

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

- `02-distributed-backend-completion` depends on `01-core-productization-and-upstreaming`
  for a stable review surface.
- `04-correctness-verification-matrix` depends on
  `02-distributed-backend-completion`.
- `05-observability-and-operations` depends on stable service and backend
  behavior from the earlier workstreams.
- `06-performance-capacity-and-deployment` depends on all earlier
  workstreams, because benchmarking incomplete or weakly observable
  behavior gives misleading evidence.
