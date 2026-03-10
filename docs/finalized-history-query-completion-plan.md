# Finalized History Query Completion Plan

This document describes how to finish the finalized-history-query migration in a principled way from the current partially-converged state.

The wave-1 public/service surface is mostly in place already:

- transport-free `query_logs`
- `ExecutionBudget`
- `QueryPage` / `QueryPageMeta`
- `QueryOrder::Ascending`
- `FinalizedHistoryService`

The remaining work is mostly architectural convergence and ownership cleanup rather than feature expansion.

## Principles

- keep wave 1 log-only
- do not pull in relation hydration, future families, tag parsing, or storage redesign
- remove duplicate architectures quickly rather than maintaining both old and new paths
- prefer shared/log-specific view splits over persisted-byte redesign
- keep each PR narrow and leave the tree green after each step

## Finish Sequence

### 1. Retire the legacy query stack

Remove the old `src/query/` path and any dead types that only existed for:

- `query_finalized(...) -> Vec<Log>`
- `QueryOptions`
- in-crate `block_hash` query handling

Exit criteria:

- no production code depends on `src/query/`
- the only active query path is `query_logs`
- no old query-only request types remain in active modules

### 2. Finish shared-vs-log-specific ownership

Keep persisted bytes unchanged, but make ownership cleaner:

- shared code should consume `FinalizedHeadState`, `BlockIdentity`, `BlockRef`, and page/range types
- log-specific code should own `next_log_id`, `first_log_id`, `count`, locators, and log payload codecs
- shared modules should not import log filter types or log-only metadata fields directly

Exit criteria:

- `core/` is family-agnostic
- mixed persisted records are hidden behind shared/log-specific views

### 3. Extract a real stream substrate

Expand `streams/` so it owns:

- manifest/tail/chunk types and codecs
- append + seal lifecycle
- overlap/planning helpers

Keep log-specific stream naming and fanout under `logs/`.

Exit criteria:

- ingest orchestrates stream operations instead of implementing stream internals directly
- `streams/` is more than just key helpers

### 4. Tighten the logs family boundary

Keep `logs/` responsible for:

- filter semantics
- block-window to log-window mapping
- exact match
- materialization
- broad-query block-scan fallback
- stream fanout

Remove leftover shims that do not add real structure.

Exit criteria:

- `logs/` reads like one coherent family adapter
- shared code no longer knows topic semantics

### 5. Lock the public surface and docs

The intended service boundary is:

- `query_logs`
- finalized ingest
- operational service methods such as maintenance, GC, and pruning

Update migration and onboarding docs so they describe the final state directly.

Exit criteria:

- docs match code
- no docs depend on deleted modules or stale architecture

### 6. Finish the focused test matrix

Keep end-to-end tests, and add focused tests for:

- shared state/codecs
- range resolution
- stream writer behavior
- overlap estimation
- candidate execution
- log window resolution

Exit criteria:

- regressions localize to the correct layer
- the wave-1 architecture is enforced by tests, not only by convention

## Recommended PR Order

1. remove legacy `src/query/*`
2. finish shared/log-specific state and codec ownership cleanup
3. extract the stream substrate
4. tighten `logs/` family boundaries and remove shims
5. update docs and focused tests to match the final structure

## Definition Of Done

The migration is complete when all of the following are true:

- `src/query/` is gone
- `core/` is transport-free and family-agnostic
- `streams/` owns stream internals, not just keys
- `logs/` owns log semantics
- the public API is centered on `query_logs`, ingest, and operational service methods
- onboarding and migration docs describe the implemented architecture directly
