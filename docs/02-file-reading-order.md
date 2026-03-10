# Finalized History Query File Reading Order

This is the shortest path for understanding the current implementation.

## Pass 1: Public surface

1. `crates/finalized-history-query/src/lib.rs`
2. `crates/finalized-history-query/src/api/query_logs.rs`
3. `crates/finalized-history-query/src/api/write.rs`
4. `crates/finalized-history-query/src/api/service.rs`

Look for:

- `QueryLogsRequest`
- `ExecutionBudget`
- `QueryPage`
- `QueryPageMeta`
- `FinalizedHistoryService`

## Pass 2: Shared finalized-history substrate

5. `crates/finalized-history-query/src/core/clause.rs`
6. `crates/finalized-history-query/src/core/page.rs`
7. `crates/finalized-history-query/src/core/refs.rs`
8. `crates/finalized-history-query/src/core/state.rs`
9. `crates/finalized-history-query/src/core/range.rs`
10. `crates/finalized-history-query/src/core/execution.rs`
11. `crates/finalized-history-query/src/core/runtime.rs`
12. `crates/finalized-history-query/src/streams/keys.rs`
13. `crates/finalized-history-query/src/streams/tail_manager.rs`
14. `crates/finalized-history-query/src/streams/writer.rs`

Look for:

- shared finalized-head and block-identity helpers
- block-range clipping against finalized head
- candidate execution on primary IDs
- runtime degraded vs throttled handling
- shared stream append / seal lifecycle

## Pass 3: Logs family

15. `crates/finalized-history-query/src/logs/filter.rs`
16. `crates/finalized-history-query/src/logs/types.rs`
17. `crates/finalized-history-query/src/logs/state.rs`
18. `crates/finalized-history-query/src/logs/window.rs`
19. `crates/finalized-history-query/src/logs/materialize.rs`
20. `crates/finalized-history-query/src/logs/block_scan.rs`
21. `crates/finalized-history-query/src/logs/query.rs`
22. `crates/finalized-history-query/src/logs/ingest.rs`

Look for:

- log clause semantics
- family-local block metadata reads
- block-window to log-ID-window mapping
- indexed path vs broad-query block scan
- exact pagination boundary handling
- packed-log, locator-page, and log block-metadata writes
- stream fanout during ingest

## Pass 4: Persisted bytes and storage

23. `crates/finalized-history-query/src/domain/types.rs`
24. `crates/finalized-history-query/src/codec/finalized_state.rs`
25. `crates/finalized-history-query/src/codec/log.rs`
26. `crates/finalized-history-query/src/streams/manifest.rs`
27. `crates/finalized-history-query/src/streams/chunk.rs`
28. `crates/finalized-history-query/src/store/traits.rs`

Look for:

- `MetaState`
- `BlockMeta`
- locator pages
- manifests, tails, and chunks
- store fencing and CAS behavior

## Pass 5: Ingest orchestration

29. `crates/finalized-history-query/src/ingest/engine.rs`
30. `crates/finalized-history-query/src/recovery/startup.rs`

Look for:

- finalized sequence and parent validation
- log-family delegation from ingest
- shared finalized-state persistence
- startup recovery state views

## Pass 6: End-to-end behavior

31. `crates/finalized-history-query/tests/finalized_index.rs`
32. `crates/finalized-history-query/tests/differential_and_gc.rs`

Focus on:

- resume-token pagination
- exact `has_more`
- empty-page metadata
- broad-query fallback
- ingest / query invariants
- differential correctness
