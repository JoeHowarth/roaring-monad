# Finalized History Query File Reading Order

This is the shortest path for understanding the implemented finalized-history-query architecture.

## Pass 1: Public surface

1. `crates/finalized-history-query/src/lib.rs`
2. `crates/finalized-history-query/src/api/query_logs.rs`
3. `crates/finalized-history-query/src/api/write.rs`
4. `crates/finalized-history-query/src/api/service.rs`

Look for:

- `QueryLogsRequest`
- `ExecutionBudget`
- `QueryPageMeta`
- `FinalizedHistoryService`
- maintenance / GC / pruning methods that remain on the concrete service

## Pass 2: Shared substrate

5. `crates/finalized-history-query/src/core/clause.rs`
6. `crates/finalized-history-query/src/core/page.rs`
7. `crates/finalized-history-query/src/core/refs.rs`
8. `crates/finalized-history-query/src/core/range.rs`
9. `crates/finalized-history-query/src/core/execution.rs`
10. `crates/finalized-history-query/src/core/runtime.rs`
11. `crates/finalized-history-query/src/streams/keys.rs`
12. `crates/finalized-history-query/src/streams/tail_manager.rs`
13. `crates/finalized-history-query/src/streams/writer.rs`

Look for:

- shared request/page primitives
- block-range clipping against finalized head
- candidate execution on primary IDs
- runtime degraded vs throttled handling

## Pass 3: Log family

14. `crates/finalized-history-query/src/logs/filter.rs`
15. `crates/finalized-history-query/src/logs/types.rs`
16. `crates/finalized-history-query/src/logs/state.rs`
17. `crates/finalized-history-query/src/logs/window.rs`
18. `crates/finalized-history-query/src/logs/materialize.rs`
19. `crates/finalized-history-query/src/logs/block_scan.rs`
20. `crates/finalized-history-query/src/logs/query.rs`
21. `crates/finalized-history-query/src/logs/ingest.rs`

Look for:

- log clause semantics
- family-local block metadata reads
- block-window to log-ID-window mapping
- indexed path vs broad-query block scan
- exact pagination boundary handling
- packed-log, log block-metadata, and locator-page writes during ingest
- stream fanout during ingest

## Pass 4: Persisted bytes and storage

22. `crates/finalized-history-query/src/domain/types.rs`
23. `crates/finalized-history-query/src/codec/finalized_state.rs`
24. `crates/finalized-history-query/src/codec/log.rs`
25. `crates/finalized-history-query/src/streams/manifest.rs`
26. `crates/finalized-history-query/src/streams/chunk.rs`
27. `crates/finalized-history-query/src/store/traits.rs`

Look for:

- `MetaState`
- `BlockMeta`
- locator pages
- manifests, tails, and chunks
- store fencing / CAS behavior

## Pass 5: End-to-end behavior

28. `crates/finalized-history-query/tests/finalized_index.rs`
29. `crates/finalized-history-query/tests/differential_and_gc.rs`

Focus on:

- resume-token pagination
- exact `has_more`
- empty-page metadata
- broad-query fallback
- differential correctness
