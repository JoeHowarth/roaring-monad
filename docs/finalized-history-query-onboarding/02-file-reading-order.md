# Finalized History Query File Reading Order

This is the shortest path for understanding the implemented finalized-history-query architecture.

## Pass 1: Public surface

1. `crates/finalized-log-index/src/lib.rs`
2. `crates/finalized-log-index/src/api/query_logs.rs`
3. `crates/finalized-log-index/src/api/write.rs`
4. `crates/finalized-log-index/src/api/service.rs`

Look for:

- `QueryLogsRequest`
- `ExecutionBudget`
- `QueryPageMeta`
- `FinalizedHistoryService`
- maintenance / GC / pruning methods that remain on the concrete service

## Pass 2: Shared substrate

5. `crates/finalized-log-index/src/core/clause.rs`
6. `crates/finalized-log-index/src/core/page.rs`
7. `crates/finalized-log-index/src/core/refs.rs`
8. `crates/finalized-log-index/src/core/range.rs`
9. `crates/finalized-log-index/src/core/execution.rs`
10. `crates/finalized-log-index/src/core/runtime.rs`
11. `crates/finalized-log-index/src/streams/keys.rs`
12. `crates/finalized-log-index/src/streams/tail_manager.rs`
13. `crates/finalized-log-index/src/streams/writer.rs`

Look for:

- shared request/page primitives
- block-range clipping against finalized head
- candidate execution on primary IDs
- runtime degraded vs throttled handling

## Pass 3: Log family

14. `crates/finalized-log-index/src/logs/filter.rs`
15. `crates/finalized-log-index/src/logs/types.rs`
16. `crates/finalized-log-index/src/logs/window.rs`
17. `crates/finalized-log-index/src/logs/materialize.rs`
18. `crates/finalized-log-index/src/logs/block_scan.rs`
19. `crates/finalized-log-index/src/logs/query.rs`
20. `crates/finalized-log-index/src/logs/ingest.rs`

Look for:

- log clause semantics
- block-window to log-ID-window mapping
- indexed path vs broad-query block scan
- exact pagination boundary handling
- packed-log, log block-metadata, and locator-page writes during ingest
- stream fanout during ingest

## Pass 4: Persisted bytes and storage

21. `crates/finalized-log-index/src/domain/types.rs`
22. `crates/finalized-log-index/src/codec/finalized_state.rs`
23. `crates/finalized-log-index/src/codec/log.rs`
24. `crates/finalized-log-index/src/streams/manifest.rs`
25. `crates/finalized-log-index/src/streams/chunk.rs`
26. `crates/finalized-log-index/src/store/traits.rs`

Look for:

- `MetaState`
- `BlockMeta`
- locator pages
- manifests, tails, and chunks
- store fencing / CAS behavior

## Pass 5: End-to-end behavior

27. `crates/finalized-log-index/tests/finalized_index.rs`
28. `crates/finalized-log-index/tests/differential_and_gc.rs`

Focus on:

- resume-token pagination
- exact `has_more`
- empty-page metadata
- broad-query fallback
- differential correctness
