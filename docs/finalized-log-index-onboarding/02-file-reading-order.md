# Finalized Log Index File Reading Order

This is the recommended file order for understanding the current indexing and query core.

## Pass 1: Public shape

1. `crates/finalized-log-index/src/lib.rs`
2. `crates/finalized-log-index/src/api/query_logs.rs`
3. `crates/finalized-log-index/src/api/write.rs`
4. `crates/finalized-log-index/src/api/service.rs`
5. `crates/finalized-log-index/src/config.rs`

## Pass 2: Shared query substrate

6. `crates/finalized-log-index/src/core/clause.rs`
7. `crates/finalized-log-index/src/core/page.rs`
8. `crates/finalized-log-index/src/core/range.rs`
9. `crates/finalized-log-index/src/core/execution.rs`
10. `crates/finalized-log-index/src/core/runtime.rs`
11. `crates/finalized-log-index/src/streams/keys.rs`

## Pass 3: Log family

12. `crates/finalized-log-index/src/logs/filter.rs`
13. `crates/finalized-log-index/src/logs/types.rs`
14. `crates/finalized-log-index/src/logs/window.rs`
15. `crates/finalized-log-index/src/logs/materialize.rs`
16. `crates/finalized-log-index/src/logs/block_scan.rs`
17. `crates/finalized-log-index/src/logs/query.rs`
18. `crates/finalized-log-index/src/logs/ingest.rs`

## Pass 4: Persisted bytes and ingest

19. `crates/finalized-log-index/src/domain/types.rs`
20. `crates/finalized-log-index/src/codec/log.rs`
21. `crates/finalized-log-index/src/codec/manifest.rs`
22. `crates/finalized-log-index/src/codec/chunk.rs`
23. `crates/finalized-log-index/src/ingest/engine.rs`

## Pass 5: Validation

24. `crates/finalized-log-index/tests/finalized_index.rs`
25. `crates/finalized-log-index/tests/differential_and_gc.rs`

That set covers the current public API, storage model, ingest path, indexed query path, block-scan fallback, and pagination behavior.
