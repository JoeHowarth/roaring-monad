# Finalized History Query File Reading Order

This is the shortest path for understanding the current implementation.

## Pass 1: Public surface

1. `crates/finalized-history-query/src/lib.rs`
   Start here for the crate boundary: it shows which modules exist and which public types are intentionally re-exported.
2. `crates/finalized-history-query/src/api/query_logs.rs`
   Read this for the transport-free query surface: `QueryLogsRequest`, `ExecutionBudget`, and the `FinalizedLogQueries` trait.
3. `crates/finalized-history-query/src/api/write.rs`
   Read this for the write-side public boundary: it is the small `FinalizedHistoryWriter` trait and nothing else.
4. `crates/finalized-history-query/src/api/service.rs`
   Read this to see the real service behavior: query and ingest entry points, health reporting, maintenance, GC, and runtime state transitions.

Look for:

- `QueryLogsRequest`
- `ExecutionBudget`
- `QueryPage`
- `QueryPageMeta`
- `FinalizedHistoryService`

## Pass 2: Shared finalized-history substrate

5. `crates/finalized-history-query/src/core/clause.rs`
   Read this for the tiny shared clause vocabulary (`Any`, `One`, `Or`) that log filters build on.
6. `crates/finalized-history-query/src/core/page.rs`
   Read this for the pagination/result vocabulary: query order, page items, and the metadata fields the query path must fill exactly.
7. `crates/finalized-history-query/src/core/refs.rs`
   Read this for the shared `BlockRef` type used in page metadata and internal block lookups.
8. `crates/finalized-history-query/src/core/state.rs`
   Read this for shared state projections from persisted bytes: finalized head and block identity loaders.
9. `crates/finalized-history-query/src/core/range.rs`
   Read this to understand block-range validation and clipping against the indexed finalized head, including empty-range behavior.
10. `crates/finalized-history-query/src/core/execution.rs`
    Read this for the shared matched-primary vocabulary that the query path still uses for pagination metadata and benchmark helpers.
11. `crates/finalized-history-query/src/core/runtime.rs`
    Read this for the in-memory degraded/throttled state machine driven by backend failures and guardrails.
12. `crates/finalized-history-query/src/streams/keys.rs`
    Read this for the stream/storage key helpers re-exported from the domain layer plus tail-key parsing.
13. `crates/finalized-history-query/src/streams/tail_manager.rs`
    Read this for the minimal tail read/write helper around roaring tails before looking at sealing logic.
14. `crates/finalized-history-query/src/streams/writer.rs`
    Read this for the real stream append lifecycle: manifest loading, tail caching, seal decisions, chunk writes, and manifest persistence.

Look for:

- shared finalized-head and block-identity helpers
- block-range clipping against finalized head
- shard-streaming candidate execution on primary IDs
- runtime degraded vs throttled handling
- shared stream append / seal lifecycle

## Pass 3: Logs family

15. `crates/finalized-history-query/src/logs/filter.rs`
    Read this first for actual log matching semantics and the broad-query signal derived from `max_or_terms`.
16. `crates/finalized-history-query/src/logs/types.rs`
    Read this for the logs-family aliases and lightweight projections like `LogSequencingState` and `LogBlockWindow`.
17. `crates/finalized-history-query/src/logs/state.rs`
    Read this for the small helpers that load `BlockMeta` and project just the log-window fields needed by queries.
18. `crates/finalized-history-query/src/logs/window.rs`
    Read this for the exact bridge from resolved block ranges to inclusive primary-ID ranges.
19. `crates/finalized-history-query/src/logs/materialize.rs`
    Read this to see how a `log_id` resolves into `block_num` and local ordinal, then into a single log via cached bucket/header lookups and range reads.
20. `crates/finalized-history-query/src/logs/block_scan.rs`
    Read this for the block-scan fallback that decodes whole block payloads sequentially when indexed execution is skipped.
21. `crates/finalized-history-query/src/logs/query.rs`
    Read this for the main query engine: request validation, range/window resolution, broad-query policy, shard-streaming indexed execution, clause-free sequential traversal, and page assembly.
22. `crates/finalized-history-query/src/logs/ingest.rs`
    Read this for the logs-family ingest details: block-keyed payload/header writes, directory-bucket maintenance, block metadata, and stream fanout collection.

Look for:

- log clause semantics
- family-local block metadata reads
- block-window to log-ID-window mapping
- indexed path vs broad-query block scan
- exact pagination boundary handling
- directory-bucket, block-log-header, block-keyed payload, and log block-metadata writes
- stream fanout during ingest

## Pass 4: Persisted bytes and storage

23. `crates/finalized-history-query/src/domain/types.rs`
    Read this for the core persisted/data model structs shared across codecs, ingest, and query paths.
24. `crates/finalized-history-query/src/codec/finalized_state.rs`
    Read this for the fixed-width encoding of `MetaState`, `BlockMeta`, and block-hash lookup values.
25. `crates/finalized-history-query/src/codec/log.rs`
    Read this for the byte encodings of three log artifacts: individual logs, directory buckets, and block log headers.
26. `crates/finalized-history-query/src/streams/manifest.rs`
    Read this for manifest and tail encodings, including the currently supported manifest versions.
27. `crates/finalized-history-query/src/streams/chunk.rs`
    Read this for the persisted roaring chunk format and its checksum validation.
28. `crates/finalized-history-query/src/store/traits.rs`
    Read this for the storage contract the rest of the crate depends on: meta CAS/fencing plus blob reads, deletes, listing, and logical range reads.

Look for:

- `MetaState`
- `BlockMeta`
- directory buckets
- block log headers
- block-keyed log blobs
- manifests, tails, and chunks
- store fencing and CAS behavior

## Pass 5: Ingest orchestration

29. `crates/finalized-history-query/src/ingest/engine.rs`
    Read this for the ingest orchestrator: sequence and parent checks, artifact persistence, concurrent stream appends, maintenance, and state advancement.
30. `crates/finalized-history-query/src/recovery/startup.rs`
    Read this for the current startup view, which is intentionally small: load finalized head, log sequencing state, and report warmed streams.

Look for:

- finalized sequence and parent validation
- log-family delegation from ingest
- shared finalized-state persistence
- startup recovery state views

## Pass 6: End-to-end behavior

31. `crates/finalized-history-query/tests/finalized_index.rs`
    Read this for the main end-to-end contract: ingest/query behavior, pagination metadata, validation errors, runtime degradation, and storage-side effects.
32. `crates/finalized-history-query/tests/differential_and_gc.rs`
    Read this for the naive differential query check plus the basic recovery and GC cleanup behaviors.

Focus on:

- resume-token pagination
- exact `has_more`
- empty-page metadata
- broad-query fallback
- ingest / query invariants
- differential correctness

## Storage note

33. `docs/04-block-keyed-log-storage.md`

Read this after the current implementation for the storage-design rationale behind `log_id -> block_num -> byte-range` resolution and block-keyed log payloads.
