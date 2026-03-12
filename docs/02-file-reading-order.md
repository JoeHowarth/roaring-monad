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
12. `crates/finalized-history-query/src/domain/keys.rs`
    Read this for the immutable-frontier key layout: `publication_state`, open stream-page markers, directory fragments/sub-buckets, and stream fragments/pages.
13. `crates/finalized-history-query/src/streams/chunk.rs`
    Read this for the roaring bitmap blob format reused by immutable stream fragments and compacted stream pages.

Look for:

- shared finalized-head and block-identity helpers
- block-range clipping against finalized head
- shard-streaming candidate execution on primary IDs
- runtime degraded vs throttled handling
- immutable frontier key layout and roaring bitmap blob format

## Pass 3: Logs family

15. `crates/finalized-history-query/src/logs/filter.rs`
    Read this first for actual log matching semantics and which clauses count as indexed input.
16. `crates/finalized-history-query/src/logs/types.rs`
    Read this for the logs-family aliases and lightweight projections like `LogSequencingState` and `LogBlockWindow`.
17. `crates/finalized-history-query/src/logs/state.rs`
    Read this for the small helpers that load `BlockMeta` and project just the log-window fields needed by queries.
18. `crates/finalized-history-query/src/logs/window.rs`
    Read this for the exact bridge from resolved block ranges to inclusive primary-ID ranges.
19. `crates/finalized-history-query/src/logs/materialize.rs`
    Read this to see how a `log_id` resolves into `block_num` and local ordinal, then into a single log via cached bucket/header lookups and range reads.
20. `crates/finalized-history-query/src/logs/materialize.rs`
    Read this to see `log_id -> block_num -> byte-range` resolution against compacted directory summaries with immutable-fragment fallback.
21. `crates/finalized-history-query/src/logs/query.rs`
    Read this for the main query engine: request validation, range/window resolution, indexed-clause enforcement, immutable page/fragment loading, and page assembly.
22. `crates/finalized-history-query/src/logs/ingest.rs`
    Read this for the logs-family ingest details: block-keyed payload/header writes, immutable directory fragments, immutable stream fragments, and eager compaction of sealed boundaries.

Look for:

- log clause semantics
- family-local block metadata reads
- block-window to log-ID-window mapping
- indexed-only query execution
- exact pagination boundary handling
- directory fragments, sub-bucket summaries, and optional 1M summaries
- stream fragments, page summaries, and stream fanout during ingest

## Pass 4: Persisted bytes and storage

22. `crates/finalized-history-query/src/domain/types.rs`
    Read this for the core persisted/data model structs shared across codecs, ingest, and query paths.
23. `crates/finalized-history-query/src/codec/finalized_state.rs`
    Read this for the fixed-width encoding of `PublicationState`, `BlockMeta`, and block-hash lookup values.
24. `crates/finalized-history-query/src/codec/log.rs`
    Read this for the byte encodings of three log artifacts: individual logs, directory buckets, and block log headers.
25. `crates/finalized-history-query/src/store/traits.rs`
    Read this for the storage contract the rest of the crate depends on: meta CAS/fencing plus blob reads, deletes, listing, and logical range reads.

Look for:

- `PublicationState`
- `BlockMeta`
- directory fragments and directory summaries
- block log headers
- block-keyed log blobs
- stream fragments, stream pages, and bitmap blobs
- store fencing and CAS behavior

## Pass 5: Ingest orchestration

28. `crates/finalized-history-query/src/ingest/engine.rs`
    Read this for the ingest orchestrator: finalized-sequence validation, authoritative artifact persistence, bounded eager compaction, and the `WriteAuthority` handoff points.
29. `crates/finalized-history-query/src/ingest/authority.rs`
    Read this for the write-authority boundary: `WriteToken`, `WriteAuthority`, and the engine-facing contract.
30. `crates/finalized-history-query/src/ingest/authority/lease.rs`
    Read this for the lease-backed multi-writer authority: acquisition, renewal, takeover, and head publication.
31. `crates/finalized-history-query/src/ingest/authority/single_writer.rs`
    Read this for the fail-closed single-writer authority: sentinel publication rows, fence alignment, and monotonic head publication.
32. `crates/finalized-history-query/src/recovery/startup.rs`
    Read this for the current startup view: `startup_plan` is read-only, while writer startup acquires write authority, cleans any unpublished suffix, repairs sealed open-page markers, derives `next_log_id`, and reports warmed streams.

Look for:

- finalized sequence and parent validation
- log-family delegation from ingest
- publication-state persistence and derived `next_log_id`
- startup recovery state views

## Pass 6: End-to-end behavior

33. `crates/finalized-history-query/tests/finalized_index.rs`
    Read this for the main end-to-end contract: immutable-frontier publication, publication-state ownership/visibility, pagination metadata, and boundary compaction.
34. `crates/finalized-history-query/tests/crash_injection_matrix.rs`
    Read this for crash-retry behavior around authoritative artifact publication and publication-state CAS.
35. `crates/finalized-history-query/tests/differential_and_gc.rs`
    Read this for the naive differential query check plus the basic recovery and legacy-GC cleanup behaviors.

Focus on:

- resume-token pagination
- exact `has_more`
- empty-page metadata
- indexed-only query validation
- ingest / query invariants
- immutable-fragment retry behavior
- differential correctness

## Storage note

36. `docs/04-block-keyed-log-storage.md`

Read this after the current implementation for the storage-design rationale behind `log_id -> block_num -> byte-range` resolution and block-keyed log payloads.
