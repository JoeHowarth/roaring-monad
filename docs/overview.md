# Finalized History Query Overview

This is the developer documentation for `crates/finalized-history-query`.

Topic-based docs live alongside this file. Historical design docs and migration plans live under `docs/historical/`. Active and completed design plans live under `docs/plans/`.

## Scope

The crate implements finalized history queries for the logs family.

The main path:

1. a finalized block is ingested
2. logs receive monotonic finalized `log_id`
3. immutable log-directory fragments and immutable stream-page fragments are written
4. `publication_state.indexed_finalized_head` is advanced only after all authoritative artifacts for the block exist
5. `query_logs` resolves a finalized block window
6. the logs family maps that block window to a log-ID window
7. the query reads immutable artifacts through the configured bytes cache when a table budget is enabled
8. the query reads compacted page/sub-bucket summaries when present and falls back to immutable frontier fragments otherwise
9. the query returns `QueryPage<Log>` with exact resume metadata

## Crate Layout

The crate ships as `finalized-history-query`. Related crates:

- `log-workload-gen` — synthetic log workload generation for testing and benchmarks
- `benchmarking` — benchmark harness for query and ingest paths

## Layering

The crate is organized in three layers.

### Method boundary

- `src/api/mod.rs`
- `src/api/service/*`

### Shared finalized-history substrate

- `src/cache/*`
- `src/core/*`
- `src/ingest/authority.rs`
- `src/ingest/authority/*`
- `src/streams/*`

### Logs family adapter

- `src/logs/*`

The RPC crate stays outside this boundary. It owns transport concerns such as JSON-RPC parsing, tag policy, field selection, envelope formatting, and error mapping.

For details on each subsystem, see: [storage-model.md](storage-model.md), [write-authority.md](write-authority.md), [query-execution.md](query-execution.md), [ingest-pipeline.md](ingest-pipeline.md), [caching.md](caching.md), [backend-stores.md](backend-stores.md), [config.md](config.md).

## Current Responsibilities

### Method boundary

The public query surface is transport-free:

- `QueryLogsRequest`
- `ExecutionBudget`
- `QueryPage<Log>`
- `QueryPageMeta`
- `FinalizedHistoryService`

This crate executes queries and ingest. It does not format JSON-RPC responses.

### Shared substrate

The shared layer owns:

- range resolution against finalized head
- page and resume metadata types
- shard-streaming indexed execution on primary IDs
- immutable-bytes cache policy for immutable read artifacts (see [caching.md](caching.md))
- runtime degraded / throttled state
- write-authority policy and fencing (see [write-authority.md](write-authority.md))
- publication-state reads
- shared finalized-state and block-identity reads

### Logs family

The logs layer owns:

- filter semantics
- block-window to log-window mapping
- exact-match materialization
- log artifact writes
- log block metadata reads and writes
- immutable directory fragment writes and summary compaction
- immutable stream fragment writes and page compaction
- stream fanout for address/topic indexes

## Main Types

```python
class QueryOrder:
    ASCENDING


class QueryLogsRequest:
    from_block: int
    to_block: int
    order: QueryOrder
    resume_log_id: int | None
    limit: int
    filter: LogFilter


class ExecutionBudget:
    max_results: int | None


class BlockRef:
    number: int
    hash: bytes32
    parent_hash: bytes32


class QueryPageMeta:
    resolved_from_block: BlockRef
    resolved_to_block: BlockRef
    cursor_block: BlockRef
    has_more: bool
    next_resume_log_id: int | None


class QueryPage[T]:
    items: list[T]
    meta: QueryPageMeta


class FinalizedHeadState:
    indexed_finalized_head: int
    publication_epoch: int


class BlockIdentity:
    number: int
    hash: bytes32
    parent_hash: bytes32


class BlockMeta:
    block_hash: bytes32
    parent_hash: bytes32
    first_log_id: int
    count: u32


class LogSequencingState:
    next_log_id: LogId


class LogBlockWindow:
    first_log_id: LogId
    count: u32
```

## Persisted Key Schema

This is the canonical key reference. See [storage-model.md](storage-model.md) for the data model and lookup flow, and [backend-stores.md](backend-stores.md) for the store traits.

Shared metadata:

- `publication_state -> PublicationState { owner_id, session_id, epoch, indexed_finalized_head, lease_valid_through_block }`
- `block_meta/<block_num> -> BlockMeta { block_hash, parent_hash, first_log_id, count }`
- `block_hash_to_num/<block_hash> -> block_num`
- `block_log_headers/<block_num> -> BlockLogHeader { offsets }`

Directory metadata:

- `log_dir_frag/<sub_bucket_start>/<block_num> -> LogDirFragment { block_num, first_log_id, end_log_id_exclusive }`
- `log_dir_sub/<sub_bucket_start> -> LogDirectoryBucket { start_block, first_log_ids }`
- optional `log_dir/<bucket_start> -> LogDirectoryBucket { start_block, first_log_ids }`

Stream index metadata/blob pairs:

- `open_stream_page/<shard>/<page_start_local>/<stream_id> -> marker`
- `stream_frag_meta/<stream_id>/<page_start_local>/<block_num> -> StreamBitmapMeta { block_num, count, min_local, max_local }`
- `stream_frag_blob/<stream_id>/<page_start_local>/<block_num> -> roaring bitmap blob`
- `stream_page_meta/<stream_id>/<page_start_local> -> StreamBitmapMeta { block_num, count, min_local, max_local }`
- `stream_page_blob/<stream_id>/<page_start_local> -> roaring bitmap blob`

Payload blobs:

- `block_logs/<block_num> -> concatenated encoded logs`

Numeric key components use big-endian encoded u64. The exception is `block_hash_to_num/<block_hash>` which uses the raw 32-byte hash. Blob-store keys follow the same conventions.

## Top-Level Service Boundary

```python
class FinalizedHistoryService:
    async def startup(self) -> RecoveryPlan
    async def query_logs(self, request: QueryLogsRequest, budget: ExecutionBudget) -> QueryPage[Log]
    async def ingest_finalized_block(self, block: Block) -> IngestOutcome
    async def ingest_finalized_blocks(self, blocks: list[Block]) -> IngestOutcome
```

This boundary is transport-free:

- the RPC crate parses and validates transport requests
- this crate executes queries and ingest
- the RPC crate formats the final response envelope

## Deferred Scope

The crate intentionally does not implement:

- descending traversal
- `block_hash` query mode on the transport-free query surface
- non-log families
- relation hydration helpers
- canonical block / transaction / trace artifact stores

## Suggested Reading Path

### Pass 1: Public surface

1. `src/lib.rs` — crate boundary, re-exports
2. `src/api/mod.rs` — transport-free query surface and write-side public boundary
3. `src/api/service/` — service behavior: query, ingest, health

### Pass 2: Shared substrate

4. `src/cache/mod.rs` — immutable-bytes cache boundary, table IDs, byte-budget config
5. `src/core/clause.rs` — shared clause vocabulary (`Any`, `One`, `Or`)
6. `src/core/page.rs` — pagination/result vocabulary
7. `src/core/refs.rs` — shared `BlockRef` type
8. `src/core/state.rs` — shared state projections
9. `src/core/range.rs` — block-range validation and clipping
10. `src/core/execution.rs` — matched-primary vocabulary
11. `src/core/runtime.rs` — degraded/throttled state machine
12. `src/domain/keys.rs` — immutable-frontier key layout
13. `src/streams/chunk.rs` — roaring bitmap blob format

### Pass 3: Logs family

14. `src/logs/filter.rs` — log matching semantics, indexed clauses
15. `src/logs/types.rs` — logs-family aliases and projections
16. `src/logs/state.rs` — `BlockMeta` helpers, log-window fields
17. `src/logs/window.rs` — block range to primary-ID range bridge
18. `src/logs/materialize/` — `log_id -> block_num -> byte-range` resolution
19. `src/logs/query/` — main query engine
20. `src/logs/ingest/` — log-family ingest: artifacts, fragments, compaction

### Pass 4: Storage and codecs

21. `src/domain/types.rs` — core persisted/data model structs
22. `src/codec/finalized_state.rs` — `PublicationState`, `BlockMeta` encoding
23. `src/codec/log.rs` — log, directory bucket, block log header encodings
24. `src/store/traits.rs` — `MetaStore`, `BlobStore` contracts

### Pass 5: Ingest orchestration

25. `src/ingest/engine.rs` — ingest orchestrator
26. `src/ingest/authority.rs` — `WriteToken`, `WriteAuthority` contract
27. `src/ingest/authority/lease/` — lease-backed multi-writer authority
28. `src/ingest/authority/single_writer.rs` — fail-closed single-writer authority
29. `src/recovery/mod.rs` — startup view, cleanup, marker repair

### Pass 6: End-to-end behavior

30. `tests/publication_authority.rs` — publication state, lease authority, fencing
31. `tests/startup_recovery.rs` — startup, recovery, session reuse, roles
32. `tests/query_semantics.rs` — query pagination, limit/resume, range clipping
33. `tests/ingest_compaction.rs` — shard boundaries, compaction, directory fragments
34. `tests/cache_behavior.rs` — point log payload caching, range read coalescing
35. `tests/crash_injection_matrix.rs` — crash-retry behavior
36. `tests/differential_and_gc.rs` — differential correctness, recovery

All paths are relative to `crates/finalized-history-query/`.
