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
7. the query reads immutable artifacts through typed artifact tables backed by per-table bytes caches when a table budget is enabled
8. the query reads compacted page/sub-bucket summaries when present and falls back to immutable frontier fragments otherwise
9. the query returns `QueryPage<Log>` with exact resume metadata

## Crate Layout

The crate ships as `finalized-history-query`. Related crates:

- `log-workload-gen` — synthetic log workload generation for testing and benchmarks
- `benchmarking` — benchmark harness for query and ingest paths

## Layering

The crate is organized in three layers.

### Method boundary

- `src/api.rs`
- `src/family.rs`

### Shared finalized-history substrate

- `src/core/*`
- `src/ingest/authority.rs`
- `src/ingest/authority/*`
- `src/ingest/engine.rs`
- `src/streams/*`
- `src/tables/*`

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

- the explicit family boundary used by generic startup/ingest machinery
- range resolution against finalized head
- page and resume metadata types
- shard-streaming indexed execution on primary IDs
- typed immutable-artifact table reads with per-table bytes cache policy (see [caching.md](caching.md))
- write-authority policy (see [write-authority.md](write-authority.md))
- publication-state reads
- shared finalized-state and block-identity reads

### Logs family

The logs layer owns:

- logs-owned schema, codecs, keys, and table specs
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


class BlockIdentity:
    number: int
    hash: bytes32
    parent_hash: bytes32


class BlockRecord:
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

- `publication_state` table, key `state` -> `PublicationState { owner_id, session_id, indexed_finalized_head, lease_valid_through_block }`
- `block_record` table, key `<block_num>` -> `BlockRecord { block_hash, parent_hash, first_log_id, count }`
- `block_hash_index` table, key `<block_hash>` -> `block_num`
- `block_log_header` table, key `<block_num>` -> `BlockLogHeader { offsets }`

Directory metadata:

- `log_dir_by_block` table, partition `<sub_bucket_start>`, clustering `<block_num>` -> `DirByBlock { block_num, first_log_id, end_log_id_exclusive }`
- `log_dir_sub_bucket` table, key `<sub_bucket_start>` -> `DirBucket { start_block, first_log_ids }`
- optional `log_dir_bucket` table, key `<bucket_start>` -> `DirBucket { start_block, first_log_ids }`

Stream index metadata/blob pairs:

- `open_bitmap_page` table, partition `<shard>`, clustering `<page_start_local>/<stream_id>` -> marker
- `bitmap_by_block` table, partition `<stream_id>/<page_start_local>`, clustering `<block_num>` -> roaring bitmap blob
- `bitmap_page_meta` table, key `<stream_id>/<page_start_local>` -> `StreamBitmapMeta { block_num, count, min_local, max_local }`
- `bitmap_page_blob` blob table, key `<stream_id>/<page_start_local>` -> roaring bitmap blob

Payload blobs:

- `block_log_blob` blob table, key `<block_num>` -> concatenated encoded logs

Numeric key components use big-endian encoded u64. `block_hash_index` uses the raw 32-byte hash as its suffix key. Blob-table suffix keys follow the same conventions.

## Top-Level Service Boundary

```python
class FinalizedHistoryService:
    async def startup(self) -> StartupPlan
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
2. `src/api.rs` — transport-free request/result surface and service export
3. `src/family.rs` — explicit startup/ingest family boundary under the concrete API

### Pass 2: Shared substrate

4. `src/tables.rs` — typed immutable-artifact tables, per-table bytes caches, and cache config/metrics
5. `src/core/clause.rs` — shared clause vocabulary (`Any`, `One`, `Or`)
6. `src/core/page.rs` — pagination/result vocabulary
7. `src/core/refs.rs` — shared `BlockRef` type
8. `src/core/state.rs` — shared state projections
9. `src/core/range.rs` — block-range validation and clipping
10. `src/core/execution.rs` — matched-primary vocabulary
11. `src/domain/keys.rs` — shared publication-state key layout
12. `src/domain/types.rs` — shared publication/session state
13. `src/streams/bitmap_blob.rs` — roaring bitmap blob format

### Pass 3: Logs family

14. `src/logs/types.rs` — logs-owned schema and startup projections
15. `src/logs/keys.rs` — logs-family key layout and ID constants
16. `src/logs/table_specs.rs` — logs-family table specs and key helpers
17. `src/logs/codec.rs` — log, directory bucket, block log header, and block record encodings
18. `src/logs/log_ref.rs` — zero-copy log and bucket views
19. `src/logs/family.rs` — logs implementation of the explicit family boundary
20. `src/logs/filter.rs` — log matching semantics, indexed clauses
21. `src/logs/state.rs` — `BlockRecord` helpers, log-window fields
22. `src/logs/window.rs` — block range to primary-ID range bridge
23. `src/logs/materialize/` — `log_id -> block_num -> byte-range` resolution
24. `src/logs/query/` — main query engine
25. `src/logs/ingest/` — log-family ingest: artifacts, fragments, compaction

### Pass 4: Storage and codecs

26. `src/codec/finalized_state.rs` — shared `PublicationState` and helper encoding
27. `src/store/traits.rs` — `MetaStore`, `BlobStore` contracts

### Pass 5: Ingest orchestration

28. `src/ingest/engine.rs` — generic ingest orchestration over a family implementation
29. `src/ingest/authority.rs` — `WriteAuthority` contract
30. `src/ingest/authority/lease/` — lease-backed multi-writer authority
31. `src/startup.rs` — concrete logs startup view built on the family boundary

### Pass 6: End-to-end behavior

32. `tests/publication_authority.rs` — publication state, lease authority, publication-only safety
33. `tests/startup.rs` — startup, session reuse, roles
34. `tests/query_semantics.rs` — query pagination, limit/resume, range clipping
35. `tests/ingest_compaction.rs` — shard boundaries, compaction, directory fragments
36. `tests/cache_behavior.rs` — point log payload caching, range read coalescing
37. `tests/crash_injection_matrix.rs` — crash-retry behavior
38. `tests/differential_and_gc.rs` — differential correctness, recovery

All paths are relative to `crates/finalized-history-query/`.
