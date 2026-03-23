# Finalized History Query Overview

This is the developer documentation for `crates/finalized-history-query`.

Topic-based docs live alongside this file. Historical design docs and migration plans live under `docs/historical/`. Active and completed design plans live under `docs/plans/`.

## Scope

The crate implements finalized history queries for the logs and traces families, plus a shared ingest substrate for the logs, txs, and traces families.

The main path:

1. a shared finalized block envelope is ingested
2. the ingest coordinator validates finalized block continuity once for the whole batch
3. family-specific ingest handlers process their slice of the block in logs, txs, then traces order
4. logs receive monotonic finalized `log_id`; traces receive monotonic finalized `TraceId`
5. immutable family-owned directory fragments and immutable stream-page fragments are written
6. `publication_state.indexed_finalized_head` is advanced only after all authoritative artifacts for every participating family exist
7. `query_logs` and `query_traces` resolve finalized block windows
8. each family maps that block window to its primary-ID window
9. query, startup, and ingest share one long-lived runtime that owns store handles plus typed artifact tables
10. the query reads immutable artifacts through typed artifact tables backed by per-table bytes caches when a table budget is enabled
11. ingest writes seed those same typed caches immediately
12. the query reads compacted page/sub-bucket summaries when present and falls back to immutable frontier fragments otherwise
13. the query returns `QueryPage<T>` with exact resume metadata keyed by the family primary ID

## Crate Layout

The crate ships as `finalized-history-query`. Related crates:

- `log-workload-gen` — synthetic log workload generation for testing and benchmarks
- `benchmarking` — benchmark harness for query and ingest paths

## Layering

The crate is organized in three layers.

### Method boundary

- `src/api.rs`
- `src/block.rs`

### Shared finalized-history substrate

- `src/core/*`
- `src/family.rs`
- `src/ingest/authority.rs`
- `src/ingest/authority/*`
- `src/ingest/engine.rs`
- `src/kernel/*`
- `src/runtime.rs`
- `src/streams/*`
- `src/tables.rs`

### Family adapters

- `src/logs/*`
- `src/txs/*`
- `src/traces/*`

The RPC crate stays outside this boundary. It owns transport concerns such as JSON-RPC parsing, tag policy, field selection, envelope formatting, and error mapping.

For details on each subsystem, see: [storage-model.md](storage-model.md), [write-authority.md](write-authority.md), [query-execution.md](query-execution.md), [ingest-pipeline.md](ingest-pipeline.md), [queryx-support.md](queryx-support.md), [caching.md](caching.md), [backend-stores.md](backend-stores.md), [config.md](config.md).

## Current Responsibilities

### Method boundary

The public query surface is transport-free:

- `QueryLogsRequest`
- `QueryTracesRequest`
- `ExecutionBudget`
- `QueryPage<Log>`
- `QueryPage<Trace>`
- `QueryPageMeta`
- `FinalizedHistoryService`

This crate executes queries and ingest. It does not format JSON-RPC responses.

### Shared substrate

The shared layer owns:

- the explicit family boundary and concrete family registry used by startup and ingest
- the shared finalized block envelope used by concrete ingest entrypoints
- the long-lived runtime that shares store handles and typed tables across query/startup/ingest
- the multi-family ingest coordinator that validates sequence once and publishes once per batch
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
- logs-specific sequencing state (`next_log_id`) and per-block ingest behavior

### Traces family

The traces layer owns:

- traces-owned schema, codecs, keys, and table specs
- raw per-block `trace_rlp` blob storage plus compact per-block trace headers
- trace block metadata reads and writes
- trace block-window to trace-ID-window mapping
- zero-copy `CallFrameView` access over stored RLP bytes
- trace exact-match materialization from stored bytes
- immutable trace directory fragment writes and summary compaction
- immutable trace stream fragment writes and page compaction
- stream fanout for `from`, `to`, `selector`, and `has_value`
- traces-specific sequencing state (`next_trace_id`) and per-block ingest behavior
- public service-level `query_traces` execution over trace-owned indexes

### Txs family

The txs layer already participates in the shared family boundary:

- `src/txs/*`

Today txs still provide startup-state scaffolds and a concrete family slot in the shared ingest coordinator. Non-empty tx payloads are still rejected until that family grows real storage, codecs, and ingest behavior.

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


class QueryTracesRequest:
    from_block: int | None
    to_block: int | None
    from_block_hash: bytes32 | None
    to_block_hash: bytes32 | None
    order: QueryOrder
    resume_trace_id: int | None
    limit: int
    filter: TraceFilter


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
    next_resume_id: int | None


class QueryPage[T]:
    items: list[T]
    meta: QueryPageMeta


class Tx:
    tx_idx: int
    tx_hash: bytes32


class Trace:
    block_num: int
    block_hash: bytes32
    tx_idx: int
    trace_idx: int
    typ: int
    flags: int
    from: bytes20
    to: bytes20 | None
    value: bytes
    gas: int
    gas_used: int
    input: bytes
    output: bytes
    status: int
    depth: int


class FinalizedBlock:
    block_num: int
    block_hash: bytes32
    parent_hash: bytes32
    logs: list[Log]
    txs: list[Tx]
    trace_rlp: bytes


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


class StartupPlan:
    head_state: FinalizedHeadState
    log_state: LogSequencingState
    tx_state: TxStartupState
    trace_state: TraceStartupState
    warm_streams: int
```

## Persisted Key Schema

This is the canonical key reference. See [storage-model.md](storage-model.md) for the data model and lookup flow, and [backend-stores.md](backend-stores.md) for the store traits.

Shared metadata:

- `publication_state` table, key `state` -> `PublicationState { owner_id, session_id, indexed_finalized_head, lease_valid_through_block }`
- `block_record` table, key `<block_num>` -> `BlockRecord { block_hash, parent_hash, first_log_id, count }`
- `block_hash_index` table, key `<block_hash>` -> `block_num`
- `block_log_header` table, key `<block_num>` -> `BlockLogHeader { offsets }`
- `trace_block_record` table, key `<block_num>` -> `TraceBlockRecord { block_hash, parent_hash, first_trace_id, count }`
- `block_trace_header` table, key `<block_num>` -> `BlockTraceHeader { encoding_version, offsets, tx_starts }`

Trace metadata and blobs:

- `block_trace_blob` blob table, key `<block_num>` -> raw per-block `trace_rlp`
- `trace_dir_bucket` table, key `<trace_bucket_start>` -> compact canonical directory summary
- `trace_dir_sub_bucket` table, key `<trace_sub_bucket_start>` -> compact canonical sub-bucket summary
- `trace_dir_by_block` scannable table, partition `<trace_sub_bucket_start>`, clustering `<block_num>` -> immutable frontier fragment
- `trace_bitmap_by_block` scannable table, partition `<stream_id>/<page_start>`, clustering `<block_num>` -> immutable bitmap fragment
- `trace_bitmap_page_meta` table, key `<stream_id>/<page_start>` -> compacted page metadata
- `trace_bitmap_page_blob` blob table, key `<stream_id>/<page_start>` -> compacted page bitmap

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

Today only the logs family persists finalized-history artifacts. The shared ingest coordinator already reserves concrete family slots for txs and traces, but those families do not yet write storage artifacts.

## Top-Level Service Boundary

```python
class FinalizedHistoryService:
    async def startup(self) -> StartupPlan
    async def query_logs(self, request: QueryLogsRequest, budget: ExecutionBudget) -> QueryPage[Log]
    async def ingest_finalized_block(self, block: FinalizedBlock) -> IngestOutcome
    async def ingest_finalized_blocks(self, blocks: list[FinalizedBlock]) -> IngestOutcome
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

4. `src/kernel/cache.rs` — per-table bytes-cache config, metrics, and cache internals
5. `src/kernel/point_table.rs` — shared cache-backed point-table reads/writes
6. `src/kernel/scannable_table.rs` — shared scannable partition loading
7. `src/kernel/blob_table.rs` — shared cache-backed blob access
8. `src/tables.rs` — typed immutable-artifact table assembly and family-facing wrappers
9. `src/core/clause.rs` — shared clause vocabulary (`Any`, `One`, `Or`)
10. `src/core/page.rs` — pagination/result vocabulary
11. `src/core/refs.rs` — shared `BlockRef` type
12. `src/core/state.rs` — shared state projections
13. `src/core/range.rs` — block-range validation and clipping
14. `src/core/execution.rs` — matched-primary vocabulary
15. `src/domain/keys.rs` — shared publication-state key layout
16. `src/domain/types.rs` — shared publication/session state
17. `src/streams/bitmap_blob.rs` — roaring bitmap blob format

### Pass 3: Logs family

18. `src/logs/types.rs` — logs-owned schema and startup projections
19. `src/logs/keys.rs` — logs-family key layout and ID constants
20. `src/logs/table_specs.rs` — logs-family table specs and key helpers
21. `src/logs/codec.rs` — log, directory bucket, block log header, and block record encodings
22. `src/logs/log_ref.rs` — zero-copy log and bucket views
23. `src/logs/family.rs` — logs-specific startup and per-block ingest handler
24. `src/logs/filter.rs` — log matching semantics, indexed clauses
25. `src/logs/state.rs` — `BlockRecord` helpers, log-window fields
26. `src/logs/materialize/` — `log_id -> block_num -> byte-range` resolution
27. `src/logs/query/` — main query engine
28. `src/logs/ingest/` — log-family ingest: artifacts, fragments, compaction

### Pass 4: Storage and codecs

29. `src/kernel/codec.rs` — shared storage codec trait and fixed-layout codec macro
30. `src/store/traits.rs` — `MetaStore`, `BlobStore` contracts

### Pass 5: Ingest orchestration

31. `src/ingest/engine.rs` — ingest orchestration over the concrete `Families { logs, txs, traces }` registry
32. `src/ingest/authority.rs` — `WriteAuthority` contract
33. `src/ingest/authority/lease/` — lease-backed multi-writer authority
34. `src/startup.rs` — startup view built on the family boundary

### Pass 6: End-to-end behavior

35. `tests/publication_authority.rs` — publication state, lease authority, publication-only safety
36. `tests/startup.rs` — startup, session reuse, roles
37. `tests/query_semantics.rs` — query pagination, limit/resume, range clipping
38. `tests/ingest_compaction.rs` — shard boundaries, compaction, directory fragments
39. `tests/cache_behavior.rs` — point log payload caching, range read coalescing
40. `tests/crash_injection_matrix.rs` — crash-retry behavior
41. `tests/differential_and_gc.rs` — differential correctness, recovery

All paths are relative to `crates/finalized-history-query/`.
