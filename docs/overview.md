# Finalized History Query Overview

This is the main current-state architecture doc for
`crates/finalized-history-query`.

If you are new to the repo, start with [README.md](README.md) for the
recommended reading order. In particular:

- read [reference/queryX](reference/queryX) and [queryx-support.md](queryx-support.md)
  first for the product boundary
- read the two docs under [design/](design/) next for the core mental model
- then use this doc as the bridge into the current implementation

Topic docs live alongside this file. Historical design docs and migration
plans live under `docs/historical/`. Active and completed plans live under
`docs/plans/`.

## Scope

The crate implements the finalized-history query substrate that powers the
`queryX` family of RPC methods. Today it exposes finalized-history queries for
shared block identities plus the logs and traces families, alongside a shared
ingest substrate for logs, txs, and traces.

The main path:

1. a shared finalized block envelope is ingested
2. the ingest coordinator validates finalized block continuity once for the whole batch
3. family-specific ingest handlers process their slice of the block in logs, txs, then traces order
4. logs receive monotonic finalized `log_id`; traces receive monotonic finalized `TraceId`
5. immutable family-owned directory fragments and immutable stream-page fragments are written
6. `publication_state.indexed_finalized_head` is advanced only after all authoritative artifacts for every participating family exist
7. `query_blocks`, `query_logs`, and `query_traces` resolve finalized block windows
8. block queries scan shared block metadata directly; indexed families map that block window to a primary-ID window
9. query, status, and ingest share one long-lived runtime that owns store handles plus typed artifact tables
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
- `src/family.rs`

### Shared finalized-history substrate

- `src/blocks.rs`
- `src/core/*`
- `src/family.rs`
- `src/ingest/authority.rs`
- `src/ingest/bitmap_pages.rs`
- `src/ingest/engine.rs`
- `src/ingest/open_pages.rs`
- `src/ingest/primary_dir.rs`
- `src/ingest/recovery.rs`
- `src/kernel/*`
- `src/runtime.rs`
- `src/streams.rs`
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

- `QueryBlocksRequest`
- `QueryLogsRequest`
- `QueryTransactionsRequest`
- `QueryTracesRequest`
- `ExecutionBudget`
- `QueryPage<Block>`
- `QueryPage<LogRef>`
- `QueryPage<TxRef>`
- `QueryPage<TraceRef>`
- `QueryPageMeta`
- `FinalizedHistoryService`

This crate executes queries and ingest. It does not format JSON-RPC responses.

### Shared substrate

The shared layer owns:

- the explicit family boundary and concrete family registry used by status and ingest
- the shared finalized block envelope used by concrete ingest entrypoints
- the long-lived runtime that shares store handles and typed tables across query/status/ingest
- the multi-family ingest coordinator that validates sequence once and publishes once per batch
- range resolution against finalized head
- direct finalized block scans over shared block metadata
- page and resume metadata types
- shard-streaming indexed execution on primary IDs
- typed immutable-artifact table reads with per-table bytes cache policy (see [caching.md](caching.md))
- write-authority policy (see [write-authority.md](write-authority.md))
- publication-state reads
- shared finalized-state and block-identity reads
- shared primary-directory fragment persistence plus sealed sub-bucket/bucket compaction
- shared bitmap-page fragment persistence plus sealed-page compaction mechanics

### Logs family

The logs layer owns:

- logs-owned schema, codecs, keys, and table specs
- filter semantics
- block-window to log-window mapping
- exact-match materialization
- log artifact writes
- log block metadata reads and writes
- logs-specific stream fanout and open-page marker handling
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
- traces-specific stream fanout from `trace_rlp`
- stream fanout for `from`, `to`, `selector`, and `has_value`
- traces-specific sequencing state (`next_trace_id`) and per-block ingest behavior
- public service-level `query_traces` execution over trace-owned indexes

### Txs family

The txs layer already participates in the shared family boundary:

- `src/txs/*`

Txs now persist authoritative block tx blobs, block tx headers, hash lookup rows, and shared directory fragments during ingest. Transaction query execution, stream indexing, and signed-tx variant materialization remain incomplete.
The public indexed-family query items are zero-copy view types: logs return `LogRef`, txs return `TxRef`, and traces return `TraceRef`.

## Main Types

```python
class QueryOrder:
    ASCENDING


class QueryLogsRequest:
    from_block: int | None
    to_block: int | None
    from_block_hash: bytes32 | None
    to_block_hash: bytes32 | None
    order: QueryOrder
    resume_id: int | None
    limit: int
    filter: LogFilter


class QueryBlocksRequest:
    from_block: int | None
    to_block: int | None
    from_block_hash: bytes32 | None
    to_block_hash: bytes32 | None
    order: QueryOrder
    limit: int


class QueryTracesRequest:
    from_block: int | None
    to_block: int | None
    from_block_hash: bytes32 | None
    to_block_hash: bytes32 | None
    order: QueryOrder
    resume_id: int | None
    limit: int
    filter: TraceFilter


class QueryTransactionsRequest:
    from_block: int | None
    to_block: int | None
    from_block_hash: bytes32 | None
    to_block_hash: bytes32 | None
    order: QueryOrder
    resume_id: int | None
    limit: int
    filter: TxFilter


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


class Block:
    number: int
    hash: bytes32
    parent_hash: bytes32


class IngestTx:
    tx_idx: int
    tx_hash: bytes32
    sender: bytes20
    signed_tx_bytes: bytes


class LogRef:
    def address(self) -> bytes20: ...
    def topic_count(self) -> int: ...
    def topic(self, i: int) -> bytes32: ...
    def data(self) -> bytes: ...
    def block_num(self) -> int: ...
    def tx_idx(self) -> int: ...
    def log_idx(self) -> int: ...
    def block_hash(self) -> bytes32: ...


class TxRef:
    def block_num(self) -> int: ...
    def block_hash(self) -> bytes32: ...
    def tx_idx(self) -> int: ...
    def tx_hash(self) -> bytes32: ...
    def sender(self) -> bytes20: ...
    def signed_tx_bytes(self) -> bytes: ...


class TraceRef:
    def block_num(self) -> int: ...
    def block_hash(self) -> bytes32: ...
    def tx_idx(self) -> int: ...
    def trace_idx(self) -> int: ...
    def typ(self) -> int: ...
    def flags(self) -> int: ...
    def from_addr(self) -> bytes20: ...
    def to_addr(self) -> bytes20 | None: ...
    def value_bytes(self) -> bytes: ...
    def gas(self) -> int: ...
    def gas_used(self) -> int: ...
    def input(self) -> bytes: ...
    def output(self) -> bytes: ...
    def status(self) -> int: ...
    def depth(self) -> int: ...


class FinalizedBlock:
    block_num: int
    block_hash: bytes32
    parent_hash: bytes32
    logs: list[Log]
    txs: list[IngestTx]
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
    logs: PrimaryWindowRecord | None
    txs: PrimaryWindowRecord | None
    traces: PrimaryWindowRecord | None


class PrimaryWindowRecord:
    first_primary_id: int
    count: u32


class LogSequencingState:
    next_log_id: LogId


class TxFamilyState:
    next_tx_id: TxId


class LogBlockWindow:
    first_log_id: LogId
    count: u32


class ServiceStatus:
    head_state: FinalizedHeadState
    log_state: LogSequencingState
    tx_state: TxFamilyState
    trace_state: TraceSequencingState
```

## Persisted Key Schema

This is the canonical key reference. See [storage-model.md](storage-model.md) for the data model and lookup flow, and [backend-stores.md](backend-stores.md) for the store traits.

Shared metadata:

- `publication_state` table, key `state` -> `PublicationState { owner_id, session_id, indexed_finalized_head, lease_valid_through_block }`
- `block_record` table, key `<block_num>` -> `BlockRecord { block_hash, parent_hash, logs: Option<PrimaryWindowRecord>, txs: Option<PrimaryWindowRecord>, traces: Option<PrimaryWindowRecord> }`
- `block_hash_index` table, key `<block_hash>` -> `block_num`
- `block_log_header` table, key `<block_num>` -> `BlockLogHeader { offsets }`
- `block_trace_header` table, key `<block_num>` -> `BlockTraceHeader { encoding_version, offsets, tx_starts }`

Trace metadata and blobs:

- `block_trace_blob` blob table, key `<block_num>` -> raw per-block `trace_rlp`
- `trace_dir_bucket` table, key `<trace_bucket_start>` -> compact canonical directory summary
- `trace_dir_sub_bucket` table, key `<trace_sub_bucket_start>` -> compact canonical sub-bucket summary
- `trace_dir_by_block` scannable table, partition `<trace_sub_bucket_start>`, clustering `<block_num>` -> immutable frontier fragment
- `trace_bitmap_by_block` scannable table, partition `<stream_id>/<page_start>`, clustering `<block_num>` -> immutable bitmap fragment
- `trace_bitmap_page_meta` table, key `<stream_id>/<page_start>` -> compacted page metadata
- `trace_bitmap_page_blob` blob table, key `<stream_id>/<page_start>` -> compacted page bitmap
- `trace_open_bitmap_page` scannable table, partition `<shard>`, clustering `<page_start_local>/<stream_id>` -> marker

Directory metadata:

- `log_dir_by_block` table, partition `<sub_bucket_start>`, clustering `<block_num>` -> `DirByBlock { block_num, first_log_id, end_log_id_exclusive }`
- `log_dir_sub_bucket` table, key `<sub_bucket_start>` -> `DirBucket { start_block, first_log_ids }`
- optional `log_dir_bucket` table, key `<bucket_start>` -> `DirBucket { start_block, first_log_ids }`

Stream index metadata/blob pairs:

- `open_bitmap_page` table, partition `<shard>`, clustering `<page_start_local>/<stream_id>` -> marker
- `bitmap_by_block` table, partition `<stream_id>/<page_start_local>`, clustering `<block_num>` -> roaring bitmap blob
- `bitmap_page_meta` table, key `<stream_id>/<page_start_local>` -> `StreamBitmapMeta { count, min_local, max_local }`
- `bitmap_page_blob` blob table, key `<stream_id>/<page_start_local>` -> roaring bitmap blob

Payload blobs:

- `block_log_blob` blob table, key `<block_num>` -> concatenated encoded logs

Numeric key components use big-endian encoded u64. `block_hash_index` uses the raw 32-byte hash as its suffix key. Blob-table suffix keys follow the same conventions.

Today the logs and traces families persist finalized-history artifacts. The shared ingest coordinator also carries the txs family slot, but tx payload ingest still rejects non-empty tx batches until that family grows real storage and query support.

## Top-Level Service Boundary

```python
class FinalizedHistoryService:
    async def status(self) -> ServiceStatus
    async def query_logs(self, request: QueryLogsRequest, budget: ExecutionBudget) -> QueryPage[LogRef]
    async def query_transactions(self, request: QueryTransactionsRequest, budget: ExecutionBudget) -> QueryPage[TxRef]
    async def query_traces(self, request: QueryTracesRequest, budget: ExecutionBudget) -> QueryPage[TraceRef]
    async def ingest_finalized_block(self, block: FinalizedBlock) -> IngestOutcome
    async def ingest_finalized_blocks(self, blocks: list[FinalizedBlock]) -> IngestOutcome
```

This boundary is transport-free:

- the RPC crate parses and validates transport requests
- this crate executes queries and ingest
- the RPC crate formats the final response envelope
- read-only service inspection remains available through `status()` or `service_status(...)`

## Deferred Scope

The crate intentionally does not implement:

- descending traversal
- tx artifact storage and query support
- relation hydration helpers
- canonical block / transaction / trace artifact stores

## Suggested Reading Path

### Pass 1: Public surface

1. `src/lib.rs` — crate boundary, re-exports
2. `src/api.rs` — transport-free request/result surface and service export
3. `src/family.rs` — explicit status/ingest family boundary under the concrete API

### Pass 2: Shared substrate

4. `src/kernel/cache.rs` — per-table bytes-cache config, metrics, and cache internals
5. `src/kernel/point_table.rs` — shared cache-backed point-table reads/writes
6. `src/kernel/scannable_table.rs` — shared scannable partition loading
7. `src/kernel/blob_table.rs` — shared cache-backed blob access
8. `src/tables.rs` — typed immutable-artifact table assembly, shared directory bundles, and family-facing wrappers
9. `src/core/types.rs` — shared clause, pagination, and `BlockRef` vocabulary
10. `src/core/state.rs` — shared state projections
11. `src/core/range.rs` — block-range validation and clipping
12. `src/core/layout.rs` — shared finalized-history ID layout constants
13. `src/core/ids.rs` — shared `FamilyId` core plus family ID wrappers and shared ranges
14. `src/core/directory.rs` — shared directory bucket and fragment payloads
15. `src/core/directory_resolver.rs` — shared primary-ID directory resolution
16. `src/query/runner.rs` — shared matched-item vocabulary, public candidate runner, and indexed query runner
17. `src/store/publication.rs` — shared publication/session state and storage key
18. `src/streams.rs` — roaring bitmap blob format and stream-page metadata

### Pass 3: Logs family

19. `src/logs/types.rs` — logs-owned schema and sequencing projections
20. `src/logs/table_specs.rs` — logs-family table specs and key helpers
21. `src/logs/codec.rs` — log and block-header encodings over shared storage payloads
22. `src/logs/log_ref.rs` — zero-copy log views
23. `src/logs/family.rs` — logs-specific state derivation and per-block ingest handler
24. `src/logs/filter.rs` — log matching semantics and indexed clauses
25. `src/logs/materialize/` — logs-specific hydration on top of shared ID resolution
26. `src/logs/query/` — main query engine
27. `src/logs/ingest.rs` — log-family ingest: artifacts and stream fanout

### Pass 4: Traces family

28. `src/traces/types.rs` — traces-owned schema and sequencing projections
29. `src/traces/table_specs.rs` — traces-family table specs
30. `src/traces/codec.rs` — trace block-header encodings
31. `src/traces/view.rs` — zero-copy `CallFrameView` access over stored RLP bytes
32. `src/traces/materialize.rs` — `trace_id -> block_num -> trace bytes` resolution
33. `src/traces/filter.rs` — trace matching semantics and indexed clauses
34. `src/traces/query/` — trace query engine
35. `src/traces/ingest.rs` — trace-family ingest: artifacts and stream fanout

### Pass 5: Storage and codecs

36. `src/kernel/codec.rs` — shared storage codec trait and fixed-layout codec macro
37. `src/kernel/sharded_streams.rs` — shared sharded stream/page helpers used by logs and traces
38. `src/kernel/compaction.rs` — shared sealed-boundary compaction helpers
39. `src/store/traits.rs` — `MetaStore`, `BlobStore` contracts

### Pass 6: Ingest orchestration

42. `src/ingest/engine.rs` — writer preflight and ingest orchestration over the concrete `Families { logs, txs, traces }` registry
43. `src/ingest/authority.rs` — `WriteAuthority` contract
44. `src/ingest/recovery.rs` — ownership-transition recovery preflight
45. `src/status.rs` — observational service state built on the family boundary

### Pass 7: End-to-end behavior

46. `tests/authority.rs` — lease authority, publication-only safety, writer-side authority behavior
47. `tests/status.rs` — observational status behavior
48. `tests/recovery.rs` — ownership-transition recovery and hot-path no-scan guarantees
49. `tests/ingest.rs` — ingest validation and block-sequence behavior
50. `tests/query_logs.rs` — logs query pagination, limit/resume, and edge cases
51. `tests/query_traces.rs` — trace query semantics
52. `tests/crash_recovery.rs` — crash and retry behavior across protocol phases

All paths are relative to `crates/finalized-history-query/`.
