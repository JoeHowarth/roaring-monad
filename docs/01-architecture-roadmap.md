# Finalized History Query Architecture

This folder is the current developer documentation for `crates/finalized-history-query`.

Historical design docs, migration plans, and superseded onboarding material live under `docs/historical/`.

The current log-payload storage design lives in:

- `docs/04-block-keyed-log-storage.md`
- `docs/publication-flow-visualization.html`

## Scope

The crate currently implements finalized history queries for the logs family.

Follow the main path first:

1. a finalized block is ingested
2. logs receive monotonic finalized `log_id`
3. immutable log-directory fragments and immutable stream-page fragments are written
4. `publication_state.indexed_finalized_head` is advanced only after all authoritative artifacts for the block exist
5. `query_logs` resolves a finalized block window
6. the logs family maps that block window to a log-ID window
7. the query reads compacted page/sub-bucket summaries when present and falls back to immutable frontier fragments otherwise
8. the query returns `QueryPage<Log>` with exact resume metadata

## Layering

The crate is organized in three layers.

- method boundary
  - `src/api/query_logs.rs`
  - `src/api/write.rs`
  - `src/api/service.rs`
- shared finalized-history substrate
  - `src/core/*`
  - `src/streams/*`
- logs family adapter
  - `src/logs/*`

The RPC crate stays outside this boundary. It owns transport concerns such as JSON-RPC parsing, tag policy, field selection, envelope formatting, and error mapping.

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
- runtime degraded / throttled state
- publication-state reads
- shared finalized-state and block-identity reads

Important shared view types:

- `FinalizedHeadState`
- `BlockIdentity`
- `BlockRef`

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

Important log-specific view types:

- `LogSequencingState`
- `LogBlockWindow`

## Persisted Model

Shared metadata:

- `publication_state -> PublicationState { owner_id, session_id, epoch, indexed_finalized_head, lease_expires_at_ms }`
- `block_meta/<block_num> -> BlockMeta`
- `block_hash_to_num/<block_hash> -> block_num`
- `block_log_headers/<block_num> -> BlockLogHeader`

Directory metadata:

- `log_dir_frag/<sub_bucket_start>/<block_num> -> LogDirFragment`
- `log_dir_sub/<sub_bucket_start> -> LogDirectoryBucket`
- optional `log_dir/<bucket_start> -> LogDirectoryBucket`

Stream index metadata/blob pairs:

- `stream_frag_meta/<stream_id>/<page_start_local>/<block_num> -> StreamBitmapMeta`
- `stream_frag_blob/<stream_id>/<page_start_local>/<block_num> -> roaring bitmap blob`
- `stream_page_meta/<stream_id>/<page_start_local> -> StreamBitmapMeta`
- `stream_page_blob/<stream_id>/<page_start_local> -> roaring bitmap blob`

Payload blobs:

- `block_logs/<block_num> -> concatenated encoded logs`

## Query Semantics

`query_logs` works in ascending finalized-block order.

- `RangeResolver` clips the request to the indexed finalized head.
- `LogWindowResolver` maps the resolved block range to a log-ID range.
- queries must include at least one indexed address/topic clause
- indexed queries execute one shard at a time in ascending `log_id` order
- stream scans prefer compacted `stream_page_*` blobs and fall back to `stream_frag_*` blobs for the bounded frontier or compaction lag
- `log_id -> block_num` resolution prefers compacted `log_dir_sub/*` or `log_dir/*` summaries and falls back to `log_dir_frag/*`
- non-indexed queries are rejected instead of falling back to a scan.
- pagination uses `resume_log_id` as a declarative lower bound.
- responses return `cursor_block` separately from `next_resume_log_id`.

The executor preserves primary IDs through page assembly so `has_more`, `next_resume_log_id`, and `cursor_block` are exact. It does not load full-window clause sets up front.

## Ingest Semantics

`IngestEngine` is now an orchestrator.

It is responsible for:

- finalized sequence and parent validation
- coordinating log artifact writes
- coordinating immutable directory and stream frontier publication
- advancing `publication_state.indexed_finalized_head` last

`FinalizedHistoryService::startup()` owns active publication lifecycle:

- publication ownership acquisition
- cleanup-first recovery of unpublished suffix artifacts
- sealed open-page marker repair
- deriving the startup `next_log_id` view

`startup_plan(...)` is observational only:

- loads `publication_state`
- derives `next_log_id`
- never mutates ownership

The logs family owns:

- directory-fragment writes
- directory sub-bucket and optional 1M-bucket compaction
- block-log-header writes
- block-log blob writes
- log block-metadata writes
- block-hash lookup writes
- immutable stream-fragment writes
- stream-page compaction
- stream fanout rules

## Deferred Scope

The current crate intentionally does not implement:

- descending traversal
- `block_hash` query mode on the transport-free query surface
- non-log families
- relation hydration helpers
- canonical block / transaction / trace artifact stores

## Reading Order

Read these files in order:

1. `crates/finalized-history-query/src/lib.rs`
2. `crates/finalized-history-query/src/api/query_logs.rs`
3. `crates/finalized-history-query/src/api/service.rs`
4. `crates/finalized-history-query/src/core/page.rs`
5. `crates/finalized-history-query/src/core/range.rs`
6. `crates/finalized-history-query/src/logs/query.rs`
7. `crates/finalized-history-query/src/logs/ingest.rs`
8. `crates/finalized-history-query/src/ingest/engine.rs`
9. `crates/finalized-history-query/tests/finalized_index.rs`
