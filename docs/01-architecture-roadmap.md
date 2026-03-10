# Finalized History Query Architecture

This folder is the current developer documentation for `crates/finalized-history-query`.

Historical design docs, migration plans, and superseded onboarding material live under `docs/historical/`.

The current log-payload storage design lives in:

- `docs/04-block-keyed-log-storage.md`

## Scope

The crate currently implements finalized history queries for the logs family.

Follow the main path first:

1. a finalized block is ingested
2. logs receive monotonic finalized log IDs
3. roaring stream indexes are updated
4. `query_logs` resolves a finalized block window
5. the logs family maps that block window to a log-ID window
6. the query returns `QueryPage<Log>` with resume and reorg metadata

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
- candidate execution on primary IDs
- runtime degraded / throttled state
- stream tail / manifest / chunk lifecycle
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
- broad-query block-scan fallback
- log artifact writes
- log block metadata reads and writes
- stream fanout for address/topic indexes

Important log-specific view types:

- `LogSequencingState`
- `LogBlockWindow`

## Persisted Model

The persisted bytes still use the existing records:

- `MetaState`
- `BlockMeta`
- `LogDirectoryBucket`
- `BlockLogHeader`
- block-keyed log blobs
- manifests, tails, and chunks

The code treats those bytes through cleaner shared and family-local helpers instead of exposing the mixed record shape everywhere.

## Query Semantics

`query_logs` works in ascending finalized-block order.

- `RangeResolver` clips the request to the indexed finalized head.
- `LogWindowResolver` maps the resolved block range to a log-ID range.
- indexed queries execute through shared candidate execution on primary IDs.
- broad queries can fall back to log-family block scan.
- pagination uses `resume_log_id` as a declarative lower bound.
- responses return `cursor_block` separately from `next_resume_log_id`.

The executor preserves primary IDs through page assembly so `has_more`, `next_resume_log_id`, and `cursor_block` are exact.

## Ingest Semantics

`IngestEngine` is now an orchestrator.

It is responsible for:

- finalized sequence and parent validation
- coordinating log artifact writes
- coordinating stream appends
- advancing shared finalized state

The logs family owns:

- directory-bucket writes
- block-log-header writes
- block-log blob writes
- log block-metadata writes
- block-hash lookup writes
- stream fanout rules

The shared stream layer owns append/seal lifecycle.

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
