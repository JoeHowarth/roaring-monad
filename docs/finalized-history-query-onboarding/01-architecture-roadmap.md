# Finalized History Query Architecture Roadmap

This guide is for a developer who wants to understand the current wave-1 history-query architecture in `crates/finalized-log-index`.

Start with [docs/finalized-history-query-migration-plan.md](/home/jhow/roaring-monad/docs/finalized-history-query-migration-plan.md). That document explains the design goals. This guide explains the code that now implements that shape.

## Scope

Focus first on the `query_logs` path:

1. a finalized block is ingested
2. logs receive finalized monotonic log IDs
3. shared roaring-stream infrastructure is updated
4. `query_logs` resolves a finalized block window
5. the logs family maps that block window to a log-ID window
6. the query returns `QueryPage<Log>` plus pagination and reorg metadata

## The System In One Picture

The crate now has three explicit layers:

- method adapters
  - `src/api/query_logs.rs`
  - `src/api/write.rs`
  - `src/api/service.rs`
- shared finalized-history substrate
  - `src/core/clause.rs`
  - `src/core/page.rs`
  - `src/core/range.rs`
  - `src/core/execution.rs`
  - `src/core/runtime.rs`
  - `src/streams/keys.rs`
- family adapters
  - `src/logs/filter.rs`
  - `src/logs/window.rs`
  - `src/logs/materialize.rs`
  - `src/logs/block_scan.rs`
  - `src/logs/query.rs`
  - `src/logs/ingest.rs`

The RPC crate is still outside this boundary. It owns JSON-RPC parsing, block-tag policy, error mapping, and final response-envelope formatting.

## Core Concepts

### 1. Transport-free method boundary

The public query boundary is now:

- `QueryLogsRequest`
- `ExecutionBudget`
- `QueryPage<Log>`
- `QueryPageMeta`

This crate no longer exposes the old `query_finalized(LogFilter, QueryOptions) -> Vec<Log>` surface.

### 2. Block window vs log-ID window

`RangeResolver` resolves a finalized block window first. The logs family then derives the log-ID window from `BlockMeta.first_log_id` and `BlockMeta.count`.

That split is the main reusable boundary for future families.

### 3. Resume token vs reorg metadata

Wave 1 now returns:

- `next_resume_log_id`
  - item-level continuation
- `cursor_block`
  - block-scoped reorg metadata

They are intentionally different outputs.

### 4. Shared views over persisted records

The storage bytes are still the existing `MetaState` and `BlockMeta` records, but the code now exposes shared views such as:

- `FinalizedHeadState`
- `BlockIdentity`

and log-specific views such as:

- `LogSequencingState`
- `LogBlockWindow`

### 5. Shared candidate execution

The indexed query path now preserves primary IDs until page assembly so it can emit:

- exact `has_more`
- exact `next_resume_log_id`
- correct `cursor_block`

That logic lives under `src/core/execution.rs` and is reused by the logs family through `LogMaterializer`.

## File Walk

Read these in order:

1. `crates/finalized-log-index/src/lib.rs`
2. `crates/finalized-log-index/src/api/query_logs.rs`
3. `crates/finalized-log-index/src/api/service.rs`
4. `crates/finalized-log-index/src/core/page.rs`
5. `crates/finalized-log-index/src/core/range.rs`
6. `crates/finalized-log-index/src/logs/filter.rs`
7. `crates/finalized-log-index/src/logs/window.rs`
8. `crates/finalized-log-index/src/logs/materialize.rs`
9. `crates/finalized-log-index/src/logs/block_scan.rs`
10. `crates/finalized-log-index/src/logs/query.rs`
11. `crates/finalized-log-index/src/ingest/engine.rs`
12. `crates/finalized-log-index/src/logs/ingest.rs`
13. `crates/finalized-log-index/tests/finalized_index.rs`

## What Is Still Deferred

Wave 1 still does not implement:

- `block_hash` query mode in this crate
- descending traversal
- non-log families
- relation hydration
- canonical block/transaction/trace artifact stores

## Mental Model

Keep these roles distinct:

- method layer: block ranges, limits, continuation, page metadata
- family layer: log filter semantics, materialization, block scan fallback
- shared substrate: range resolution, stream math, candidate execution, runtime state
