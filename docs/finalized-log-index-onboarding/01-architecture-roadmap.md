# Finalized Log Index Architecture Roadmap

This guide is for a new developer who wants to understand the current finalized-log-index architecture.

The crate is now a transport-free finalized history-query service whose wave-1 family is logs.

## Scope

Focus first on the happy path:

1. a finalized block is ingested
2. logs are packed, indexed, and assigned monotonic log IDs
3. `query_logs` resolves a finalized range
4. the logs family executes either the indexed path or the block-scan fallback
5. the result is returned as `QueryPage<Log>`

## Service Shape

The main concrete service is `FinalizedHistoryService` in `crates/finalized-log-index/src/api/service.rs`.

Its public method surface is split across:

- `crates/finalized-log-index/src/api/query_logs.rs`
- `crates/finalized-log-index/src/api/write.rs`
- `crates/finalized-log-index/src/api/service.rs`

Operational methods such as maintenance, GC, pruning, head inspection, and health remain on the concrete service.

## Current Module Roles

- `src/core/*`
  shared finalized-history request, range, page, runtime, and candidate-execution logic
- `src/logs/*`
  log-family filter semantics, windowing, materialization, block scan, and query orchestration
- `src/ingest/engine.rs`
  finalized ingest orchestration
- `src/logs/ingest.rs`
  log-specific artifact writes, block-metadata writes, and stream fanout
- `src/streams/keys.rs`
  stream/shard key helpers

## Storage Model

The storage layout is still:

- meta store
  - `meta/state`
  - `block_meta/*`
  - `block_hash_to_num/*`
  - `log_locator_pages/*`
  - `manifests/*`
  - `tails/*`
- blob store
  - `log_packs/*`
  - `chunks/*`

The query surface no longer exposes `block_hash` mode, but the block-hash lookup is still written and pruned as an operational index.

## What To Read First

1. `crates/finalized-log-index/src/lib.rs`
2. `crates/finalized-log-index/src/api/query_logs.rs`
3. `crates/finalized-log-index/src/api/service.rs`
4. `crates/finalized-log-index/src/core/range.rs`
5. `crates/finalized-log-index/src/logs/query.rs`
6. `crates/finalized-log-index/src/ingest/engine.rs`
7. `crates/finalized-log-index/tests/finalized_index.rs`

## Mental Model

Remember these five points:

1. `MetaState` is still the authority for finalized head and next log ID.
2. Full logs live in packed blobs; the indexed path works on roaring-stream candidates first.
3. Each stream still uses tail plus immutable chunk history.
4. Pagination now depends on preserving log IDs through exact-match materialization.
5. The crate surface is centered on `query_logs`, not `query_finalized`.
