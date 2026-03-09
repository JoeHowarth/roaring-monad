# Finalized History Query Architecture Roadmap

This guide is for a new developer who wants to understand the planned target architecture for `crates/finalized-log-index` after the finalized-history-query migration.

Start with [docs/finalized-history-query-migration-plan.md](/home/jhow/roaring-monad/docs/finalized-history-query-migration-plan.md). This onboarding set explains that target architecture and maps it back to the current codebase.

Important framing:

- this guide describes the intended wave-1 architecture
- the current codebase still uses the pre-migration layout
- use these docs to understand both the target boundaries and where the logic lives today

## Scope

Focus first on the planned wave-1 `query_logs` path:

1. a finalized block is ingested
2. logs receive finalized monotonic log IDs
3. shared roaring stream infrastructure is updated
4. a transport-free `query_logs` request resolves a block window
5. the logs family maps that block window to a log-ID window
6. the query returns a page of logs plus pagination / reorg metadata

Do not start with future joins, block bundles, transactions, traces, or transfers. Those are follow-on layers.

## The System In One Picture

The target design has three layers inside `crates/finalized-log-index`:

- shared finalized-history substrate
  - range normalization
  - page metadata
  - runtime health
  - roaring stream planning and execution
- family adapters
  - `logs` in wave 1
  - later `transactions`, `traces`, `transfers`
- method adapters
  - wave-1 `query_logs`
  - later `query_blocks`, `query_transactions`, `query_traces`, `query_transfers`

And one explicit boundary outside the crate:

- the RPC server crate
  - owns JSON-RPC parsing
  - owns block-tag policy
  - owns error mapping
  - owns `fields` parsing and final response-envelope construction

## Core Concepts To Learn First

### 1. Transport-free method boundary

The target public service boundary is not JSON-RPC-shaped. It is Rust-shaped and transport-free:

- `QueryLogsRequest`
- `ExecutionBudget`
- `QueryPage<Log>`
- `QueryPageMeta`

Why it matters:

- this crate should not know about HTTP or JSON-RPC envelopes
- the RPC crate should not need to understand storage internals

### 2. Block window vs primary-ID window

The shared layer resolves a finalized block window.

The logs family then maps that block window into a log-ID window using log-specific metadata such as:

- `BlockMeta.first_log_id`
- `BlockMeta.count`

Why it matters:

- range resolution is partly shared
- primary-window derivation is still family-specific in wave 1

### 3. Resume token vs reorg metadata

Wave-1 pagination has two different outputs:

- `next_resume_log_id`
  - the item-level token to feed into the next request's `resume_log_id`
- `cursor_block`
  - block-scoped metadata for client-side reorg handling

Those are not interchangeable.

Why it matters:

- the current executor can stop mid-block once the limit is hit
- resuming from a block-only cursor would duplicate or skip logs
- the request is stateless, so the log-ID token is a declarative lower bound rather than an opaque session cursor

### 4. Shared views over persisted records

The current persisted records mix shared and log-specific fields:

- `MetaState`
- `BlockMeta`

The target architecture does not require redesigning those bytes in wave 1.

Instead it introduces conceptual shared views such as:

- finalized head state
- block identity

and log-specific views such as:

- log sequencing state
- log block window

Why it matters:

- it gives cleaner ownership without mixing storage redesign into the refactor

### 5. Shared substrate vs family adapter

The shared substrate should own:

- stream overlap estimation
- candidate-set intersection
- exact pagination boundary detection
- page metadata assembly

The logs family should own:

- address/topic semantics
- exact log matching
- block-window to log-ID-window mapping
- broad-query block scan fallback
- log materialization

Why it matters:

- future families should reuse the shared mechanics without inheriting log semantics

## Suggested Study Roadmap

### Stage 1. Read the target architecture first

Read:

- [docs/finalized-history-query-migration-plan.md](/home/jhow/roaring-monad/docs/finalized-history-query-migration-plan.md)
- [docs/finalized-history-query-onboarding/02-file-reading-order.md](/home/jhow/roaring-monad/docs/finalized-history-query-onboarding/02-file-reading-order.md)
- [docs/finalized-history-query-onboarding/03-high-level-pseudocode.md](/home/jhow/roaring-monad/docs/finalized-history-query-onboarding/03-high-level-pseudocode.md)

Questions to answer:

- what does the RPC crate own vs this crate own?
- what is the difference between a method adapter and a family adapter?
- why is `query_logs` the target method shape instead of `eth_getLogs`?

### Stage 2. Map the current public surface to the target one

Read current files:

- `crates/finalized-log-index/src/api.rs`
- `crates/finalized-log-index/src/domain/filter.rs`
- `crates/finalized-log-index/src/domain/types.rs`

Goals:

- identify the current `FinalizedLogIndex` surface
- see where `QueryOptions` and `LogFilter` live today
- separate today’s mixed types into target shared vs log-specific ownership

Questions to answer:

- which current public types become transport-free method-layer types?
- which ones stay log-family-specific?
- which concrete service methods must stay on the orchestrator even after the split?

### Stage 3. Learn the shared substrate candidates

Read current files:

- `crates/finalized-log-index/src/domain/keys.rs`
- `crates/finalized-log-index/src/codec/manifest.rs`
- `crates/finalized-log-index/src/codec/chunk.rs`
- `crates/finalized-log-index/src/query/planner.rs`
- `crates/finalized-log-index/src/ingest/engine.rs`

Goals:

- understand the roaring stream substrate
- see what logic belongs under `streams/`
- identify which helpers are already generic enough to extract

Questions to answer:

- what parts of stream planning are log-agnostic?
- what parts of append + seal lifecycle are family-agnostic?
- where does exact pagination boundary detection need to be added?

### Stage 4. Learn the log family boundary

Read current files:

- `crates/finalized-log-index/src/query/engine.rs`
- `crates/finalized-log-index/src/query/executor.rs`
- `crates/finalized-log-index/src/ingest/engine.rs`

Focus on:

- block-window to log-ID-window mapping
- topic/address exact-match rules
- `collect_stream_appends`
- broad-query block scan fallback
- log materialization by locator

Questions to answer:

- what remains log-specific in wave 1?
- why does `RangeResolver` stop at block windows?
- why must broad-query block scan stay in the logs layer for now?

### Stage 5. Learn the new pagination model

Read current files:

- `crates/finalized-log-index/src/query/executor.rs`
- `crates/finalized-log-index/tests/finalized_index.rs`
- `crates/finalized-log-index/tests/differential_and_gc.rs`

Goals:

- understand why the old `Vec<Log>` result is insufficient
- see how the current executor stops at `max_results`
- map that behavior to the target `QueryPageMeta`

Questions to answer:

- why must the executor preserve primary IDs until the page is finalized?
- why are `has_more`, `next_resume_log_id`, and `cursor_block` all needed?
- why does `limit = 0` need to be rejected explicitly?
- which tests must cover indexed-path and block-scan pagination separately?

### Stage 6. Learn what is intentionally deferred

Read the deferred-work sections of the migration plan.

Questions to answer:

- why are relation helpers deferred until canonical artifact stores exist?
- why is `block_hash` mode out of scope for this crate in wave 1?
- why is descending traversal a follow-on change instead of a wave-1 feature?

## Mental Model To Keep

The cleanest way to think about the target architecture is:

- the method layer speaks in block ranges, limits, continuation, and page metadata
- the family layer speaks in filters, primary IDs, exact-match rules, and materialization
- the shared substrate speaks in roaring streams, shards, overlap estimates, and candidate execution

If a piece of logic does not fit one of those roles cleanly, it is probably still sitting on the wrong side of a boundary.
