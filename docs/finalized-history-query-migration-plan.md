# Finalized History Query Migration Plan

## Purpose

This document describes how to migrate `crates/finalized-log-index` from a log-specific finalized index into a reusable finalized-history query foundation.

The target consumer is an RPC server crate that owns HTTP, JSON-RPC parsing, request validation, tag resolution policy, and final response formatting.

The goal of this migration is to make `crates/finalized-log-index` the transport-free engine and service layer that future RPC methods can build on:

- `eth_queryBlocks`
- `eth_queryTransactions`
- `eth_queryLogs`
- `eth_queryTraces`
- `eth_queryTransfers`

The first implementation wave remains log-only, but the internal boundaries should be chosen for the larger family of query methods rather than for `eth_getLogs`.

There is no production backwards-compatibility requirement for the current internal or public APIs. This migration should take advantage of that freedom to choose cleaner names and boundaries, while still avoiding unnecessary scope expansion.

## Decisions Captured In This Plan

This plan reflects the following decisions:

- this crate is not responsible for HTTP or JSON-RPC transport
- this crate does not need to return the final JSON-RPC response envelope
- this crate should return enough structured metadata that the RPC crate can construct that envelope directly
- wave 1 in this crate does not include `block_hash` query mode
- future families are expected to have monotonic primary IDs, even if physical artifact storage is block-keyed
- the primary long-term method shape is `eth_queryLogs`, not `eth_getLogs`

## Scope

### In scope

- refactor the crate into explicit shared and family-specific layers
- preserve current finalized-only ingest semantics
- preserve current range-based log query correctness while the code is moved
- reframe the service API around transport-free `eth_queryLogs` semantics
- return page metadata that makes the RPC response envelope easy to build
- create extension points for future families without implementing them yet

### Out of scope

- HTTP server concerns
- JSON-RPC request parsing and error-code mapping
- block-tag parsing and policy for `latest` / `safe` / `finalized`
- `block_hash` query mode in this crate
- field projection and normalized response assembly in wave 1
- implementing `eth_queryTransactions`, `eth_queryTraces`, `eth_queryTransfers`, or `eth_queryBlocks`
- storage redesign for block bundles, receipts, or trace materialization
- speculative generic relation frameworks

## Why The Current Plan Needs To Change

The current crate already contains useful reusable mechanics:

- roaring stream storage
- shard math and stream key construction
- tail / manifest / chunk lifecycle
- block-range clipping against finalized head
- clause selectivity estimation
- candidate-set intersection
- runtime degraded / throttled behavior

But the crate still presents itself as a log-specific index with a log-specific public trait:

- `FinalizedLogIndex`
- `query_finalized(LogFilter, QueryOptions) -> Vec<Log>`

That shape is too narrow for the intended future query methods and too tied to the current internal organization.

The new plan is to make the crate a history-query engine with:

- shared finalized-history substrate
- family adapters such as `logs`
- method adapters such as `query_logs`

## Architectural Direction

The target architecture has three layers.

### 1. Shared finalized-history substrate

This layer owns mechanics that should be reusable across blocks, transactions, logs, traces, and transfers:

- finalized range normalization
- block reference loading
- runtime health transitions
- shard-local stream planning
- overlap estimation
- candidate-set intersection
- stream append and seal lifecycle

This layer must not know log topic semantics or future transaction / trace filter semantics.

### 2. Family adapters

Each family owns its own semantics:

- filter meaning
- stream fanout rules
- exact-match logic
- family-specific mapping from block windows to primary-ID windows
- materialization from storage
- family-specific broad-query fallback
- relation references that primary objects can expose

Wave 1 only implements the `logs` family.

### 3. Method adapters

This layer adapts family queries into service methods whose semantics match the future RPC methods without being tied to JSON-RPC wire types.

For wave 1 that means a transport-free `query_logs` API shaped around `eth_queryLogs` semantics:

- resolved block range
- explicit traversal order, with ascending-only support in wave 1
- requested primary-object limit
- optional resume from a declarative log-ID boundary
- page metadata containing resolved `fromBlock`, `toBlock`, `cursorBlock`, a next-page resume token, and an explicit completion signal

This layer should not parse JSON strings, assign JSON-RPC error codes, or build the final normalized response envelope.

## Boundary Between This Crate And The RPC Crate

### This crate should own

- finalized-history indexing and query execution
- family-specific filters and materialization
- transport-free request / response types
- page metadata needed for pagination and reorg detection
- runtime health, maintenance, GC, and ingest orchestration

### The RPC crate should own

- JSON-RPC method names
- request parsing and schema validation
- block-tag parsing and resolution policy
- defaulting rules for omitted request fields
- JSON-RPC error mapping
- `fields` parsing and normalized response assembly

That split keeps the hard storage and indexing logic here, while keeping transport and envelope concerns in the RPC server crate where they belong.

## Transport-Free Query Model

The primary service surface should be shaped around `eth_queryLogs`, but expressed in transport-free Rust types.

Recommended first-wave shape:

```rust
pub enum QueryOrder {
    Ascending,
}

pub struct QueryLogsRequest {
    pub from_block: u64,
    pub to_block: u64,
    pub order: QueryOrder,
    pub resume_log_id: Option<u64>,
    pub limit: usize,
    pub filter: LogFilter,
}

pub struct ExecutionBudget {
    pub max_results: Option<usize>,
}

pub struct BlockRef {
    pub number: u64,
    pub hash: [u8; 32],
    pub parent_hash: [u8; 32],
}

pub struct QueryPageMeta {
    pub resolved_from_block: BlockRef,
    pub resolved_to_block: BlockRef,
    pub cursor_block: BlockRef,
    pub has_more: bool,
    pub next_resume_log_id: Option<u64>,
}

pub struct QueryPage<T> {
    pub items: Vec<T>,
    pub meta: QueryPageMeta,
}

#[async_trait::async_trait]
pub trait FinalizedLogQueries: Send + Sync {
    async fn query_logs(
        &self,
        request: QueryLogsRequest,
        budget: ExecutionBudget,
    ) -> Result<QueryPage<Log>>;
}

#[async_trait::async_trait]
pub trait FinalizedHistoryWriter: Send + Sync {
    async fn ingest_finalized_block(&self, block: Block) -> Result<IngestOutcome>;
}
```

Important notes:

- `QueryLogsRequest` is transport-free. The RPC crate is responsible for mapping JSON-RPC input into it.
- `ExecutionBudget` is server-side control, not part of the RPC method request.
- Wave 1 should keep `ExecutionBudget` minimal and only include controls the engine actually enforces now, namely result count.
- `limit` must be at least `1`. `limit = 0` is invalid in wave 1.
- The effective primary-object page size is `min(request.limit, budget.max_results.unwrap_or(request.limit))`.
- If the caller wants stricter policy for oversized requests, that should be enforced before calling this crate rather than hidden inside the engine.
- Wave 1 only supports `QueryOrder::Ascending`. The enum is present now so descending traversal can be added later without reshaping the request type.
- `resume_log_id` is a declarative resume boundary, not an opaque session token.
- In wave 1 ascending mode, when `resume_log_id` is present, the query resumes strictly after that finalized log ID.
- The engine validates that `resume_log_id`, if present, lies within the resolved block window. Otherwise the request is invalid.
- Requests are stateless. If the caller reuses the same `from_block`, `to_block`, `order`, and `filter`, then feeding `next_resume_log_id` into the next request yields exact page continuation. If those fields change, the request is simply a new query with a different lower bound.
- `has_more` is the authoritative completion signal. `has_more = false` means the caller has exhausted the resolved query window.
- `next_resume_log_id` is only present when `has_more = true`, and it is the last returned log ID from the page.
- `cursor_block` is the block containing the last returned primary object. If the page is empty, it falls back to the resolved range endpoint that the engine actually examined. It remains block-scoped metadata for client-side reorg handling and is not the pagination token.
- `QueryPageMeta` exists so the RPC crate can construct the future `fromBlock`, `toBlock`, `cursorBlock`, and resume fields without re-querying storage.
- `block_hash` query mode is outside this crate's wave-1 query surface.
- The engine must determine `has_more` exactly, for example by collecting up to `effective_limit + 1` matching primary IDs or by an equivalent lookahead strategy before final result trimming.

## Relation Strategy

The main query path should not eagerly hydrate relations.

Wave 1 should stop at:

- primary objects
- page metadata

Relation hydration and canonical artifact lookup should move to a later expansion once this crate also owns the additional artifact stores needed to serve non-log families.

## Shared Types vs Log-Specific Types

The current persisted records mix shared finalized-history data with log-specific sequencing and window fields:

- `MetaState.indexed_finalized_head` and `MetaState.writer_epoch` are shared finalized-history concerns
- `MetaState.next_log_id` is log-family sequencing state
- `BlockMeta.block_hash` and `BlockMeta.parent_hash` are shared block identity fields
- `BlockMeta.first_log_id` and `BlockMeta.count` are log-family window fields

Wave 1 should not redesign those bytes just to make the module tree cleaner.

Instead, wave 1 should:

- keep the current persisted bytes and codecs stable unless a small targeted change is clearly worthwhile
- introduce shared view types over the shared portions of those records
- introduce log-specific view types over the log-only portions
- make shared modules depend on the shared views, not on log-specific types

Recommended conceptual split:

```rust
pub struct FinalizedHeadState {
    pub indexed_finalized_head: u64,
    pub writer_epoch: u64,
}

pub struct LogSequencingState {
    pub next_log_id: u64,
}

pub struct BlockIdentity {
    pub number: u64,
    pub hash: [u8; 32],
    pub parent_hash: [u8; 32],
}

pub struct LogBlockWindow {
    pub first_log_id: u64,
    pub count: u32,
}
```

The implementation can decode a persisted `MetaState` or `BlockMeta` once, then expose these views without changing the stored representation.

## Monotonic Primary IDs vs Block-Keyed Storage

The long-term architecture should separate:

- logical primary IDs used by indexes and query execution
- physical artifact storage layout

Those do not need to match.

The shared query substrate should operate on family-specific monotonic primary IDs:

- `LogId`
- later `TxId`
- later `TraceId`
- later `TransferId`

Family materializers can then map those IDs into block-keyed artifact storage.

That is compatible with the likely future storage direction:

- canonical block-keyed bundles
- per-block offsets for logs, transactions, traces, and transfers
- auxiliary pages that map primary-ID windows back to block intervals

Wave 1 should preserve that distinction in naming and interfaces even if logs still use the current locator-based storage path.

## Proposed Module Split

This is a conceptual target, not a file-move checklist.

```text
crates/finalized-log-index/src/
  api/
    mod.rs
    service.rs
    query_logs.rs
    write.rs
  core/
    clause.rs
    execution.rs
    page.rs
    range.rs
    refs.rs
    runtime.rs
    ids.rs
  codec/
    primitives.rs
    finalized_state.rs
  streams/
    keys.rs
    manifest.rs
    chunk.rs
    planner.rs
    writer.rs
    catalog.rs
  logs/
    types.rs
    filter.rs
    window.rs
    index_spec.rs
    ingest.rs
    materialize.rs
    block_scan.rs
    query.rs
  store/
  gc/
  recovery/
  metrics/
```

Notes:

- `api/query_logs.rs` is a service-method adapter for `eth_queryLogs` semantics, not a JSON-RPC transport module.
- `core/page.rs` and `core/refs.rs` exist specifically to make the RPC response envelope easy to construct.
- `logs/window.rs` owns log-specific block-window to log-ID-window mapping.
- `logs/block_scan.rs` keeps the broad-query fallback family-specific in wave 1.

## Key Internal Interfaces

Wave 1 should introduce a few explicit boundaries before deeper cleanup.

### `RangeResolver`

Purpose:

- normalize and validate a finalized block window
- clip against indexed finalized head
- load block references needed for page metadata

Wave 1 constraint:

- `RangeResolver` resolves block windows only
- it must not resolve log-ID windows

Family-specific mapping from block windows to primary-ID windows remains in the family layer.

### `PrimaryMaterializer`

Purpose:

- load a primary object by its monotonic ID
- apply family-specific exact-match logic

Example shape:

```rust
#[async_trait::async_trait]
pub trait PrimaryMaterializer {
    type Primary;
    type Filter;
    type Id: Copy;

    async fn load_by_id(&self, id: Self::Id) -> Result<Option<Self::Primary>>;
    fn exact_match(&self, item: &Self::Primary, filter: &Self::Filter) -> bool;
}
```

For logs this wraps the existing locator-based `load_log_by_id`.

### `PrimaryIndexer`

Purpose:

- derive stream appends for a primary object during ingest

Example shape:

```rust
pub trait PrimaryIndexer {
    type Primary;
    type Id: Copy;

    fn collect_stream_appends(
        &self,
        item: &Self::Primary,
        id: Self::Id,
        out: &mut Vec<StreamAppend>,
    );
}
```

### `FamilyWindowResolver`

Purpose:

- map a block window into the family's primary-ID window for indexed queries

Wave 1:

- the logs family owns this logic
- it reads log-specific block-window metadata
- it is not yet generic because the current persisted block metadata is log-specific

### `RelationLookup`

Wave 1 should not introduce relation lookup interfaces.

Defer relation helpers until this crate also owns the canonical artifact stores needed to serve block, transaction, trace, and transfer families directly.

## Migration Phases

The migration should be done in narrow phases. Each phase should leave the crate buildable and tested.

### Phase 0: Replace The Old Plan

Deliverables:

- this document
- explicit confirmation that the crate is a transport-free engine / service layer
- explicit confirmation that `eth_queryLogs` is the primary method target

Acceptance:

- no code changes
- alignment on the new boundary before editing internals

### Phase 1: Introduce Shared Request / Page Primitives

Goal:

- create the transport-free method-layer vocabulary before moving logic

Changes:

- move `Clause<T>` into a shared `core` module
- replace `QueryOptions` with transport-free request and budget types aligned to `query_logs`
- add shared page metadata types such as `BlockRef`, `QueryPageMeta`, `QueryPage<T>`, and `QueryOrder`
- add the log-ID resume-token fields needed for pagination
- move runtime health state out of `api.rs`

Acceptance:

- current log behavior remains unchanged
- page metadata types exist even if the old query path still adapts into them temporarily
- the crate surface no longer centers `eth_getLogs`

### Phase 2: Split Shared Views From Log-Specific Types

Goal:

- make shared finalized-history ownership explicit without redesigning stored bytes

Changes:

- split `domain/types.rs` into shared finalized-history views and log-specific types
- split `domain/filter.rs` into shared clause pieces and log-specific filter definitions
- split `codec/log.rs` into shared finalized-state codecs and log-only payload / locator codecs
- add view helpers over `MetaState` and `BlockMeta` rather than redesigning the persisted bytes

Acceptance:

- no behavior change
- shared modules decode shared state without importing log-only modules
- log-only sequencing and block-window fields remain owned by the log layer

### Phase 3: Extract The Stream Substrate

Goal:

- make roaring stream storage an explicit reusable layer

Changes:

- move shard math and stream-key helpers into `streams`
- move manifest / tail / chunk codecs and types into `streams`
- extract append + seal logic from `ingest/engine.rs` into a stream writer component
- extract overlap estimation helpers from `query/planner.rs` into shared stream planning helpers

Keep family-specific:

- stream family names such as `addr`, `topic0`, `topic1`, `topic2`, `topic3`
- clause enumeration and filter mapping

Acceptance:

- stream management code no longer depends on log filter types
- log indexing becomes a client of the shared stream substrate

### Phase 4: Extract Shared Block-Window And Candidate Execution Skeleton

Goal:

- separate reusable indexed-query mechanics from log materialization

Changes:

- extract finalized block-window normalization from `query/engine.rs`
- extract candidate intersection and candidate-driven execution from `query/executor.rs`
- introduce `RangeResolver` and `PrimaryMaterializer`-style boundaries
- preserve primary IDs alongside materialized items long enough to emit `next_resume_log_id`
- add exact pagination completion detection, using one-item lookahead or an equivalent mechanism to set `has_more` correctly
- keep broad-query block scan family-specific in wave 1

Acceptance:

- shared executor code does not import `Log`
- shared code works on candidate IDs and page metadata
- pagination metadata is exact at the page boundary, including `has_more` and `next_resume_log_id`
- family-specific block-scan logic remains outside the shared executor

### Phase 5: Move Log Query Logic Behind A Log Family Adapter

Goal:

- make logs look like one family using the shared substrate

Changes:

- move `collect_stream_appends` into `logs/ingest.rs`
- add `logs/window.rs` for block-window to log-ID-window mapping
- move exact-match and materialization into `logs/materialize.rs`
- move broad-query fallback into `logs/block_scan.rs`
- create `logs/query.rs` that binds:
  - shared range resolution
  - shared stream planning
  - shared candidate execution
  - log-specific window resolution
  - log-specific materialization
  - log-specific block-scan fallback

Acceptance:

- log-specific code lives under `logs/`
- shared query code no longer needs to know topic semantics

### Phase 6: Re-home The Concrete Service Surface

Goal:

- make the crate surface read like a transport-free history-query service

Changes:

- add `api/query_logs.rs` with a transport-free `query_logs` service interface
- add `api/write.rs` with the finalized ingest interface
- add `api/service.rs` for the concrete orchestrator
- keep `run_maintenance`, `run_gc_once`, and `prune_block_hash_index_below` as concrete-service methods because they remain tightly coupled to runtime state
- remove or retire `FinalizedLogIndex` once the new service surface is in place

Acceptance:

- the crate is easy for the RPC server crate to consume
- the crate no longer presents log querying as `query_finalized(LogFilter, QueryOptions)`
- operational methods still have a clear home

### Phase 7: Reorganize Tests Around The New Layers

Goal:

- make future family additions safer

Changes:

- keep current end-to-end ingest/query tests
- add focused tests for:
  - range resolution
  - page metadata construction
  - ascending-order request handling
  - resume-token pagination, including exact end-of-results detection
  - empty-page pagination edge cases:
    - no matches in the resolved block window
    - `resume_log_id` already beyond the last matching log while still inside the resolved block window
  - invalid request validation for:
    - `limit = 0`
    - `resume_log_id` outside the resolved block window
  - same-block multi-page pagination on the indexed path
  - same-block multi-page pagination on the broad-query block-scan path
  - stream writer behavior
  - stream overlap estimation
  - shared candidate execution
  - log family window resolution

Acceptance:

- regressions localize to the right layer
- pagination metadata is covered across indexed and block-scan execution paths, including empty-page edge cases
- the RPC crate can rely on page metadata without only end-to-end coverage

## Recommended PR Sequence

1. Add shared clause, page, ref, and budget primitives.
2. Split shared finalized-history views from log-only types and codecs.
3. Extract the stream substrate.
4. Extract shared block-window resolution and candidate execution.
5. Move log-specific windowing, materialization, and block-scan logic under `logs/`.
6. Re-home the service API around `query_logs` and writer traits.
7. Reorganize tests.

Each PR should combine mechanical movement with one narrow boundary extraction. Avoid mixing large file moves, public API churn, and behavior changes in the same review unless the coupling is unavoidable.

## What Should Preferably Stay Unchanged In Wave 1

To control refactor risk, the following should preferably remain unchanged during the structural migration:

- `MetaStore` and `BlobStore` traits
- ingest sequencing and parent validation
- current stream key format
- roaring bitmap encoding
- manifest and chunk layout
- log locator pages
- current packed-log encoding

These are not compatibility promises. They are scope controls. Change them only if a targeted change materially improves the architecture.

## Deferred Work

The following work should happen after the structural split lands:

- canonical block-, transaction-, receipt-, and trace-artifact stores
- relation hydration helpers built on top of those canonical artifact stores
- block-bundle storage redesign
- transaction family indexing
- trace family indexing
- transfer family indexing
- efficient projection for `fields`
- normalized response assembly helpers, if the RPC crate proves too repetitive
- fully generic relation systems, if multiple families actually need them

## Acceptance Criteria

The first migration wave is complete when all of the following are true:

- the crate has explicit shared modules for reusable finalized-history mechanics
- the logs family lives in a dedicated family-specific module tree
- the primary service surface is transport-free and aligned to `query_logs`, not `eth_getLogs`
- the wave-1 service surface supports ascending traversal explicitly through `QueryOrder`
- the query result includes page metadata, an explicit `has_more` signal, and a log-ID resume token sufficient to construct the future RPC response envelope and resume pagination
- shared code no longer depends on log topic semantics
- log-specific block-window and broad-query fallback logic remain correctly isolated
- observable range-based log-query correctness is unchanged aside from intentional API renaming, result wrapping, and removal of out-of-scope `block_hash` mode
- operational service methods still have a clear concrete home
- tests cover shared substrate behavior, log family behavior, and page metadata

## Follow-On Design Docs

Once this migration lands, the next design documents should cover:

- block-keyed canonical artifact storage
- transaction query indexing
- trace query indexing
- transfer query indexing
- relation hydration strategy once multiple families exist
- field projection and normalized response assembly boundaries
- adding descending traversal by extending `QueryOrder`
- richer pagination details for the full `eth_query*` surface
