# Block-Based Query Pagination

## Status

Superceded before implementation.

The original motivation was to align the transport-free substrate with the
`queryX` block-based pagination model and remove primary-ID continuation
tokens.

That direction is currently shelved because real-world trace blocks can be far
larger than the plan assumed. In particular, very large single-block trace
payloads make unconditional "always finish the current block" pagination risky
for latency and response size.

If this topic is revisited, it should start with benchmarking and profiling
rather than direct implementation.

## Original Goal

Align the transport-free query substrate with the `queryX` pagination model:

- page by block range
- return `cursor_block` as the continuation anchor
- stop only on block boundaries
- remove primary-ID continuation tokens from the public query contract

This plan covers `query_logs`, `query_transactions`, and `query_traces`.
`query_blocks` already uses block-based pagination.

## Why Change It

The current indexed-family API exposes substrate-specific pagination state:

- `resume_id` on request types
- `next_resume_id` on `QueryPageMeta`

That model is precise, but it leaks internal primary-ID mechanics into the
surface area and creates two different mental models:

- blocks paginate by block number
- indexed families paginate by family ID

The `queryX` reference shape is simpler:

- the client provides a block range
- the response returns the last block scanned
- the next page starts one block past `cursorBlock`

Now that unfiltered queries also have a direct block-scan path, the substrate
has a natural opportunity to make all families follow the same block-aligned
pagination rule.

## Design Choice

Adopt one pagination rule for all query families:

1. resolve the finalized block range
2. scan within that range
3. always finish the current block before returning
4. set `cursor_block` to the last block scanned
5. omit any primary-ID continuation token from the public API

Do **not** keep dual pagination models long-term.

During implementation, internal helpers may still use family IDs to drive the
indexed scan, but that state should not remain part of the public request or
response contract.

## Scope

### In scope

- removing `resume_id` from:
  - `QueryLogsRequest`
  - `QueryTransactionsRequest`
  - `QueryTracesRequest`
- removing `next_resume_id` from `QueryPageMeta`
- making indexed queries block-aligned rather than primary-ID-aligned
- keeping `cursor_block`, `has_more`, and resolved block refs exact
- updating tests and docs to the new continuation model

### Out of scope

- descending traversal
- tag handling in this crate
- field selection
- joins
- changing the queryX response envelope owned by the RPC layer

## Behavioral Target

After this change, all transport-free query methods should behave like this:

```python
request:
    from_block
    to_block
    order
    limit
    filter

response.meta:
    resolved_from_block
    resolved_to_block
    cursor_block
    has_more
```

Continuation becomes:

- next ascending page starts at `cursor_block.number + 1`
- pagination is complete when `cursor_block.number == resolved_to_block.number`

The effective `limit` remains a target, not a hard cap, because the executor
may need to include all matches from the last scanned block.

## Current State

Today the shared indexed executor fetches `limit + 1` candidates and can stop
mid-block. That is why the substrate returns:

- exact `next_resume_id`
- exact primary-ID continuation semantics

The unfiltered side path added recently already scans block-by-block, but it
still preserves `resume_id` compatibility.

So the codebase is currently in an intentionally transitional state:

- execution paths are partly block-oriented
- the public indexed-family API is still ID-oriented

This plan removes that mismatch.

## Proposed Execution Model

### Blocks

No semantic change. `query_blocks` already paginates by block number and
returns block-aligned pages.

### Logs, txs, and traces

Each indexed family should continue to use its existing indexed search
machinery to find matching candidates efficiently, but page assembly should be
block-aligned.

The key rule:

- once any item from block `B` is admitted to the page, all remaining matches
  from block `B` must also be admitted before the page can stop

That means the executor should:

1. stream matches in ascending primary-ID order as today
2. track the current block of the page tail
3. once `effective_limit` is reached, continue consuming matches only while
   they belong to that same tail block
4. stop before the first match from the next block

This preserves the benefits of indexed search while adopting block-based
continuation semantics.

## API Changes

### Request types

Change the indexed request shape from:

```rust
pub struct IndexedQueryRequest<F> {
    pub from_block: Option<u64>,
    pub to_block: Option<u64>,
    pub from_block_hash: Option<[u8; 32]>,
    pub to_block_hash: Option<[u8; 32]>,
    pub order: QueryOrder,
    pub resume_id: Option<u64>,
    pub limit: usize,
    pub filter: F,
}
```

to:

```rust
pub struct IndexedQueryRequest<F> {
    pub from_block: Option<u64>,
    pub to_block: Option<u64>,
    pub from_block_hash: Option<[u8; 32]>,
    pub to_block_hash: Option<[u8; 32]>,
    pub order: QueryOrder,
    pub limit: usize,
    pub filter: F,
}
```

### Page metadata

Change:

```rust
pub struct QueryPageMeta {
    pub resolved_from_block: BlockRef,
    pub resolved_to_block: BlockRef,
    pub cursor_block: BlockRef,
    pub has_more: bool,
    pub next_resume_id: Option<u64>,
}
```

to:

```rust
pub struct QueryPageMeta {
    pub resolved_from_block: BlockRef,
    pub resolved_to_block: BlockRef,
    pub cursor_block: BlockRef,
    pub has_more: bool,
}
```

## Implementation Strategy

### 1. Remove public continuation tokens

- remove `resume_id` from the transport-free indexed request structs
- remove `next_resume_id` from `QueryPageMeta`
- update the service docs and overview docs accordingly

This should be done only together with the execution change below, not before.

### 2. Make page construction block-aligned

Refactor the shared query runner so that page assembly is driven by block
boundaries rather than `limit + 1` primary IDs.

The most likely clean seam is:

- keep the candidate-generation logic mostly unchanged
- change the page-building logic to accumulate through the end of the tail
  block

The runner should return enough information to know:

- whether another block with matches remains beyond the page
- the exact final `cursor_block`

### 3. Simplify the unfiltered side path

Once `resume_id` is gone, the direct block-scan path becomes simpler:

- no resume-ID validation
- no ID-based skipping within a block
- direct block iteration until the page tail block is complete

This path should become the reference shape for non-indexed queries.

### 4. Update pagination callers and tests

Replace resume-token tests with block-based continuation tests:

- first page returns `cursor_block = B`
- second request starts at `B + 1`
- no `next_resume_id` assertions remain

Tests should cover:

- a page ending exactly on a block boundary
- a page that exceeds `limit` because the last scanned block has multiple
  matches
- indexed and unfiltered queries both following the same continuation rule

## Risks

### Response-size overshoot

Block-aligned completion means pages can exceed `limit`, especially for
trace-heavy blocks.

This is intentional and matches the `queryX` spec, but it changes the previous
bounded-by-primary-ID behavior.

Mitigation:

- keep the execution budget and range limits
- profile heavy trace blocks after the change
- add explicit guardrails later only if real workloads demand them

### Indexed-runner complexity

The shared runner is currently optimized around exact primary-ID continuation.
Trying to preserve every existing optimization while changing pagination
semantics could produce awkward code.

Mitigation:

- prefer a cleaner block-aligned runner even if it is slightly less minimal as
  a diff
- keep candidate generation and page assembly as separate concerns

### Transitional churn

RPC-layer code, tests, benchmarks, and docs may all assume `resume_id` today.

Mitigation:

- land the substrate and RPC changes in a coordinated stack
- avoid a long-lived intermediate state with both continuation models exposed

## Work Packages

### 1. Public API cleanup

- remove `resume_id` from indexed request types
- remove `next_resume_id` from page metadata
- update crate exports and docs

### 2. Shared runner refactor

- redesign page assembly around block alignment
- preserve exact `cursor_block` and `has_more`
- keep indexed filtering behavior unchanged

### 3. Unfiltered runner cleanup

- delete resume-ID-specific logic from direct block scans
- keep the block-direct path as the empty-filter execution route

### 4. Tests and benches

- rewrite pagination tests to use block continuation
- update any benchmark or helper code that assumes ID-token pagination

### 5. RPC integration follow-up

- make the RPC layer page by block range only
- remove substrate-token translation glue if any exists

## Success Criteria

This plan is successful if:

- `query_logs`, `query_transactions`, `query_traces`, and `query_blocks` all
  paginate by block range
- no transport-free query request contains `resume_id`
- no transport-free page metadata contains `next_resume_id`
- indexed and unfiltered queries share the same block-aligned continuation
  semantics
- docs and tests describe one pagination model instead of two

## Non-Goals

This plan is not trying to solve every queryX gap at once. In particular, it
does not attempt to add:

- descending order
- richer block fields
- joins
- field projection
- tag parsing in the substrate

Those remain separate follow-up decisions.
