# QueryBlocks Implementation

## Goal

Implement `query_blocks` as the next `queryX`-aligned method in
`finalized-history-query`.

This should be a small, low-risk addition that validates the crate's shared
service boundary and block-range machinery before taking on the much larger
transaction family.

## Why Blocks First

`queryBlocks` is the cheapest remaining `queryX` method:

- it does not require filtering
- it does not require family-owned primary IDs
- it does not require directory lookup
- it does not require bitmap indexes
- it can reuse the crate's existing finalized block-range and publication logic

That makes it a good test of whether the shared substrate is the right shape
for adding more `queryX` methods without introducing another full family.

## Design Choice

Do **not** model blocks as a new indexed family.

The crate already persists authoritative per-block metadata in `block_record`.
`queryBlocks` should start as a lightweight shared read path over that data.

This keeps the design honest:

- logs, txs, and traces are families with family-owned IDs and artifacts
- blocks are shared finalized metadata, already present in the substrate

## Scope

### In scope

- a transport-free `QueryBlocksRequest`
- a transport-free `query_blocks(...)` service method
- finalized-range resolution using the existing publication head
- pagination over block numbers in ascending order
- block materialization from shared block metadata
- exact `QueryPage<Block>` metadata using existing `BlockRef` values

### Out of scope

- descending traversal
- transport-level tags and field selection
- block relations
- introducing a block family with its own ingest path
- persisting a richer canonical block artifact set unless the minimal shape is
  proven insufficient

## Product Shape Versus Stored Shape

The reference `queryX` spec describes `eth_queryBlocks` as a block query
surface. The current crate, however, only stores block identity in the shared
`block_record`:

- `number`
- `hash`
- `parent_hash`

It does **not** currently persist broader block-header fields such as
`timestamp`.

So the first implementation should be framed as a substrate-level
`query_blocks` over finalized block identity. If later product requirements
need richer block fields, that should be a follow-up storage decision rather
than hidden scope creep in the first `queryBlocks` landing.

## Intended API Shape

Add a request type parallel to the existing transport-free query requests:

```rust
pub struct QueryBlocksRequest {
    pub from_block: Option<u64>,
    pub to_block: Option<u64>,
    pub from_block_hash: Option<[u8; 32]>,
    pub to_block_hash: Option<[u8; 32]>,
    pub order: QueryOrder,
    pub limit: usize,
}
```

And add a lightweight block result type:

```rust
pub struct Block {
    pub number: u64,
    pub hash: [u8; 32],
    pub parent_hash: [u8; 32],
}
```

The service boundary becomes:

```rust
async fn query_blocks(
    &self,
    request: QueryBlocksRequest,
    budget: ExecutionBudget,
) -> Result<QueryPage<Block>>;
```

No filter object is needed at this layer.

## Query Flow

The implementation should be intentionally simple:

1. resolve and clip the requested finalized block range with the existing
   range resolver
2. reject unsupported traversal modes using the existing `QueryOrder` rules
3. apply `limit` and execution budget using the existing pagination helpers
4. iterate block numbers in the resolved range
5. load `block_record` for each block
6. materialize `Block { number, hash, parent_hash }`
7. return `QueryPage<Block>` with:
   - `resolved_from_block`
   - `resolved_to_block`
   - `cursor_block`
   - `has_more`
   - `next_resume_id = None`

This path should not go through the indexed query runner. That runner is for
family-owned primary-ID queries; `queryBlocks` is a direct finalized block scan.

## Work Packages

### 1. Public API

- add `Block` and `QueryBlocksRequest`
- export them from `lib.rs`
- add `query_blocks(...)` to `FinalizedHistoryService`

### 2. Shared block query implementation

- add a small `blocks` query module or shared helper
- reuse `resolve_block_range(...)`
- add minimal page-planning logic for block-number scans

### 3. Materialization

- load `BlockRecord`
- convert it to the public `Block` shape
- use existing `BlockRef` values for page metadata

### 4. Tests

Add focused tests for:

- empty result when the requested range is fully beyond the finalized head
- clipping `to_block` to the finalized head
- pagination across multiple blocks
- exact `cursor_block` behavior
- `from_block_hash` / `to_block_hash` resolution if supported at this layer
- missing `block_record` handling on sparse/uninitialized ranges

## Success Criteria

This plan is successful if:

- the crate exposes a clean `query_blocks(...)` transport-free method
- the implementation is obviously shared-substrate code, not a disguised new
  family
- the diff is small relative to logs/traces
- the result clarifies whether the crate can add more `queryX` methods without
  forcing everything through the indexed-family path

## Follow-Ups If Needed

If the first implementation proves too narrow for product needs, follow-up work
can decide whether to:

- persist richer block-header artifacts
- support transport-level field selection over those artifacts
- add joins from other families onto block payload fields

Those should be separate decisions. The first `queryBlocks` landing should only
validate the shared finalized block query shape.
