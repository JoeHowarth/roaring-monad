# Global Log ID Newtypes And Shard Fix Plan

## Summary

This plan addresses two related problems in `crates/finalized-history-query`:

1. the current shard helper type is too narrow for the configured `LOCAL_ID_BITS = 24`
2. the query path loses structure by passing raw `u64` global log IDs around everywhere

The proposed end state introduces explicit ID newtypes, fixes shard width, and moves indexed execution onto shard-aware roaring bitmap operations without changing external query semantics.

## Problem Statement

### 1. Shard width bug

`LOCAL_ID_BITS = 24` means a global log ID is split into:

- `local`: 24 bits
- `shard`: 40 bits

The current helper returns `u32` for `log_shard(global_log_id)`. That truncates the high 8 bits of the shard for sufficiently large global IDs.

This is a correctness issue, not just a type-style nit.

### 2. Raw `u64` global IDs hide intent

The code currently passes raw `u64` values for:

- `resume_log_id`
- `next_resume_log_id`
- candidate IDs
- block window to log window conversion
- shard/local composition and decomposition

That makes it easy to:

- use the wrong integer in the wrong place
- repeat bit-twiddling logic
- blur the boundary between global IDs and shard-local IDs

### 3. Candidate execution inflates roaring state too early

The current indexed query path loads shard-local roaring data, then eagerly converts it into `BTreeSet<u64>` of global IDs before set intersection.

That works, but it loses:

- compact roaring representation
- efficient bitmap intersection
- explicit shard locality

This plan includes replacing tree-set candidate execution with shard-aware roaring bitmap execution as part of the implementation, not as a later optional idea.

## Goals

- fix shard typing so it is correct for the current bit layout
- introduce explicit newtypes for global log IDs, shard IDs, and local log IDs
- centralize compose/split logic on the newtypes
- keep public query semantics unchanged
- minimize churn by allowing staged migration
- make roaring-backed shard-aware execution the standard indexed query path

## Non-Goals

- changing on-disk encoding of stored IDs
- changing query pagination semantics
- changing block-keyed log payload storage
- redesigning stream IDs or key layout beyond type-safe helper updates
- changing block-scan fallback semantics except where needed to preserve current behavior for unindexed or broad queries

## Desired End State

### New core ID types

Introduce explicit ID newtypes in a shared module, likely `src/core/ids.rs` or a nearby dedicated module:

- `LogId(u64)`
- `LogShard(u64)`
- `LogLocalId(u32)`

Suggested API shape:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogId(u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogShard(u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogLocalId(u32);

impl LogId {
    pub const fn new(raw: u64) -> Self;
    pub const fn get(self) -> u64;
    pub const fn shard(self) -> LogShard;
    pub const fn local(self) -> LogLocalId;
    pub const fn split(self) -> (LogShard, LogLocalId);
}

impl LogShard {
    pub const fn new(raw: u64) -> Self;
    pub const fn get(self) -> u64;
}

impl LogLocalId {
    pub fn new(raw: u32) -> Result<Self, InvalidLogLocalId>;
    pub const fn new_unchecked(raw: u32) -> Self;
    pub const fn get(self) -> u32;
}

pub const fn compose_log_id(shard: LogShard, local: LogLocalId) -> LogId;
```

`LogLocalId` should enforce the 24-bit bound. The checked constructor should reject values above `MAX_LOCAL_ID`. If an unchecked constructor exists, it should stay private or be tightly scoped to internal helpers that already proved the invariant.

### Public API compatibility

Keep the transport-free public API fields as `u64` initially:

- `QueryLogsRequest.resume_log_id: Option<u64>`
- `QueryPageMeta.next_resume_log_id: Option<u64>`

Inside the service/query implementation, convert to `LogId` at the boundary.

This keeps the first rollout small and avoids forcing immediate downstream changes.

If later desired, the public API can expose `LogId` directly, but that is a separate decision.

### Correct shard width everywhere

All shard helpers should use `LogShard` or `u64`, never `u32`.

That affects:

- `log_shard(...)`
- `compose_global_log_id(...)`
- `local_range_for_shard(...)`
- query-side shard iteration
- stream identifier helpers
- any planned shard-aware in-memory candidate structures

## Proposed Implementation Phases

## Phase 1: Introduce canonical ID newtypes and fix shard width together

### Changes

- extend `src/core/ids.rs` to define:
  - `LogId`
  - `LogShard`
  - `LogLocalId`
  - conversion helpers
- change canonical compose/split helpers to use the newtypes and correct-width shard handling immediately
- widen shard-bearing helpers and call sites in the same patch
- change `PrimaryIdRange` in the same phase to use `LogId` internally
- update low-level compose/split helpers to delegate to the newtypes

### Key design decisions

- `LogId::shard()` and `LogId::local()` become the canonical split operations
- the patch that makes those helpers canonical must also remove the current truncation bug
- `compose_global_log_id(...)` should either:
  - become `compose_log_id(LogShard, LogLocalId) -> LogId`, or
  - stay as a compatibility wrapper over the newtype constructor
- avoid sprinkling raw shifts and masks outside one module
- avoid saturating arithmetic on `LogId` unless a specific invariant-preserving use case requires it and is documented

### Expected impact

- minimal behavioral risk
- immediate readability improvement
- actual bug fix lands in the same patch as the new canonical helpers
- easier review of later correctness changes

### Concrete hotspots in this phase

- `src/core/ids.rs`
- `src/domain/keys.rs`
- `src/logs/query.rs`
- `src/logs/index_spec.rs`
- `src/logs/ingest.rs`
- any tests constructing stream IDs directly

### Compatibility note

This phase widens internal shard-bearing helpers, but it should not affect persisted bytes or public query inputs.

## Phase 2: Migrate remaining internal raw `u64` log IDs to `LogId`

### Changes

Convert internal query and ingest logic to use `LogId` where the value is semantically a global log ID:

- `PrimaryIdRange`
- `MatchedPrimary.id`
- resume handling in `LogsQueryEngine`
- `LogWindowResolver`
- `LogSequencingState.next_log_id`
- `LogBlockWindow.first_log_id`
- directory bucket lookup and sentinel math

### Guidance

Do not force every persisted field to become a newtype immediately if it creates broad serde/codec churn. It is acceptable to:

- keep persisted structs as primitive integers
- add typed projection helpers on top

Example:

- `MetaState.next_log_id` can remain `u64`
- `LogSequencingState.next_log_id` can become `LogId`

That keeps encoded formats stable while improving internal type safety.

## Phase 3: Move indexed execution onto shard-aware roaring bitmaps

### Current shape

Today:

1. read roaring chunk/tail data per stream and per shard
2. expand to global IDs
3. union into `BTreeSet<u64>`
4. intersect tree sets
5. materialize by global ID

### Target shape

Keep candidates shard-aware until materialization:

- represent one clause as `BTreeMap<LogShard, RoaringBitmap>`
- union values inside a clause per shard
- intersect clauses per shard
- iterate shards in ascending order
- iterate locals in ascending order within each shard
- compose `LogId` only at the iterator edge

This phase also needs an explicit policy for the current "no indexed clauses" case. Today an empty clause-set input means "iterate the full `PrimaryIdRange`". The roaring executor must preserve that behavior intentionally or deliberately route those requests onto the block-scan path. It cannot rely only on intersecting present shard bitmaps.

### Suggested internal types

```rust
pub type ShardBitmapSet = BTreeMap<LogShard, RoaringBitmap>;
```

Potential helper surface:

- `union_into_shard_bitmaps(...)`
- `intersect_shard_bitmaps(...)`
- `iter_log_ids(&ShardBitmapSet) -> impl Iterator<Item = LogId>`
- `clip_shard_bitmap_to_range(...)`
- `full_range_shard_bitmaps(...)` or an equivalent explicit representation for the empty-clause-set case

### Executor changes

`execute_candidates(...)` would become shard-aware, for example:

- input: `Vec<ShardBitmapSet>`
- handle the empty-clause-set case explicitly
- perform per-shard intersection
- generate ordered `LogId`s lazily
- stop once `take` matches are collected

`load_clause_sets(...)` and `fetch_union_log_level(...)` should also change in this phase so they return shard-aware roaring structures directly instead of building `BTreeSet` intermediates.

### Why newtypes help here

Without typed `LogShard` and `LogLocalId`, shard-aware execution invites more subtle mistakes:

- using `u32` shard keys
- composing IDs inconsistently
- mixing local shard positions with global IDs

### Expected impact

- removes the current eager expansion into tree sets
- keeps query execution aligned with the roaring representation already used in tails and chunks
- should reduce memory churn and improve intersection efficiency for indexed queries

## File-Level Work Plan

### `crates/finalized-history-query/src/core/ids.rs`

- add `LogId`, `LogShard`, `LogLocalId`
- move compose/split logic here or wrap existing helpers
- add tests for:
  - boundary values
  - roundtrip split/compose
  - shard values above `u32::MAX`

### `crates/finalized-history-query/src/domain/keys.rs`

- replace raw shard helpers with typed wrappers or widen them to `u64`
- ensure stream ID generation accepts `LogShard`
- keep key encoding unchanged

### `crates/finalized-history-query/src/logs/query.rs`

- convert resume and candidate logic to typed IDs
- replace `BTreeSet`-based clause loading with shard-aware roaring structures
- keep broad-query and empty-clause-set behavior explicit and tested

### `crates/finalized-history-query/src/core/execution.rs`

- change `MatchedPrimary.id` to `LogId`
- replace tree-set intersection with shard-aware roaring intersection and ordered iteration

### `crates/finalized-history-query/src/logs/window.rs`

- return typed global-ID ranges if `PrimaryIdRange` is updated

### `crates/finalized-history-query/src/logs/materialize.rs`

- accept `LogId` for lookup APIs
- use `.shard()` / `.local()` where relevant

### `crates/finalized-history-query/src/logs/ingest.rs`

- use typed composition when deriving global IDs and shard-local values for stream fanout

### Tests

Add explicit tests for:

- shard split/compose roundtrip above `u32::MAX`
- pagination around shard boundaries
- stream fanout correctness at shard transitions
- directory bucket lookup correctness for IDs in high shards

## Migration Strategy

Recommended sequence:

1. add newtypes and land the shard-width fix in the canonical helpers and their immediate call sites
2. migrate remaining internal APIs to typed `LogId`
3. move indexed execution and clause loading to shard-aware roaring bitmaps
4. update tests and call sites

This keeps each patch reviewable and reduces the chance of mixing mechanical edits with logic changes.

## Risks

### Type churn

Introducing newtypes can touch many files quickly. Keep the first pass narrow and prefer boundary conversions over all-at-once rewrites.

### Hidden assumptions in tests

Some tests may implicitly rely on `u32` shard values or raw integer APIs. Expect mechanical fallout there.

### Over-scoping

The shard-width fix and the roaring-execution refactor are related but not equally urgent.

Priority should be:

1. fix correctness
2. improve type safety
3. optimize execution shape

Even so, this plan intentionally includes step 3 rather than deferring it, because the current `BTreeSet` execution shape is directly at odds with the persisted roaring index model.

## Verification Plan

At minimum:

- unit tests for the new ID types
- existing finalized-history-query test suite
- targeted tests for shard values that exceed `u32::MAX`

Suggested targeted additions:

- `LogId` roundtrip with shard `0x1_0000_0000`
- `local_range_for_shard` equivalent behavior using widened shard types
- candidate ordering preserved across shard boundaries
- indexed execution returns the same ordered results when multiple clauses intersect across multiple shards
- empty-clause-set execution preserves current semantics after the roaring refactor
- one end-to-end ingest plus indexed-query test with `next_log_id` seeded into shard `0x1_0000_0000`, exercising:
  - stream ID generation
  - ingest stream fanout
  - indexed candidate loading
  - roaring-based clause intersection
  - successful materialization through the normal query path

Tests that can be added immediately against the current code should land first, even if they fail:

- helper-level coverage for split/compose with shard values above `u32::MAX`
- an end-to-end indexed-query regression test showing that a high-shard log can alias to a low-shard result today because stream IDs and recomposed candidate IDs truncate the shard

The remaining tests above depend on the new implementation shape and should land with the relevant feature work:

- `local_range_for_shard` equivalent behavior using widened typed shard helpers
- candidate ordering preserved across shard boundaries after the typed-ID migration
- indexed execution equivalence across multiple clauses and multiple shards after shard-aware roaring execution exists
- empty-clause-set execution semantics after the roaring executor replaces the current tree-set path

Routine repo verification should use:

- `scripts/verify.sh finalized-history-query`

## Open Questions

### Should `PrimaryIdRange` become log-specific?

Right now it is a generic-looking global-ID range. If this crate is currently log-only, renaming or specializing it may improve clarity.

Recommendation: either make `PrimaryIdRange` typed over `LogId` in the first correctness patch or replace it with a log-specific typed range. Leaving the main range type raw undermines the purpose of the refactor.

### Should public API fields expose `LogId` directly?

Recommendation: not in the first pass. Keep external API stable while improving internal correctness first.

### Should shard-aware execution use `BTreeMap<LogShard, RoaringBitmap>` or `RoaringTreemap`?

Recommendation: start with `BTreeMap<LogShard, RoaringBitmap>`.

Reason:

- it matches the current shard-local storage layout
- it keeps first/last shard clipping explicit
- it avoids introducing another bitmap abstraction during the correctness refactor

`RoaringTreemap` can be revisited later if we want a single 64-bit bitmap abstraction.

## Recommendation

Approve and implement Phases 1 through 3 as one coherent refactor sequence: correctness first, then internal typing, then roaring-based execution.
