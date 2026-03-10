# Finalized History Query Shard-Streaming Query Architecture

## Summary

This document proposes a high-level replacement for the current whole-window indexed query path in `crates/finalized-history-query`.

The proposed end state is:

- keep the existing public `query_logs` API and pagination semantics
- keep `log_id` as the primary ordering and resume identity
- stop loading clause sets for the full resolved `log_id` window up front
- execute indexed queries one shard at a time in ascending `log_id` order
- materialize and emit matches as soon as they are discovered
- stop as soon as `limit + 1` matches are found

The goal is to improve first-page latency, bound per-request memory, and make pagination naturally align with the storage and indexing model.

## Problem Statement

The current indexed query path in:

- `crates/finalized-history-query/src/logs/query.rs`
- `crates/finalized-history-query/src/core/execution.rs`

has a whole-window shape:

1. resolve the block window
2. map it to one inclusive `log_id` window
3. load each indexed clause across every shard in that full `log_id` window
4. intersect full-window clause sets
5. iterate candidate `log_id`s globally
6. materialize exact matches until the page is full

That shape is simple, but it front-loads the most shard-sensitive work.

At high ingest rates, broad time windows become large shard spans quickly. For paginated queries, the first page often does not need full-window clause state, but the current architecture still pays that cost before it can return anything.

## Goals

- preserve exact ascending result order
- preserve exact `next_resume_log_id`
- preserve exact `has_more` using `limit + 1`
- reduce first-page latency for paginated indexed queries
- bound in-memory clause/intersection state to one shard at a time
- avoid full-window clause loading when early shards already satisfy the page
- keep broad-query fallback behavior available where it still makes sense

## Non-Goals

- changing the public `QueryLogsRequest` shape
- changing `log_id` pagination identity
- changing block-scan fallback semantics
- redesigning ingest or on-disk stream formats in the first pass
- solving remote-storage latency in this document

## Current Shape

Today the indexed path behaves conceptually like this:

```text
resolved block range
    ->
resolved log_id range
    ->
load clause sets across all overlapping shards
    ->
intersect all shard-local bitmaps
    ->
iterate all candidate ids in order
    ->
materialize exact matches
```

That means the first page depends on the cost of:

- loading every overlapping shard for every clause
- unioning OR values across every overlapping shard
- building the full intersected shard map

before the query can return any result.

## Proposed Shape

The proposed indexed path is shard-streaming.

Conceptually:

```text
resolved block range
    ->
resolved log_id range
    ->
for each overlapping shard in ascending order:
    load only this shard's clause bitmaps
    union OR values inside this shard
    intersect clauses inside this shard
    iterate local candidates in order
    materialize exact matches immediately
    stop once page has limit + 1 matches
```

This shifts the unit of work from:

- one full `log_id` window

to:

- one shard-local `log_id` slice at a time

## Visual Shape

### Current whole-window indexed path

```text
query window:
[shard 0][shard 1][shard 2][shard 3] ... [shard N]

clause load:
[all shards for address]
[all shards for topic0]
[all shards for topic1]

then:
intersect everything
then:
iterate/materialize
then:
return page
```

### Proposed shard-streaming indexed path

```text
query window:
[shard 0][shard 1][shard 2][shard 3] ... [shard N]

execute:
shard 0 -> load/intersect/materialize -> emit
shard 1 -> load/intersect/materialize -> emit
shard 2 -> load/intersect/materialize -> emit
...
stop as soon as page is satisfied
```

### Multi-clause query inside one shard

```text
shard K

address=A : [..1....1..1.....]
topic0=T0 : [....1..1.1......]
topic1=T1 : [..1......1......]

intersection:
            [..1.............]

materialize only these surviving locals
```

### OR list inside one shard

```text
shard K

A1   -> [..1.............]
A2   -> [.....1..........]
A3   -> [.......1........]
...
A256 -> [.............1..]

union inside shard:
       [..1..1.1.....1..]

then intersect with other clauses for the same shard
```

## Architecture Overview

The proposed indexed executor is composed of four high-level stages.

### 1. Window resolution

Keep the current steps:

- resolve finalized block range
- map block range to inclusive `log_id` range
- validate `resume_log_id`
- compute effective limit and `take = limit + 1`

This preserves the current public semantics and metadata rules.

### 2. Shard plan

Instead of loading full-window clause sets, derive a lightweight shard plan:

- first overlapping shard
- last overlapping shard
- local range for each shard
- clause order for the query

This stage should be cheap and should not load roaring data yet.

### 3. Per-shard clause execution

For each shard in ascending order:

1. compute shard-local `[from_local, to_local]`
2. for each clause in planned order:
   - load only this shard's stream data
   - union OR values for this shard only
   - intersect with the shard accumulator
   - short-circuit if the shard becomes empty
3. if the shard has survivors:
   - iterate local IDs in order
   - compose global `log_id`
   - materialize exact matches immediately

This stage is the core execution change.

### 4. Page assembly

Collect at most `take = effective_limit + 1` matches globally across shards.

Then build page metadata exactly as today:

- `has_more = matched.len() > effective_limit`
- `next_resume_log_id = last returned log id` only when `has_more`
- `cursor_block = last returned block` or resolved endpoint when empty

This keeps pagination behavior unchanged.

## Query Semantics

The proposal keeps these current invariants:

- ascending `log_id` order
- exact block-range clipping against finalized head
- `resume_log_id` remains a declarative lower bound
- `next_resume_log_id` remains the last returned `log_id`
- `has_more` remains exact by reading one extra match

The key implementation rule is:

- do not pre-count all shard results

Instead:

- stop at `limit + 1`

That preserves exact pagination without turning page construction into a full count.

## Why This Helps

### First-page latency

If the first few shards already contain enough matches, the query can return without touching the rest of the resolved window.

### Bounded memory

The executor only needs:

- one shard's clause bitmaps
- one shard accumulator
- the output page buffer

instead of a full-window `Vec<ShardBitmapSet>`.

### Better pagination behavior

Pagination becomes a natural continuation of ascending shard traversal:

- page 1 walks early shards
- later pages resume from the last returned `log_id`
- the executor skips all earlier shard-local state naturally

### Better alignment with actual hot costs

The current benchmarks show that shard fanout and OR width are major drivers of cost. A shard-streaming executor lets those costs grow incrementally with actual page discovery rather than paying the full resolved-window cost up front.

## What This Does Not Solve

Shard streaming does not make every broad query cheap.

It still has to pay for:

- many shards when the query window is broad
- wide OR unions inside each touched shard
- sparse queries that require traversing many shards before enough matches appear

It improves:

- first-page latency
- memory shape
- early-stop behavior

It does not eliminate:

- the fundamental cost of very broad, very selective queries

## Execution Model Details

### Clause ordering

The current clause-order planner can still be used, but the best ordering criterion may change slightly.

Today the planner optimizes mostly for estimated clause cardinality. In a shard-streaming design, it may also be valuable to favor clauses that collapse shard-local accumulators early.

High-level rule:

- prefer the earliest clause that is expected to make a shard empty quickly

### OR handling

OR lists should stay shard-local.

Do not build a full-window union for a clause before streaming. Instead:

- load OR values for the current shard only
- union them into one shard-local bitmap
- discard that union before moving to the next shard

### Exact-match materialization

Materialization should remain pointwise and exact:

- compose one global `log_id`
- call the existing log materializer
- recheck exact match
- append to output if matched

No semantic change is needed here.

### `has_more`

Use the same current rule:

- gather `limit + 1`

Do not compute full counts per shard or full counts for the whole window.

## Interaction With Block Scan

Broad-query block scan can remain as a separate path.

The proposed change applies to the indexed path only.

Decision boundary remains:

- if the query should force block scan, keep doing that
- otherwise use shard-streaming indexed execution

This keeps the architecture incremental and reduces migration risk.

## Interaction With Shard Size

This proposal improves execution behavior even if the shard/local bit split stays unchanged.

However, it does not remove the architectural importance of shard size.

If one shard covers only a few minutes of ingest at target throughput, then:

- one-hour queries still span many shards
- one-day queries still span hundreds of shards

So shard streaming and shard-size changes are complementary:

- shard streaming improves the per-request execution model
- larger local-bit width would reduce the number of shards that any broad query touches

## Implementation Sketch

High-level pseudocode:

```text
resolve block range
resolve log_id window
validate resume_log_id
compute take = limit + 1
build clause order

matched = []

for shard in overlapping_shards(log_window):
    local_range = local_range_for_shard(log_window, shard)

    shard_accumulator = None

    for clause in clause_order:
        shard_clause = load_clause_for_one_shard(clause, shard, local_range)
        if shard_clause is empty:
            shard_accumulator = empty
            break

        shard_accumulator =
            if shard_accumulator is None:
                shard_clause
            else:
                shard_accumulator AND shard_clause

        if shard_accumulator is empty:
            break

    for local_id in shard_accumulator:
        global_id = compose(shard, local_id)
        item = materialize(global_id)
        if exact_match(item):
            matched.push(item)
            if matched.len == take:
                break outer

assemble page from matched
```

## Migration Strategy

### Phase 1: Internal executor split

Refactor the indexed path into separable stages:

- window resolution
- clause ordering
- per-shard clause loading
- page assembly

Goal:

- make the current code structurally ready for shard streaming

### Phase 2: Add one-shard clause loader

Introduce a narrow internal helper that can load:

- one clause
- one shard
- one local range

without requiring full-window `ShardBitmapSet` construction.

### Phase 3: Add shard-streaming executor

Implement the streaming path behind the current indexed-query decision boundary.

### Phase 4: Verify semantics

Add tests that lock down:

- exact ordering
- exact pagination
- empty-page metadata
- resume behavior at shard boundaries
- equality against the current executor on representative workloads

### Phase 5: Benchmark and tune

Use the current benchmark suite to compare:

- whole-window executor
- shard-streaming executor

with emphasis on:

- pagination-heavy queries
- wide OR queries
- large shard spans
- sparse vs dense candidates

## Risks

### More storage calls for full traversals

If a query truly needs many shards, shard streaming may issue smaller repeated loads rather than one larger full-window load.

That is acceptable if:

- first-page latency improves
- memory shape improves
- storage adapters cache effectively

### More complex control flow

The current executor is conceptually simpler because it centralizes clause loading before execution. Shard streaming adds a more stateful loop.

### Planner quality matters more

Because execution is incremental, poor clause ordering can waste work inside every shard.

## Recommendation

Move the indexed path toward shard-streaming execution.

The strongest reasons are:

- first-page latency should depend on how quickly the page can be satisfied, not on the full resolved shard span
- pagination becomes a natural property of the executor
- the memory shape becomes bounded and easier to reason about
- the approach aligns with the actual structure of the roaring indexes and the materializer

This should be treated as:

- an execution-model improvement first
- a shard-size redesign second

Both may still be worth doing, but shard-streaming is the more direct response to the observed query-shape problem.
