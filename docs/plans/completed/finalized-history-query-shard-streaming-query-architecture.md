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

This document is intentionally about executor shape, not cache hierarchy.

Unless stated otherwise, the indexed-path discussion in this document applies to queries that remain within the current OR guardrails. Queries that exceed `planner_max_or_terms` still follow the existing error-or-block-scan policy.

Cross-request metadata caching is a separate architectural concern and is covered only as an assumption and dependency here. A separate plan exists at:

- `docs/plans/metadata-caching-architecture.md`

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

However, the expected ingest regime is materially lower than the previously discussed worst case.

For planning purposes, this document now assumes the more typical operating point is closer to:

- `1,000` transactions per second
- `1` log per transaction
- `1,000` logs per second

With the current `24` local-bit width:

- `1` shard = `16,777,216` logs
- `1` shard fills in about `4.66` hours at `1,000` logs per second
- `1` day spans about `5.15` shards
- `1` year spans about `1,880` shards

This is much less extreme than the `100,000 logs/s` worst-case thought experiment. That changes the shard-size discussion, but it does not remove the case for improving the execution model.

## Goals

- preserve exact ascending result order
- preserve exact `next_resume_log_id`
- preserve exact `has_more` using `limit + 1`
- reduce first-page latency for paginated indexed queries
- bound in-memory clause/intersection state to one shard at a time
- avoid full-window clause loading when early shards already satisfy the page
- keep broad-query fallback behavior available where it still makes sense
- make it explicit where bounded-work partial pages would fit if introduced later

## Non-Goals

- changing the public `QueryLogsRequest` shape
- changing `log_id` pagination identity
- changing block-scan fallback semantics
- redesigning ingest or on-disk stream formats in the first pass
- redesigning manifest storage in the first pass
- solving remote-storage latency in this document
- specifying a cross-request metadata cache hierarchy in this document

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

The default semantic model in this document remains:

- exact pagination
- exact `has_more`
- exact `next_resume_log_id`

That means the executor still keeps searching until it either:

- finds `limit + 1` matches, or
- exhausts the resolved query window

This document also identifies where an optional bounded-work mode could be introduced later, but it does not make that mode part of the base proposal.

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

Instead of loading full-window clause sets, derive a lightweight traversal plan:

- first overlapping shard
- last overlapping shard
- local range for each shard

This stage should be cheap and should not load roaring data yet.

It should only answer:

- which shards must be visited
- what local bounds apply inside each shard

Clause planning belongs to shard execution, not this stage.

### 3. Per-shard clause execution

For each shard in ascending order:

1. compute shard-local `[from_local, to_local]`
2. build a shard-local clause plan:
   - load only this shard's manifest metadata for each active clause stream
   - allow bounded intra-shard concurrency while loading that metadata
   - derive a cheap shard-local estimate from the loaded manifest metadata
   - choose clause order for this shard
3. for each clause in shard-local planned order:
   - reuse the already loaded shard manifest metadata for that clause
   - allow bounded intra-shard concurrency while loading that clause
   - load tail data only when actually executing the clause
   - union OR values for this shard only
   - intersect with the shard accumulator
   - short-circuit if the shard becomes empty
4. if the shard has survivors:
   - iterate local IDs in order
   - compose global `log_id`
   - materialize exact matches immediately

This stage is the core execution change.

Concurrency rule:

- shard traversal is serial in result order
- clause execution inside a shard is serial by default so clause ordering can short-circuit work
- stream loads within one clause for one shard may run with bounded concurrency
- shard-local manifest metadata should be reused by execution rather than fetched twice

### 4. Page assembly

Collect at most `take = effective_limit + 1` matches globally across shards.

Then build page metadata exactly as today:

- `has_more = matched.len() > effective_limit`
- `next_resume_log_id = last returned log id` only when `has_more`
- `cursor_block = last returned block` or resolved endpoint when empty

This keeps pagination behavior unchanged.

## Optional Bounded-Work Mode

The base proposal preserves current exact-page behavior.

That means a sparse query may still traverse many shards before returning the first page.

If the system later needs stronger latency bounds, shard streaming provides a natural place to add an explicit bounded-work mode. That mode could stop after one or more execution budgets are exhausted, for example:

- maximum shards touched
- maximum clause loads
- maximum candidate IDs examined
- maximum wall-clock time

In that mode, the executor could intentionally return fewer than `limit` items even when more matches may exist later in the resolved window.

This document does not choose those semantics yet. It only identifies the boundary:

- base mode: exact current behavior
- future bounded-work mode: explicit partial-page semantics

Any future bounded-work mode must define:

- whether `has_more` means "known more items exist" or "query was cut short"
- whether a new metadata bit is needed to distinguish "partial by budget" from "naturally exhausted"
- whether the existing resume token remains sufficient

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

### Clause-free queries

The clause-free path is part of the required semantics.

Today `LogFilter::default()` is valid and behaves as a sequential traversal of the resolved `log_id` window. The shard-streaming design must preserve that behavior explicitly.

If `clause_order` is empty:

- do not force block scan merely because there are no indexed clauses
- do not require a shard accumulator built from indexed clause bitmaps
- instead traverse the resolved `log_id` window directly in ascending shard/local order
- materialize exact matches immediately
- stop at `limit + 1`

Conceptually, the clause-free path becomes:

```text
for shard in overlapping_shards(log_window):
    local_range = local_range_for_shard(log_window, shard)
    for local_id in local_range:
        global_id = compose(shard, local_id)
        materialize and exact-match
        stop at limit + 1
```

This keeps ordinary pagination behavior unchanged for empty filters.

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

For indexed queries that remain within the current OR guardrails, the current benchmarks show that shard fanout and OR width are major drivers of cost. A shard-streaming executor lets those costs grow incrementally with actual page discovery rather than paying the full resolved-window cost up front.

### Better fit for the expected ingest regime

At the expected `1,000 logs/s` regime, the current `24` local-bit layout yields:

- about `5.15` shards per day
- about `36` shards per week
- about `155` shards per 30-day month

That means the system is not under immediate pressure to enlarge shard-local space just to make day-scale queries feasible. The bigger opportunity is improving how indexed queries traverse those shards.

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

- repeated metadata decode/load churn by itself

Shard streaming improves the execution order, but it will not realize its full benefit if each shard step repeatedly reloads or redecodes the same manifests, tails, chunks, and headers without effective caching.

## Execution Model Details

### Clause ordering

Clause ordering remains useful, but it should become shard-local.

The current global planner optimizes mostly for estimated clause cardinality, and it does so by scanning manifests and tails across the full resolved window before execution begins.

That is not the right steady-state shape for shard streaming.

Instead, for each shard:

- load manifest metadata for the active clause streams in that shard
- derive a cheap estimate from those already-needed reads
- order clauses for that shard
- reuse the loaded manifest metadata during execution

This avoids a separate full-window planning pass and aligns planning cost with the shard currently being executed.

High-level rule:

- prefer the earliest clause that is expected to make a shard empty quickly

The executor should not parallelize all clauses in a shard by default.

Reason:

- clause ordering only helps if later clauses are skipped when earlier clauses empty the shard accumulator
- eager clause-parallel loading would reintroduce avoidable front-loaded work

So the base design is:

- serial across clauses
- short-circuit when the accumulator becomes empty
- bounded concurrency only within the current clause's shard-local loads

Examples of shard-local planning signals:

- zero-overlap stream metadata, because it can kill the shard immediately
- smaller overlapping sealed count from manifest chunk refs

Planning intentionally ignores tail cardinality in the base design.

Reason:

- tails are bounded by seal policy and concentrated near the tip
- historical query cost is dominated by sealed chunks, not tails
- decoding tails during planning adds work without materially improving ordering for most shards

So the intended planning heuristic is:

- use manifest-derived sealed overlap only
- treat tail overlap as an execution concern, not a planning concern

This still front-loads manifest reads for the active clause streams in the current shard.

That is an accepted trade in the base design:

- those manifest reads are needed anyway to execute indexed clauses in the shard
- avoiding tail decode/count work keeps the planning step small and predictable
- short-circuiting still saves chunk loads, unions, intersections, and materialization for later clauses

### OR handling

OR lists should stay shard-local.

Do not build a full-window union for a clause before streaming. Instead:

- load OR values for the current shard only
- allow those value loads to run concurrently up to a bounded per-clause limit
- union them into one shard-local bitmap
- discard that union before moving to the next shard

This preserves the important throughput property of the current loader:

- wide OR clauses do not have to serialize every value load one by one

At the same time, it avoids drifting back to the whole-window model:

- do not speculate across many future shards in parallel in the base design

The intended concurrency boundary is therefore:

- parallel within one clause for one shard
- not parallel across many shards
- not parallel across all clauses in one shard by default

### Metadata reuse assumptions

The executor should be designed so it can benefit from both:

- per-request reuse
- cross-request shared caches

This document does not define those caches in detail, but the execution model should assume the following objects are strong cache candidates:

- stream manifests
- stream tails
- decoded chunk bitmaps
- block log headers
- log directory buckets

Without that reuse, shard streaming could improve early-stop behavior while still paying too much repeated metadata overhead on expensive traversals.

Within a shard, metadata reuse is mandatory rather than optional:

- if planning loads manifest metadata for a clause stream, execution should consume that prepared result
- do not fetch shard manifest metadata once to choose order and then again to run the clause

Tail reads are different:

- planning does not need decoded tails in the base design
- execution still loads tails when evaluating a clause so results stay exact

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

This proposal does not broaden indexed support for super-wide OR queries. If a query currently exceeds `planner_max_or_terms`, it should keep following the existing policy:

- `BroadQueryPolicy::Error` returns `QueryTooBroad`
- `BroadQueryPolicy::BlockScan` falls back to block scan

So the intended benefit surface here is:

- indexed queries within the current OR guardrails
- especially multi-shard windows and moderate OR widths

It is not:

- removing the current wide-OR guardrail
- making arbitrarily wide OR queries part of the indexed path

Decision boundary remains:

- if the query should force block scan, keep doing that
- otherwise use shard-streaming indexed execution

This keeps the architecture incremental and reduces migration risk.

## Interaction With Shard Size

This proposal improves execution behavior even if the shard/local bit split stays unchanged.

However, it does not remove the architectural importance of shard size.

Under the lower expected ingest regime, the current `24` local-bit split is more defensible than it looked in the worst-case throughput thought experiment.

At `1,000 logs/s`:

- `24` bits gives about `4.66` hours per shard
- `1` day is about `5.15` shards
- `1` year is about `1,880` shards

That is not free, but it is materially better than the worst-case regime where one shard represented only a few minutes of data.

At the same time, increasing local-bit width has a real metadata cost with the current manifest design.

Given the current defaults:

- target chunk size is about `1,950` entries
- each manifest chunk ref costs `20` bytes
- manifests are rewritten as a single object on each seal

That implies maximum hot-stream manifest sizes roughly like:

- `24` bits: about `172 KB`
- `28` bits: about `2.75 MB`
- `32` bits: about `44 MB`

So shard streaming and shard-size changes are not symmetric decisions.

If one shard covers only a few minutes of ingest at target throughput, then:

- one-hour queries still span many shards
- one-day queries still span hundreds of shards

Under the expected ingest regime, this document therefore recommends:

- treat `24` bits as the working assumption for now
- improve the executor first
- reconsider larger local-bit widths only after measuring realistic workloads and only with explicit acknowledgement of manifest-growth costs

So shard streaming and shard-size changes are still complementary, but they are not equally urgent:

- shard streaming improves the per-request execution model
- larger local-bit width would reduce the number of shards that any broad query touches

In practice:

- shard streaming is in scope for this plan
- shard-size redesign is deferred until there is evidence that `24` bits is insufficient at expected ingest or until manifest storage is redesigned

## Implementation Sketch

High-level pseudocode:

```text
resolve block range
resolve log_id window
validate resume_log_id
compute take = limit + 1

matched = []

if filter has no active indexed clauses:
    for shard in overlapping_shards(log_window):
        local_range = local_range_for_shard(log_window, shard)
        for local_id in local_range:
            global_id = compose(shard, local_id)
            item = materialize(global_id)
            if exact_match(item):
                matched.push(item)
                if matched.len == take:
                    break outer
    assemble page from matched
    return

for shard in overlapping_shards(log_window):
    local_range = local_range_for_shard(log_window, shard)
    shard_plan = plan_one_shard_from_manifests(filter, shard, local_range)

    if shard_plan.clauses is empty:
        continue

    shard_accumulator = None

    for clause in shard_plan.clauses:
        shard_clause = execute_prepared_clause_with_tail(clause)
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
- shard traversal planning
- clause-free sequential traversal
- shard-local clause planning
- per-shard clause loading
- page assembly

Goal:

- make the current code structurally ready for shard streaming

### Phase 2: Add one-shard planner/loader

Introduce a narrow internal helper that can prepare:

- one shard
- one local range
- the active clause streams for that shard
- cheap shard-local estimates derived from manifest metadata

without requiring full-window `ShardBitmapSet` construction.

That helper should return reusable prepared clause state, not just an ordering decision.

### Phase 3: Add shard-streaming executor and explicit clause-free path

Implement the streaming path behind the current indexed-query decision boundary.

That phase must preserve:

- the empty-filter sequential traversal behavior
- exact pagination semantics for that path

### Phase 4: Remove or bypass the current full-window planner from the streaming path

The shard-streaming path should not depend on a separate full-window `build_clause_order(...)` prepass.

If the old planner remains in the codebase temporarily, it should stay off the shard-streaming critical path.

### Phase 5: Verify semantics

Add tests that lock down:

- exact ordering
- exact pagination
- empty-page metadata
- resume behavior at shard boundaries
- empty-filter pagination behavior
- equality against the current executor on representative workloads

### Phase 6: Benchmark and tune

Use the current benchmark suite to compare:

- whole-window executor
- shard-streaming executor

with emphasis on:

- pagination-heavy queries
- wide OR queries
- large shard spans
- sparse vs dense candidates
- expected-ingest shard spans, not only extreme worst-case shard spans
- shard-local planning overhead
- manifest-metadata reuse effectiveness between shard planning and execution
- impact of excluding tails from planning heuristics

### Phase 7: Re-evaluate shard sizing separately

Only after the shard-streaming executor is benchmarked under expected workloads should the project decide whether to revisit local-bit width.

That later evaluation should include both:

- query execution cost
- manifest and metadata growth cost

It should not be folded implicitly into this executor plan.

## Risks

### More storage calls for full traversals

If a query truly needs many shards, shard streaming may issue smaller repeated loads rather than one larger full-window load.

That is acceptable if:

- first-page latency improves
- memory shape improves
- storage adapters cache effectively

### Metadata churn can erase the win

If each shard step repeatedly loads or decodes the same metadata, then shard streaming may improve easy first pages while still regressing expensive traversals.

The architecture therefore depends on a coherent metadata-reuse story across:

- manifests
- tails
- chunk blobs or decoded chunk bitmaps
- block headers
- directory buckets

That reuse may exist partly within a request and partly across requests, but it cannot be treated as incidental.

### More complex control flow

The current executor is conceptually simpler because it centralizes clause loading before execution. Shard streaming adds a more stateful loop.

### Bad concurrency boundaries can erase the win

If the implementation makes shard-local OR and stream loads fully serial, wide-OR queries may regress relative to the current loader.

If the implementation speculatively loads many future shards or all clauses in parallel, it drifts back toward the current front-loaded whole-window cost shape.

So the architecture depends on using the right concurrency boundary:

- bounded concurrency within a clause for one shard
- ordered traversal across shards
- clause ordering preserved for short-circuiting

### Planner quality matters more

Because execution is incremental, poor shard-local clause ordering can waste work inside every shard.

### Planning/execution reuse must be real

If shard-local planning loads manifests and execution then reloads them, the design loses much of its benefit.

The implementation therefore needs a prepared-clause representation that can carry:

- decoded manifest or equivalent overlap summary
- overlapping chunk refs for the shard
- shard-local estimate used for ordering

The base design does not require tails to be part of that prepared-planning state.

That is intentional:

- tail contribution is bounded by seal policy
- tail relevance is concentrated near the tip
- correctness still comes from loading tails during execution

### Exact-page semantics can still allow long first pages

Because the base proposal preserves exact `limit + 1` semantics, sparse queries may still traverse large shard spans before returning the first page.

That is acceptable for the base design, but it means bounded-work partial-page behavior may still be needed later as a separate semantic choice.

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
- a metadata-caching architecture as a separate parallel design track

All three may still be worth doing, but shard-streaming is the most direct response to the current query-shape problem and does not require an immediate shard-size change.
