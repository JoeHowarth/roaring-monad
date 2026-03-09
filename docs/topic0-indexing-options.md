# Topic0 Indexing Options

## Purpose

This note compares the cost and semantics of three `topic0` indexing choices:

1. `topic0_block` only
2. adaptive `topic0_log` plus always-on `topic0_block`
3. always-on `topic0_log` plus always-on `topic0_block`

It also outlines a concrete implementation plan for moving to always-on `topic0_log`.

## Current Decision

The current implementation keeps both `topic0_block` and `topic0_log`, and `topic0_log` is always indexed.

That means:

- topic0 queries have a stable exact log-level stream
- the planner can treat `topic0_log` like the other exact log-level clauses
- the executor can skip `topic0_block` prefilter work when `Topic0Log` is already part of the plan

## Cost Framing

- `topic0_block` is always indexed once per `(topic0, block)` when a signature appears in a block.
- `topic0_log` is always indexed once per matching log.
- the main cost tradeoff is no longer correctness complexity; it is write and storage amplification for hot signatures.

## Cost Model

Assumptions:

- block time: `400 ms` (`2.5 blocks/sec`)
- blocks/year: `2.5 * 60 * 60 * 24 * 30 * 12 = 77,760,000`
- default chunk target: `1950` entries and `32 KiB`
- manifest `ChunkRef` size: `20` bytes
- local ID layout: `24` local bits, so one full shard holds `16,777,216` entries

Derived formulas for always-on `topic0_log` for one signature:

- `entries/year = logs_per_block * 77,760,000`
- `chunks/year ~= entries/year / 1950`
- `manifest_bytes/year ~= chunks/year * 20`
- `chunk_blob_target_bytes/year ~= chunks/year * 32 KiB`
- `shards/year ~= entries/year / 16,777,216`

## Worst-Case Cost Table

The table below is for one hot `topic0` signature that appears in every block.

| Matching logs per block | `topic0_log` entries/year | Chunks/year | Manifest refs/year | Chunk blob target/year | Shards/year |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 77.76M | 39.9k | 0.80 MB | 1.31 GB | 4.6 |
| 10 | 777.60M | 398.8k | 7.98 MB | 13.07 GB | 46.3 |
| 100 | 7.776B | 3.99M | 79.75 MB | 130.69 GB | 463.5 |
| 1000 | 77.760B | 39.88M | 797.54 MB | 1.31 TB | 4634.9 |

Notes:

- The chunk-blob column uses the configured `32 KiB` target as a rough upper-budget estimate, not a guaranteed exact byte count.
- The main storage cost of always-on `topic0_log` is chunk blobs, not manifests.
- The `24`-bit local layout keeps per-shard manifests reasonable even for hot signatures. One completely full shard is about `8604` chunks and about `172 KB` of manifest refs.

## Comparison With `topic0_block`

`topic0_block` stores one entry per block, not one entry per matching log.

For a signature that appears in every block:

- `topic0_block` entries/year: `77.76M`
- `topic0_block` chunks/year: `39.9k`
- `topic0_block` manifest refs/year: `0.80 MB`
- `topic0_block` chunk blob target/year: `1.31 GB`

So the multiplier from `topic0_block` to always-on `topic0_log` is approximately the number of matching logs per matching block:

- `1` log/block: same order of magnitude
- `10` logs/block: about `10x`
- `100` logs/block: about `100x`
- `1000` logs/block: about `1000x`

## Choice Summary

### `topic0_block` only

Pros:

- simple ingest semantics
- low storage cost
- no historical coverage ambiguity

Cons:

- weaker topic0 selectivity
- more exact log materialization for topic0-heavy queries

### adaptive `topic0_log`

Pros:

- can reduce storage for hot signatures that are not worth exact indexing

Cons:

- hard to reason about historical coverage
- planner and executor need coverage-aware logic
- easy to make `topic0_log` behave like a partial exact index

### always-on `topic0_log`

Pros:

- simplest exact-query semantics
- `topic0_log` is always safe to use as a normal clause
- planner and executor become easier to reason about

Cons:

- storage and ingest amplification can be very large for hot signatures
- worst-case cost scales linearly with matching logs per block

## Recommendation Framing

Always-on `topic0_log` is attractive if:

- exact topic0 filtering is important for many queries
- the hottest signatures do not reach extreme `100+ logs/block` regimes often
- correctness and planner simplicity are worth extra write and storage cost

Always-on `topic0_log` is unattractive if:

- one or more signatures are expected to behave like chain-wide `Transfer`-style firehoses
- storage budget is tight
- the system mostly benefits from block-level prefiltering rather than exact log-level topic0 indexing

## Follow-Up Options

### 1. Revisit whether `topic0_block` should remain

After unconditional `topic0_log` is working:

- measure whether `topic0_block` still improves any important workloads
- if not, consider removing `topic0_block` as a second cleanup step

This should be a separate decision. Keeping it at first reduces migration risk and preserves optional block-level planning heuristics.

## Testing Plan

### Correctness

- add ingest tests proving `topic0_log` is written for all seen `topic0` values
- add query tests proving `topic0` clauses return correct results across the entire range without any adaptive-mode assumptions

### Cleanup

- add planner tests showing `Topic0Log` is used like any other exact clause
- add executor tests showing `topic0_block` is skipped when `Topic0Log` is already applied

### Performance

- benchmark ingest before and after unconditional `topic0_log`
- measure:
  - blocks/sec
  - logs/sec
  - Scylla write volume
  - blob bytes written
  - per-signature manifest growth

## Suggested Next Measurements

1. Measure ingest throughput with a workload that has hot `topic0` signatures at `1`, `10`, and `100` matching logs per block.
2. Measure per-signature manifest and chunk growth over fixed block windows.
3. Compare query latency for topic0-heavy workloads before deciding whether `topic0_block` is still worth keeping.
