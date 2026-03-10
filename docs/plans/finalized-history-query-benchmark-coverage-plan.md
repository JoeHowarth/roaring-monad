# Finalized History Query Benchmark Coverage Plan

## Summary

This plan adds only the benchmark coverage that is clearly worth maintaining for `crates/finalized-history-query`.

The immediate goal is to catch regressions like:

- replacing shard-local roaring operations with eager `BTreeSet<u64>` expansion
- adding avoidable per-shard union or intersection overhead
- turning `log_id` materialization into repeated decode / lookup churn

The reduced end state is a readable multi-file benchmark suite focused on:

- candidate execution
- clause loading
- materialization
- end-to-end query behavior

## Problem Statement

The current benchmark coverage in:

- `crates/finalized-history-query/benches/finalized_history_query_bench.rs`

is too coarse to reliably catch subcomponent regressions.

The most important current gaps are:

1. the indexed candidate executor is not benchmarked directly
2. clause loading and shard fanout are not benchmarked directly
3. materialization and cache behavior are not benchmarked directly
4. the broad query benches do not make shard-heavy regressions obvious

That means a performance regression can land in an important internal path while the existing broad query benchmarks stay noisy or inconclusive.

## Goals

- split benchmark coverage into multiple bench files for readability
- benchmark the highest-value hot subcomponents directly
- add shard-heavy and high-shard-ID workloads where they matter
- keep one end-to-end query layer as an integration backstop
- keep the suite small enough that contributors will actually run it

## Non-Goals

- replacing the existing `benchmarking` crate for distributed or production-shape runs
- benchmarking every subsystem in the crate
- introducing CI gating on absolute timings in the first pass
- adding separate codec, block-scan, ingest-fanout, or stream-writer microbenches unless later profiling shows they matter

## Desired Bench Layout

Replace the current single-file bench organization with:

- `crates/finalized-history-query/benches/common.rs`
- `crates/finalized-history-query/benches/execution_bench.rs`
- `crates/finalized-history-query/benches/query_clause_loading_bench.rs`
- `crates/finalized-history-query/benches/materialize_bench.rs`
- `crates/finalized-history-query/benches/query_end_to_end_bench.rs`

The point is not to exhaustively benchmark everything. The point is to isolate the internal query-path components that are both performance-sensitive and easy to regress.

## Shared Benchmark Support

Add a small shared helper module in:

- `crates/finalized-history-query/benches/common.rs`

This module should own:

- deterministic block/log builders
- reusable service setup
- reusable in-memory stores
- shard-aware synthetic bitmap builders
- helpers for seeding manifests, tails, chunks, headers, and block blobs
- canonical workload presets

Important principle:

- each bench file should read as benchmark intent, not fixture plumbing

## Benchmark Categories

### 1. Candidate execution

Target file:

- `crates/finalized-history-query/benches/execution_bench.rs`

This is the highest-priority addition because it is where the roaring-vs-`BTreeSet` regression lives.

Benchmark:

- single-clause ordered iteration over one shard
- single-clause ordered iteration over many shards
- multi-clause intersection with sparse candidates
- multi-clause intersection with dense candidates
- OR-union of many shard-local bitmaps before intersection
- empty-clause-set execution over a range
- clipping candidate sets at first shard, last shard, and both edges
- high-shard-ID iteration with shard values above `u32::MAX`

Regression this should catch:

- eager conversion of roaring data into tree sets
- repeated recomposition of global IDs too early
- inefficient full-range handling when no indexed clauses exist

### 2. Query clause loading

Target file:

- `crates/finalized-history-query/benches/query_clause_loading_bench.rs`

Benchmark:

- manifest-only streams
- tail-only streams
- manifest plus tail streams
- one value across one shard
- one value across many shards
- OR-list unions with widths `1`, `4`, `16`, `64`, `256`
- sparse chunks vs dense chunks
- narrow local range vs full-shard local range
- high-shard stream loading

Hot code to exercise:

- `load_clause_sets(...)`
- `fetch_union_log_level(...)`
- `load_stream_entries(...)`
- `overlapping_chunk_refs(...)`

Regression this should catch:

- too much work per shard
- loading or merging too many chunks for narrow windows
- OR-list costs growing worse than expected

### 3. Materialization

Target file:

- `crates/finalized-history-query/benches/materialize_bench.rs`

Benchmark:

- `log_id -> directory bucket -> block_num -> local ordinal`
- warm directory-bucket cache
- cold directory-bucket cache
- warm block-header cache
- cold block-header cache
- repeated materialization within one block
- repeated materialization across many blocks
- blocks spanning multiple directory buckets
- high-shard log IDs

Hot code to exercise:

- `resolve_log_id(...)`
- `load_block_header(...)`
- `load_by_id(...)`

Regression this should catch:

- extra bucket decoding
- poor cache locality
- hidden ordinal computation overhead
- unnecessary work in point materialization

### 4. End-to-end query workloads

Target file:

- `crates/finalized-history-query/benches/query_end_to_end_bench.rs`

This replaces the current mixed single-file shape with something easier to read and extend.

Benchmark groups:

- narrow indexed queries
- multi-clause indexed intersections
- wide OR queries still using indexed execution
- pagination-heavy queries
- shard-boundary queries
- high-shard-ID queries

Regression this should catch:

- broad user-visible latency changes
- mismatches between microbench wins and actual query behavior

## Workload Matrix

The suite as a whole should cover these axes, but every bench does not need every shape.

### Shard shape

- `1` shard
- `8` shards
- `64` shards
- high shard `0x1_0000_0000`

### Candidate density

- sparse
- medium
- dense

### OR width

- `1`
- `4`
- `16`
- `64`
- `256`

### Cache shape

- cold cache
- warm cache
- same-block locality
- cross-block access

## Benchmark Data Strategy

Use synthetic deterministic data for Criterion benches.

Reasons:

- stable and cheap to generate
- reproducible locally
- easy to tune density and shard count
- avoids remote backend noise

Keep production-shape and distributed runs in the existing `benchmarking` crate and scripts.

## Concrete File Changes

### `crates/finalized-history-query/Cargo.toml`

Change the bench declarations from one bench target to one target per file.

Expected shape:

- one `[[bench]]` entry per bench file
- all with `harness = false`

### `crates/finalized-history-query/benches/common.rs`

Add:

- fixture builders
- shared workload presets
- deterministic bitmap / stream seed helpers
- small helper materializer/store stubs for execution-only benches

### Existing bench file

Either:

1. delete `finalized_history_query_bench.rs` after the split, or
2. keep it briefly as a transition wrapper

Recommendation:

- remove it after the split to avoid duplicate coverage and confusion

## Implementation Phases

## Phase 1: Split the current bench file and add shared helpers

Changes:

- add `benches/common.rs`
- move the current end-to-end query benches into `query_end_to_end_bench.rs`
- update `Cargo.toml` bench targets

Acceptance:

- the benchmark layout is readable
- existing integration-level coverage still runs

## Phase 2: Add executor microbenches

Changes:

- add `execution_bench.rs`
- add shard-heavy and high-shard synthetic workloads

Acceptance:

- a roaring-vs-`BTreeSet` regression would show up clearly here

## Phase 3: Add clause-loading microbenches

Changes:

- add `query_clause_loading_bench.rs`

Acceptance:

- shard fanout and OR-union costs are benchmarked directly

## Phase 4: Add materialization microbenches

Changes:

- add `materialize_bench.rs`

Acceptance:

- lookup and cache behavior are benchmarked directly

## Verification Plan

At minimum after implementation:

- `cargo bench -p finalized-history-query --no-run`
- `scripts/verify.sh finalized-history-query`

Recommended local smoke set:

- `cargo bench -p finalized-history-query --bench execution_bench`
- `cargo bench -p finalized-history-query --bench materialize_bench`
- `cargo bench -p finalized-history-query --bench query_end_to_end_bench`

For any performance-motivated follow-up patch:

- capture before/after benchmark numbers
- append them to `OPTIMIZATION_LOG.md`
- include the relevant deltas in the commit message body

## Recommendation

Implement only the benchmark layers with the highest payoff:

1. split the current bench file for readability
2. add `execution_bench`
3. add `query_clause_loading_bench`
4. add `materialize_bench`
5. keep one strong end-to-end query bench file

That is enough to catch the important query-path regressions without turning the benchmark suite into maintenance overhead.
