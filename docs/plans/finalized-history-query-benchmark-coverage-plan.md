# Finalized History Query Benchmark Coverage Plan

## Summary

This plan adds benchmark coverage for `crates/finalized-history-query` at the subcomponent level instead of relying only on broad end-to-end Criterion runs.

The immediate motivation is to catch regressions like:

- replacing shard-local roaring operations with eager `BTreeSet<u64>` expansion
- adding hidden per-shard iteration overhead
- turning cache-friendly paths into repeated decode / lookup work

The desired end state is a readable multi-file benchmark suite that makes performance regressions visible in:

- candidate execution
- clause loading
- materialization
- block-scan fallback
- ingest fanout and stream append paths
- codec and storage-adapter hot paths
- end-to-end mixed query workloads

## Problem Statement

The current benchmark coverage in:

- `crates/finalized-history-query/benches/finalized_history_query_bench.rs`

is useful, but too coarse to diagnose or reliably catch subcomponent regressions.

Current gaps:

1. one bench file mixes ingest and several query shapes, which makes it harder to see what changed
2. the suite does not isolate the indexed candidate executor
3. shard-heavy workloads are underrepresented
4. materialization and directory-bucket lookup costs are not benchmarked directly
5. stream append / seal costs are not benchmarked directly
6. codec and manifest/chunk operations are not benchmarked directly
7. there is no explicit benchmark shape targeting high shard IDs above `u32::MAX`

That means a change can regress an important internal path while leaving the current broad query benches noisy or apparently flat.

## Goals

- split benchmark coverage into multiple bench files for readability
- benchmark all major hot subcomponents directly
- add shard-heavy and high-shard-ID workloads
- make indexed execution regressions obvious before they reach full query benches
- keep synthetic workloads deterministic and cheap enough for local iteration
- preserve some broader end-to-end benches for integration-level signal

## Non-Goals

- replacing the existing `benchmarking` crate for distributed or production-shape runs
- introducing CI gating on absolute timings in the first pass
- benchmarking backend-specific remote systems inside the Criterion microbench suite
- building a generic benchmarking framework for the whole workspace

## Desired Bench Layout

Replace the current single-file bench organization with multiple files under:

- `crates/finalized-history-query/benches/common.rs`
- `crates/finalized-history-query/benches/execution_bench.rs`
- `crates/finalized-history-query/benches/query_clause_loading_bench.rs`
- `crates/finalized-history-query/benches/materialize_bench.rs`
- `crates/finalized-history-query/benches/block_scan_bench.rs`
- `crates/finalized-history-query/benches/ingest_fanout_bench.rs`
- `crates/finalized-history-query/benches/streams_bench.rs`
- `crates/finalized-history-query/benches/codec_bench.rs`
- `crates/finalized-history-query/benches/query_end_to_end_bench.rs`

If the suite grows further, it is acceptable to split `streams_bench.rs` into:

- `tail_manager_bench.rs`
- `stream_writer_bench.rs`

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
- clipping candidate sets to:
  - first shard only
  - last shard only
  - both edges
  - full interior shards
- high-shard-ID iteration with shard values above `u32::MAX`

Metrics to care about:

- throughput
- allocation churn
- scaling as shard count increases
- scaling as candidate density increases

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
- large blocks vs small blocks
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
- unnecessary full-blob behavior in point materialization

### 4. Block-scan fallback

Target file:

- `crates/finalized-history-query/benches/block_scan_bench.rs`

Benchmark:

- empty-heavy block ranges
- dense matching blocks
- sparse matching blocks
- large blocks with low selectivity
- large blocks with high selectivity
- resume windows that start inside a block

Regression this should catch:

- repeatedly decoding too much state per block
- poor sequential payload reuse
- expensive handling of empty blocks

### 5. Ingest fanout

Target file:

- `crates/finalized-history-query/benches/ingest_fanout_bench.rs`

Benchmark:

- `collect_stream_appends(...)`
- low-topic-count logs
- max-topic logs
- high address reuse vs low address reuse
- logs that stay in one shard
- logs that cross shard boundaries
- blocks that cross directory-bucket boundaries

Regression this should catch:

- excessive map/set churn during fanout
- shard-transition inefficiencies
- over-allocation in per-stream append collection

### 6. Stream append and sealing

Target file:

- `crates/finalized-history-query/benches/streams_bench.rs`

Benchmark:

- tail append only
- append then seal at entry threshold
- append then seal at byte threshold
- many small streams
- few large streams
- warm manifest/tail load path
- cold manifest/tail load path

Hot code to exercise:

- `TailManager`
- `StreamWriter::apply_appends(...)` and related helpers

Regression this should catch:

- too much manifest rewrite overhead
- unnecessary bitmap copying
- poor scaling with many streams

### 7. Codec hot paths

Target file:

- `crates/finalized-history-query/benches/codec_bench.rs`

Benchmark:

- encode/decode block log headers
- encode/decode log directory buckets
- encode/decode roaring chunks
- encode/decode manifests
- encode/decode realistic logs and block payloads

Regression this should catch:

- serialization overhead that spills into ingest/query hot paths
- larger-than-expected cost for metadata-heavy workloads

### 8. End-to-end query workloads

Target file:

- `crates/finalized-history-query/benches/query_end_to_end_bench.rs`

This replaces the current mixed single-file shape with something easier to read and extend.

Benchmark groups:

- narrow indexed queries
- multi-clause indexed intersections
- wide OR queries still using indexed execution
- broad queries forcing block scan
- pagination-heavy queries
- shard-boundary queries
- high-shard-ID queries

Regression this should catch:

- broad user-visible latency changes
- mismatches between microbench wins and actual query behavior

## Workload Matrix

Every benchmark category does not need every axis, but the suite as a whole should cover them.

### Shard shape

- `1` shard
- `8` shards
- `64` shards
- high shard `0x1_0000_0000`

### Candidate density

- sparse: `0.01%` to `0.1%`
- medium: `1%` to `5%`
- dense: `25%` to `90%`

### OR width

- `1`
- `4`
- `16`
- `64`
- `256`

### Block shape

- many empty blocks
- many small blocks
- fewer large blocks
- large blocks with repeated addresses/topics
- blocks crossing directory-bucket boundaries

### Cache shape

- cold cache
- warm cache
- same-block locality
- cross-block random access

## Benchmark Data Strategy

Use synthetic deterministic data for Criterion benches.

Reasons:

- stable and cheap to generate
- reproducible locally
- easy to tune density and shard count
- avoids remote backend noise

The synthetic fixtures should explicitly support:

- controlled shard count
- controlled local-ID density
- controlled value reuse across address/topic streams
- controlled block-size distribution
- controlled empty-block frequency

Keep the distributed and production-shape workloads in the existing `benchmarking` crate and scripts.

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
- optional helper materializer/store stubs for execution-only benches

### New bench files

Add the files listed in the desired layout above.

### Existing bench file

Either:

1. delete `finalized_history_query_bench.rs` after the split, or
2. keep it temporarily as a thin umbrella file while moving logic out

Recommendation:

- remove it after the split to avoid duplicate coverage and confusion

## Implementation Phases

## Phase 1: Split the current bench file and introduce common helpers

Changes:

- add `benches/common.rs`
- move current ingest benches to `ingest_fanout_bench.rs` or `query_end_to_end_bench.rs` as appropriate
- move current broad query benches to `query_end_to_end_bench.rs`
- update `Cargo.toml` bench targets

Acceptance:

- no benchmark logic duplication
- existing bench coverage still runs
- bench files are readable on their own

## Phase 2: Add executor and clause-loading microbenches

Changes:

- add `execution_bench.rs`
- add `query_clause_loading_bench.rs`
- add shard-heavy and high-shard synthetic workloads

Acceptance:

- direct benchmark coverage exists for the indexed execution core
- a roaring-vs-`BTreeSet` regression would show up clearly in these benches

## Phase 3: Add materialization and block-scan microbenches

Changes:

- add `materialize_bench.rs`
- add `block_scan_bench.rs`

Acceptance:

- query-path lookup and decode work are isolated from clause loading
- cache hit vs miss behavior is visible

## Phase 4: Add ingest fanout, streams, and codec benches

Changes:

- add `ingest_fanout_bench.rs`
- add `streams_bench.rs`
- add `codec_bench.rs`

Acceptance:

- ingest-side and storage-format regressions are covered directly

## Phase 5: Tune naming, documentation, and workflow

Changes:

- document bench commands in crate docs or a focused benchmark README section
- define the expected “run these first” local workflow
- optionally add a lightweight script for running the finalized-history-query bench suite in a fixed order

Acceptance:

- contributors know which bench file to use for which kind of change

## Benchmark Naming Guidance

Names should encode the real shape under test.

Prefer names like:

- `intersect_sparse_8_shards`
- `or_union_64_values_high_shard`
- `materialize_warm_header_cache_same_block`
- `block_scan_empty_heavy_range`

Avoid names like:

- `bench1`
- `query_large`
- `mixed2`

## Verification Plan

At minimum after implementation:

- `cargo bench -p finalized-history-query --no-run`
- routine crate verification with `scripts/verify.sh finalized-history-query`

Recommended local benchmark smoke set:

- `cargo bench -p finalized-history-query --bench execution_bench`
- `cargo bench -p finalized-history-query --bench materialize_bench`
- `cargo bench -p finalized-history-query --bench query_end_to_end_bench`

For any performance-motivated follow-up patch:

- capture before/after benchmark numbers
- append them to `OPTIMIZATION_LOG.md`
- include the relevant deltas in the commit message body

## Risks

### Too many benches too quickly

A very broad suite can become expensive or noisy.

Mitigation:

- keep Criterion microbenches synthetic and in-memory
- reserve distributed or soak runs for the `benchmarking` crate

### Poor fixture design

If each bench hand-builds different data, results become hard to compare.

Mitigation:

- centralize fixture builders in `benches/common.rs`

### Overfitting to synthetic data

A microbench can hide integration-level regressions.

Mitigation:

- keep end-to-end query benches
- continue using the separate distributed benchmarking workflows

## Recommendation

Implement this as a staged benchmark refactor:

1. split the current single bench file into multiple files
2. add executor and clause-loading microbenches first
3. add materialization, ingest, stream, and codec benches next
4. keep end-to-end query benches as the final integration layer

The executor and clause-loading benches should be treated as the first-class guardrails against future regressions like eager `BTreeSet` expansion.
