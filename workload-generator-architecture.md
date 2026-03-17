# Log Workload Generator Architecture Plan (v1)

## 1. Purpose

Define the concrete architecture for implementing the spec in `workload-sidecar-spec.md` as a standalone crate:

- crate: `log-workload-gen`
- role: consume finalized chain events, build portable dataset artifacts, generate deterministic `eth_getLogs`-shaped traces

## 1.1 Normative vs informative

- Normative: public API, message protocol, artifact schemas/orderings, determinism rules, failure semantics.
- Informative: module/file layout and internal organization suggestions.

## 2. Design goals

- Strict decoupling from index internals and storage backends.
- Deterministic outputs across runs and across `max_threads` settings.
- High-throughput ingestion from caller-provided channel.
- Portable artifacts readable by multiple benchmark adapters.
- Simple operational model: batch run, fail closed on invalid input.

## 3. Non-goals

- Query execution or RPC serving.
- Tip/reorg logic.
- Adaptive online tuning from production telemetry.
- Distributed multi-node execution.

## 4. Crate structure

Proposed layout:

```text
crates/log-workload-gen/
  src/
    lib.rs
    config.rs
    error.rs
    types.rs
    pipeline.rs
    ingest/
      consumer.rs
      validator.rs
      aggregator.rs
    stats/
      key_stats.rs
      cooccurrence.rs
      range_stats.rs
      hll.rs
    artifact/
      manifest.rs
      parquet_writer.rs
      trace_writer.rs
      io.rs
    generate/
      planner.rs
      sampler.rs
      templates.rs
      trace.rs
    runtime/
      threadpool.rs
      bounded_queue.rs
  tests/
    deterministic.rs
    sparse_ranges.rs
    completion_protocol.rs
    profile_defaults.rs
    artifact_roundtrip.rs
```

## 5. Public API

## 5.1 Main entrypoints

- `async fn run_collect(config: GeneratorConfig, receiver: impl MessageReceiver) -> Result<DatasetSummary, Error>`
- `async fn run_collect_and_generate(config: GeneratorConfig, receiver: impl MessageReceiver) -> Result<DatasetSummary, Error>`
- `async fn run_offline_generate(config: GeneratorConfig, dataset_path: &Path) -> Result<TraceSummary, Error>`

`receiver` carries:

- `Message::ChainEvent(ChainEvent)`
- `Message::EndOfStream { expected_end_block }`

## 5.2 Config object

`GeneratorConfig` mirrors spec Section 8.3 exactly. The crate validates and canonicalizes this config before processing.
It also includes bounded-queue settings:

- `event_queue_capacity` (default `8192`, minimum `1`)
- `task_queue_capacity` (default `4096`, minimum `1`)

## 5.3 Stable types

- `ChainEvent`
- `TraceEntry`
- `DatasetManifest`
- `DatasetSummary`
- `TraceSummary`

All serializable with stable field names aligned to spec.

## 6. Dataflow

1. **Input receive**  
   Consume channel messages, enforce completion protocol.
2. **Validation**  
   Enforce monotonicity/hash rules; track gaps and observed-block set.
3. **Aggregation**  
   Update key/cooccurrence/range accumulators.
4. **Artifact finalize**  
   Write Parquet + manifest via temp path + atomic rename.
5. **Trace generation (optional)**  
   Load stats in-memory, sample templates/params deterministically, emit JSONL traces.

## 7. Concurrency model

- Global worker budget: `max_threads`.
- Runtime provides a shared worker pool and bounded task queues.
- Collection and generation can each parallelize internally, but combined active workers never exceed the global cap.
- Determinism rule: parallel execution must not affect output ordering/content.

Implementation approach:

- Partitioned local accumulators per worker.
- Deterministic merge order at barriers (sorted partition IDs).
- Stable sort for emitted rows and trace entries by deterministic IDs.
- Backpressure comes from bounded queues; producers await capacity rather than growing memory unbounded.

## 8. Determinism strategy

- Single seeded RNG root from config.
- Derive per-stage/per-worker RNG streams via deterministic split function.
- Stable iteration order over maps (convert to sorted vectors before write).
- Same dataset + config + seed produces byte-stable artifacts except permitted timestamps.

Resolved choices:

- RNG: `rand_chacha::ChaCha20Rng`.
- RNG split: domain-separated seed derivation using SHA-256 over `(root_seed, stage, profile, worker_id, partition_id)`.

## 8.1 Stable ordering rules

- `key_stats.parquet`: sort by `(key_type asc, key_value asc)`.
- `cooccurrence.parquet`: sort by `(pair_type asc, count_total desc, left_key asc, right_key asc)`.
- `range_stats.parquet`: sort by `(metric asc, bucket_lower asc)`.
- `trace_*.jsonl`: `id` strictly increasing from `0` and rows emitted in `id` order.

## 9. Stats subsystem design

## 9.1 Key stats

- Exact counters: `count_total`, `first_block`, `last_block`, `active_block_count`.
- `distinct_partner_estimate` via HLL sketches for `address` and `topic0` rows.
- HLL: HyperLogLog++ with default precision `p=14` (configurable).

## 9.2 Cooccurrence

- Track pair counts for `address_topic0` and `topic0_topic1`.
- Maintain top-k per `pair_type` with bounded heaps during finalize.

## 9.3 Range stats

- Histograms using fixed log2 buckets.
- Metrics: `logs_per_block`, `logs_per_window`, `interarrival_seconds`.
- Windowing anchored to block numbers per spec.

## 10. Artifact writing

## 10.1 Manifest

- Include gap metadata, canonical config hash (RFC 8785 + SHA-256), validity flags.

## 10.2 Parquet

- One file per table (`key_stats`, `cooccurrence`, `range_stats`).
- Explicit schema builders with strict field order and types.
- Implementation library: `arrow-rs` + `parquet` directly.

## 10.3 JSONL traces

- One file per profile.
- Strict line-delimited JSON; deterministic `id` assignment.

## 10.4 Atomic finalize

- Write a full dataset into a temp directory adjacent to destination.
- `fsync` files and directory as supported.
- Publish by single atomic directory rename on success.
- Readers must treat dataset directory presence as commit marker; no partial file visibility is allowed.

## 11. Error model

Error classes:

- `InputInvalid` (monotonicity/hash/completion protocol failures)
- `ConfigInvalid`
- `Io`
- `Serialization`
- `InternalInvariant`

Failure policy:

- Mark dataset invalid on input protocol violations.
- Return typed errors for unrecoverable runtime failures.

## 12. Observability outputs

Run summary includes:

- `blocks_seen`, `logs_seen`
- `artifact_write_seconds`, `trace_generate_seconds`
- `trace_queries_generated` per profile
- `max_threads_used`, `max_queue_depth`
- coverage summary and validity state

Run summary artifact:

- File: `run_summary.json`
- Format: canonical JSON object with stable keys and scalar types.
- Required keys:
  - `blocks_seen: u64`
  - `logs_seen: u64`
  - `artifact_write_seconds: f64`
  - `trace_generate_seconds: f64`
  - `trace_queries_generated: { expected: u64, stress: u64, adversarial: u64 }`
  - `max_threads_used: u32`
  - `max_queue_depth: u64`
  - `dataset_valid: bool`
  - `invalid_reason: string|null`

## 13. Benchmark adapter contract

Adapters for external systems consume:

- `dataset_manifest.json`
- `trace_*.jsonl`

No dependency on this crate’s Rust types required; JSON/Parquet schemas are the interoperability boundary.

## 14. Test strategy

## 14.1 Unit tests

- Validation rules (duplicates, non-monotonic, hash mismatch, EOS protocol).
- Histogram bucketing and window math.
- Config validation and canonical hash reproducibility.

## 14.2 Property tests

- Determinism under thread-count changes (`1` vs `num_cpus`).
- Deterministic merge/id ordering under shuffled event chunking.

## 14.3 Integration tests

- End-to-end collect-only and collect-and-generate on fixture datasets.
- Sparse-range datasets verify `gap_count`, `missing_block_ranges`, and `observed_block_coverage_ratio`.
- Artifact roundtrip checks for schema compatibility.

## 15. Implementation stages

1. **Scaffold + types + config validation**  
   Deliver compilable crate with core types and config schema.
   Exit criteria: builds cleanly; config validation tests pass; canonical config hash test passes.
2. **Ingestion validator + summaries**  
   Implement message protocol, monotonic checks, gap tracking.
   Exit criteria: completion-protocol tests pass; invalid-input cases produce expected `invalid_reason`.
3. **Stats accumulators + Parquet writers**  
   Implement key/cooccurrence/range stats and final artifacts.
   Exit criteria: Parquet schema roundtrip tests pass; ordering rules tests pass.
4. **Trace generator**  
   Implement template sampling/profiles and JSONL outputs.
   Exit criteria: trace schema tests pass; profile mix tolerance tests pass.
5. **Concurrency + determinism hardening**  
   Add worker pool, bounded queues, deterministic merges.
   Exit criteria: deterministic outputs match under `max_threads=1` and `max_threads=num_cpus`; bounded-queue tests pass.
6. **Test and polish**  
   Property/integration coverage, docs, bench harness hooks.
   Exit criteria: full test suite green; docs updated; basic benchmark adapter smoke tests pass.

Each stage ends with: `impl -> tests -> fix -> refactor/simplify -> fmt -> lint`.

## 16. Resolved implementation choices

1. Parquet library: `arrow-rs` + `parquet` directly.
2. RNG strategy: `rand_chacha::ChaCha20Rng` with SHA-256 domain-separated child seeds.
3. HLL: HyperLogLog++ with default precision `p=14`.
4. Maps: `hashbrown::HashMap` in hot path; deterministic sort at merge/flush boundaries.
