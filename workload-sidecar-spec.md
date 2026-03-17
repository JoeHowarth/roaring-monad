# Log Workload Generator Spec (v1)

## 1. Purpose

Define a standalone batch crate that consumes finalized blockchain log data and produces portable workload artifacts for benchmarking `eth_getLogs` across multiple RPC/index backends.

This crate is intentionally independent from any specific index internals (manifests, chunks, storage schema).

## 2. Scope

### In scope

- Consume normalized chain events from a channel (source of events is caller's concern).
- Maintain aggregate statistics needed to synthesize realistic benchmark queries.
- Persist dataset artifacts with deterministic, versioned schemas.
- Generate benchmark traces (sequences of `eth_getLogs`-shaped queries) from artifacts with reproducible seeds.

### Out of scope

- Executing queries against any index or RPC endpoint.
- Reading index-specific metadata (manifests, chunk refs, planner internals).
- Tip-window/reorg handling logic (input stream is assumed finalized for v1).
- Serving online traffic.
- Sourcing or producing the event stream (caller provides a channel).

## 3. Crate Boundary

Proposed crate name: `log-workload-gen`.

The crate exposes:

- A channel-based event consumer.
- Artifact writers/readers.
- Query trace synthesis APIs.

The crate does not depend on `finalized-log-index`.

## 4. Terminology

- `ChainEvent`: normalized finalized block event that includes transactions, receipts, and logs.
- `Dataset`: final artifacts (stats + manifest) for a block range and chain.
- `Trace`: generated sequence of `eth_getLogs`-shaped benchmark queries with labels and metadata.

## 5. Input Model

### 5.1 Event contract

Each event must include:

- `chain_id: u64`
- `block_number: u64`
- `block_hash: [u8; 32]`
- `timestamp: u64` (unix seconds)
- receipts/logs with at least:
  - `tx_index: u32`
  - `log_index: u32`
  - `address: [u8; 20]`
  - `topics: Vec<[u8; 32]>` (0 to 4)

v1 assumes events are finalized and strictly increasing by block number.

### 5.2 Ordering behavior

- Duplicate block events are ignored if `(chain_id, block_number, block_hash)` already committed.
- Block number gaps are allowed (sparse ranges are valid; the dataset records only blocks actually observed).
- If a non-monotonic block number is observed, the generator must fail closed and mark the dataset invalid.
- If the same block number appears with a different hash, the generator must fail closed and mark the dataset invalid.

## 6. Output Artifacts

All artifacts are versioned and portable across implementations.
Format defaults are normative for v1:

- Tabular stats artifacts use Parquet.
- Dataset metadata uses JSON.
- Query traces use JSONL.

### 6.1 Required files

- `dataset_manifest.json`
- `key_stats.parquet`
- `cooccurrence.parquet`
- `range_stats.parquet`
- Optional generated files:
  - `trace_expected.jsonl`
  - `trace_stress.jsonl`
  - `trace_adversarial.jsonl`

### 6.2 Dataset manifest

Fields:

- `schema_version` (string, e.g. `1.0.0`)
- `crate_version`
- `chain_id`
- `start_block`
- `end_block`
- `blocks_observed` (count of distinct blocks actually seen; may be less than `end_block - start_block` if sparse)
- `gap_count` (number of block-number gaps in the observed range; 0 means contiguous)
- `missing_block_ranges` (list of `[start, end]` inclusive pairs for each gap, capped at first 100 entries; null if contiguous)
- `event_count`
- `log_count`
- `created_at`
- `config_hash` (lowercase hex SHA-256 of the config serialized via RFC 8785 JSON Canonicalization Scheme, UTF-8 bytes)
- `seed` (if trace generation was run; nullable)
- `valid` (boolean)
- `invalid_reason` (nullable string)

### 6.3 Key stats schema

Rows keyed by `key_type` + `key_value` where:

- `key_type in {address, topic0, topic1, topic2, topic3, address_topic0}`

For compound key `address_topic0`, `key_value` is the concatenation `address || topic0` (20 + 32 bytes, hex-encoded).

Columns:

- `count_total`: number of log entries matching this key.
- `first_block`: earliest block number where this key appears.
- `last_block`: latest block number where this key appears.
- `active_block_count`: number of distinct blocks containing this key.
- `distinct_partner_estimate`: HyperLogLog estimate of the paired dimension, present only on simple key rows. `address` rows store the estimated distinct `topic0` count for that address; `topic0` rows store the estimated distinct address count. Null for `topic1`–`topic3` and for compound key rows (where both dimensions are already fixed).

### 6.4 Cooccurrence schema

Rows for top-k pairs (default k = 10,000 per `pair_type`, configurable):

- `pair_type in {address_topic0, topic0_topic1}`
- `left_key`
- `right_key`
- `count_total`
- `first_block`
- `last_block`

Top-k is ranked by `count_total` within each `pair_type`.

### 6.5 Range stats schema

All histograms use fixed log2-scale bucket boundaries: `[0, 1), [1, 2), [2, 4), [4, 8), ..., [2^i, 2^(i+1))` up to `2^32`.

Each histogram row contains:

- `metric` (string enum: `logs_per_block`, `logs_per_window`, `interarrival_seconds`)
- `bucket_lower` (inclusive)
- `bucket_upper` (exclusive)
- `count`
- `window_size_blocks` (present only for `logs_per_window` rows; null otherwise)

`logs_per_window` uses fixed block-number intervals: the full `[start_block, end_block]` range is divided into non-overlapping windows of `window_size_blocks` (default 1000). Windows are anchored to block numbers, not observation order — a window spanning a gap simply has fewer logs. The last window may be shorter than `window_size_blocks`.

`interarrival_seconds` is included only when input events carry non-zero timestamps.

## 7. Runtime Modes

### 7.1 Collect-only

- Consume events from channel.
- Write final dataset artifacts on channel close.
- Do not generate query traces.

### 7.2 Collect-and-generate

- Same as collect-only.
- After artifact write, generate traces according to profile configs.

### 7.3 Offline-generate

- Read existing dataset artifacts from disk.
- Generate new traces with provided seed/profile.

## 8. Query Trace Model

Each trace entry maps directly to an `eth_getLogs` filter object.

Trace entry format (JSONL):

- `id` (stable within trace)
- `profile in {expected, stress, adversarial}`
- `template` (string enum, see 8.1)
- `from_block`
- `to_block`
- `address_or` (list; maps to `eth_getLogs` `address` param)
- `topic0_or` (list; maps to `topics[0]` position)
- `topic1_or` (list; maps to `topics[1]` position)
- `topic2_or` (list; maps to `topics[2]` position)
- `topic3_or` (list; maps to `topics[3]` position)
- `expected_selectivity_bucket in {empty, tiny, small, medium, large, huge}`
- `observed_block_coverage_ratio` (float in `[0,1]`; fraction of observed blocks within `[from_block,to_block]`)
- `notes` (optional labels)

Empty lists mean "no constraint" for that field (wildcard). Non-EVM support is out of scope for v1.

`from_block` and `to_block` must both fall within `[start_block, end_block]` from the dataset manifest.
Ranges are allowed to span gaps. The generator must compute and emit `observed_block_coverage_ratio` so sparse-range behavior is explicit and portable.

### 8.1 Query templates

- `single_address`: one address, all topics wildcard.
- `single_topic0`: one topic0, address wildcard.
- `address_topic0`: one address + one topic0.
- `multi_address`: OR-list of addresses (2+), topics wildcard.
- `multi_topic0`: OR-list of topic0s (2+), address wildcard.
- `compound`: address(es) + topic(s) combined, any OR-list widths.

### 8.2 Generator config (v1 defaults)

- `trace_size_per_profile = 100_000`
- `scale_factor = 1.0`
- `max_threads = num_cpus` (configurable upper bound on internal worker concurrency)

Effective generated count per profile:

- `effective_trace_size = round(trace_size_per_profile * scale_factor)`

`scale_factor` is the preferred knob for larger offline runs without changing profile definitions.
`max_threads` bounds parallel work in collection and generation stages.

### 8.3 Config schema (machine-readable shape)

Canonical config object (JSON shape, types, defaults):

```json
{
  "trace_size_per_profile": 100000,
  "scale_factor": 1.0,
  "max_threads": "num_cpus",
  "cooccurrence_top_k_per_type": 10000,
  "logs_per_window_size_blocks": 1000,
  "profiles": {
    "expected": {
      "template_mix": {
        "single_address": 0.30,
        "single_topic0": 0.25,
        "address_topic0": 0.25,
        "multi_address": 0.10,
        "multi_topic0": 0.08,
        "compound": 0.02
      },
      "address_or_width": { "min": 1, "max": 4 },
      "topic0_or_width": { "min": 1, "max": 4 },
      "block_range_blocks": {
        "source": "empirical",
        "min": 1,
        "max": 50000
      },
      "empty_result_target_share": 0.0
    },
    "stress": {
      "template_mix": {
        "single_address": 0.10,
        "single_topic0": 0.20,
        "address_topic0": 0.20,
        "multi_address": 0.20,
        "multi_topic0": 0.20,
        "compound": 0.10
      },
      "address_or_width": { "min": 2, "max": 32 },
      "topic0_or_width": { "min": 2, "max": 32 },
      "block_range_blocks": {
        "source": "empirical_upper_tail",
        "min": 1000,
        "max": 500000
      },
      "empty_result_target_share": 0.0
    },
    "adversarial": {
      "template_mix": {
        "single_address": 0.05,
        "single_topic0": 0.15,
        "address_topic0": 0.15,
        "multi_address": 0.25,
        "multi_topic0": 0.25,
        "compound": 0.15
      },
      "address_or_width": { "min": 8, "max": 128 },
      "topic0_or_width": { "min": 8, "max": 128 },
      "block_range_blocks": {
        "source": "heavy_near_full_range",
        "min": 10000,
        "max": "full_range"
      },
      "empty_result_target_share": 0.10
    }
  }
}
```

Validation rules:

- `trace_size_per_profile >= 1`
- `scale_factor > 0`
- `max_threads >= 1` after resolving `"num_cpus"`
- `cooccurrence_top_k_per_type >= 1`
- For each profile, `sum(template_mix values) == 1.0` within epsilon `1e-9`
- For each OR width, `1 <= min <= max`
- For each block range, `1 <= min <= max` unless `max == "full_range"`
- `0.0 <= empty_result_target_share <= 1.0`

## 9. Workload Profiles

### 9.1 Expected profile

Goal: approximate regular production demand.

Selection is weighted toward empirically frequent keys and shorter/mid block windows.
Concrete defaults (template mix, OR widths, and range clamps) are defined in Section 8.3.

### 9.2 Stress profile

Goal: sustained load on expensive but realistic patterns.

Selection favors larger windows, wider OR-lists, and higher intersection pressure.
Concrete defaults are defined in Section 8.3.

### 9.3 Adversarial profile

Goal: expose worst-case planner/materialization behavior.

Selection favors high fan-out OR patterns, near-full-range scans, and intentional empty-result share.
Concrete defaults are defined in Section 8.3.

## 10. Determinism

- All random choices must use an explicit seed.
- Given same input dataset + config + seed, output traces must be byte-stable except for header timestamps.
- Manifest must record seed and config hash.

## 11. Throughput and Concurrency

- The generator should process events as fast as possible to keep up with the input channel.
- The caller controls effective throughput/resource use by controlling channel feed rate.
- Internal parallelism is bounded by `max_threads` across the entire process (global cap, not per stage).
- `max_threads` must be >= 1. If unset, default to `num_cpus`.
- Collection and trace-generation stages may each use worker pools, but total concurrent workers across all pools must never exceed `max_threads`.
- Internal channels/queues must be bounded to avoid unbounded buffering under backpressure.

## 12. Failure Handling

This is a batch tool. On failure, re-run from the beginning.

- Final artifact writes use temp path + atomic rename. A crash mid-write leaves no partial artifacts.

### 12.1 Channel completion protocol

The event channel carries two message types:

- `ChainEvent` — a normal block event as defined in Section 5.1.
- `EndOfStream { expected_end_block: u64 }` — terminal signal from the caller indicating the intended final block.

On receiving `EndOfStream`, the generator verifies that the last observed block matches `expected_end_block`. Mismatch marks the dataset invalid with reason.

If the channel closes (drops) without an `EndOfStream`, the generator marks the dataset invalid with reason `"channel_closed_unexpectedly"`. This distinguishes a clean completion from a crashed feeder.

## 13. Observability

Emit a structured run summary report on completion:

- `blocks_seen`
- `logs_seen`
- `artifact_write_seconds`
- `trace_generate_seconds`
- `trace_queries_generated` (per profile)
- `max_threads_used`
- `max_queue_depth`
- `dataset_valid` (boolean)
- Coverage stats: block range, log density summary.

## 14. Compatibility and Versioning

- Semantic versioned artifact schemas.
- Readers must reject unsupported major versions.
- Minor version bumps may add nullable fields only.

## 15. Security and Privacy

- No private key material or raw tx payloads stored.
- Artifacts may contain addresses/topics and are considered sensitive operational metadata.
- Optional hash/anonymization mode may be added later; not in v1.

## 16. Acceptance Criteria (v1)

- Can consume >= 10M logs in finalized monotonic stream without producing an invalid dataset.
- Produces required artifacts and valid manifest.
- Deterministic trace generation verified across repeated runs with same seed.
- Deterministic outputs are invariant across `max_threads` settings (e.g. `1` vs `num_cpus`) for same dataset and seed.
- Can generate all three trace profiles from the same dataset.
- Default generator config produces 100,000 queries per profile at `scale_factor = 1.0`.
- Every trace entry can be mechanically converted to a valid `eth_getLogs` JSON-RPC filter object.
- Internal buffering is bounded and observable via `max_queue_depth` in run summary.
- Can be used by at least two distinct RPC/index benchmark adapters without crate modifications.

## 17. Explicit Non-Goals (v1)

- Dynamic adaptation from runtime query telemetry.
- Index-internal path calibration (manifests/chunks).
- Distributed state sharding.

## 18. Closed Decisions (v1)

1. Artifact format split is fixed: Parquet stats, JSON manifest, JSONL traces.
2. `address_topic0` is first-class in v1 and remains part of `key_type`.
3. Default trace size is fixed at `100_000` queries per profile, scaled by `scale_factor`.
4. Per-epoch temporal slices are deferred to post-v1.
