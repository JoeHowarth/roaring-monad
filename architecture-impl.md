# Finalized Log Index Architecture and Implementation Guide

Status: Draft  
Applies to: `spec-v2.md` (finalized-only scope)

---

## 1. Purpose

This document describes how to implement the finalized log index crate in Rust.

The crate's responsibility is strictly:

1. Persist finalized EVM logs and index structures.
2. Serve finalized `eth_getLogs`-equivalent queries efficiently.
3. Expose deterministic, restart-safe behavior over abstract KV/object stores.

The crate does not track or serve non-finalized tip data.

---

## 2. Scope Boundary

### In Scope

1. Finalized block ingest and persistence.
2. Finalized query planning and execution.
3. Chunked roaring index maintenance.
4. Recovery, GC, and operational guardrails for finalized data.

### Out of Scope

1. Canonical head tracking.
2. Finality determination.
3. Tip window buffering for latest blocks.
4. Merging finalized and non-finalized query results.

Those concerns are handled by upstream/downstream components and integrated via explicit APIs.

---

## 3. External Integration Contract

### 3.1 Upstream Inputs (Required)

Upstream component provides finalized canonical blocks in strict order:

1. Monotonic block numbers (`B = last + 1`).
2. Correct parent linkage to previous finalized block.
3. No replacement of blocks at or below provided finalized height.

If upstream violates these guarantees, this crate enters fail-closed degraded mode.

### 3.2 Downstream Query Composition (Out of Scope)

External RPC composition layer must:

1. Split user range into finalized and non-finalized portions.
2. Query finalized portion from this crate.
3. Query non-finalized portion from separate tip-window service.
4. Merge and sort by `(block_num, tx_idx, log_idx)`.

### 3.3 Crate API Surface

Recommended public API:

1. `ingest_finalized_block(block) -> Result<IngestOutcome>`
2. `query_finalized(filter) -> Result<Vec<Log>>`
3. `indexed_finalized_head() -> Result<u64>`
4. `health() -> HealthReport`

---

## 4. Storage Abstractions

Use two async store traits.

### 4.1 `MetaStore` (small mutable records)

Responsibilities:

1. CAS/versioned updates.
2. Key/value reads.
3. Prefix listing for GC and diagnostics.
4. Fencing-aware writes/deletes.

### 4.2 `BlobStore` (large immutable payloads)

Responsibilities:

1. Immutable chunk object writes.
2. Chunk reads.
3. Best-effort blob deletion for GC.

### 4.3 Required Semantics

1. `MetaStore` must provide compare-and-swap via record version.
2. Every write/delete must include fencing token (`writer_epoch`).
3. Failed CAS must not partially mutate target key.
4. Blob writes are idempotent by deterministic key naming.

If a backend cannot satisfy these semantics directly, use a gateway that enforces them.

---

## 5. Keyspace and Data Layout

### 5.1 Canonical Keys

1. `logs/{global_log_id_be}` -> serialized `Log`
2. `block_meta/{block_num_be}` -> serialized `BlockMeta`
3. `block_hash_to_num/{block_hash}` -> `block_num`
4. `meta/state` -> `{ indexed_finalized_head, next_log_id, writer_epoch }`

### 5.2 Index Keys

Stream identity:

`stream_id = index_kind || value_hash || shard_hi32`

Keys:

1. `manifests/{stream_id}` -> manifest header
2. `manifest_segments/{stream_id}/{segment_id}` -> chunk refs segment
3. `tails/{stream_id}` -> mutable roaring tail checkpoint
4. `chunks/{stream_id}/{chunk_seq}` -> immutable chunk blob
5. `topic0_mode/{sig}` -> hot/cold policy state
6. `topic0_stats/{sig}` -> rolling-window counters/bitset for hot/cold mode transitions

Required `ChunkRef` fields in manifest segments/header:

1. `chunk_seq: u64`
2. `min_local: u32`
3. `max_local: u32`
4. `count: u32`

### 5.3 Encoding Notes

1. Numeric key parts use big-endian for lexical ordering.
2. Chunk payload format includes checksum (`crc32`) and version byte.
3. All structs are versioned for forward migration.

---

## 6. Internal Rust Modules

Suggested crate layout:

```text
src/
  lib.rs
  api.rs
  error.rs
  config.rs
  domain/
    types.rs
    filter.rs
    keys.rs
  store/
    traits.rs
    meta.rs
    blob.rs
  codec/
    chunk.rs
    manifest.rs
    log.rs
  ingest/
    engine.rs
    planner.rs
    tail_manager.rs
    chunk_manager.rs
  query/
    engine.rs
    planner.rs
    executor.rs
  recovery/
    startup.rs
  gc/
    worker.rs
  lease/
    manager.rs
  metrics/
    counters.rs
```

Core runtime objects:

1. `IngestEngine`
2. `QueryEngine`
3. `TailManager`
4. `ChunkManager`
5. `LeaseManager`
6. `GcWorker`

---

## 7. Ingest Architecture (Finalized Only)

### 7.1 Ingest Transaction Outline

For finalized block `B`:

1. Verify lease ownership (`writer_epoch = E`).
2. Read `meta/state`.
3. Enforce `B == indexed_finalized_head + 1`.
4. Verify parent linkage.
5. Assign `first_log_id = next_log_id`.
6. Write canonical log records (`logs/*`).
7. Write block metadata (`block_meta/*`, `block_hash_to_num/*`).
8. Produce stream appends from each log:
   - `addr`, `topic1`, `topic2`, `topic3` at log-level.
   - `topic0_block` (dedup per block).
   - `topic0_log` only if enabled for signature.
9. Apply appends to in-memory tails.
10. Seal tails hitting thresholds into immutable chunks.
11. Publish manifest updates via CAS.
12. Persist updated tails.
13. CAS commit `meta/state` to new finalized head and next log id.

If any step before state CAS fails, retry idempotently.

### 7.2 Idempotency Rules

1. Canonical writes use deterministic keys.
2. Chunk keys include deterministic stream and sequence.
3. Manifest CAS prevents lost updates.
4. State CAS is the visibility barrier.
5. Tail re-append after crash is safe: roaring inserts are set-idempotent.

### 7.3 Failure and Degraded Mode

Enter degraded fail-closed mode if:

1. Finality violation detected (replacement at `<= indexed_finalized_head`).
2. Lease lost during critical section and cannot be reacquired safely.
3. Storage invariants violated (corruption/checksum mismatch in required metadata).

---

## 8. Chunk and Tail Lifecycle

### 8.1 Tail Behavior

Per stream, keep mutable roaring tail in memory plus periodic persisted checkpoint.

Flush conditions:

1. Once per finalized block ingest.
2. Or on timer (`5s` default).

### 8.2 Seal Behavior

Seal tail into immutable chunk when:

1. `entry_count >= 1950` (default)
2. `serialized_bytes >= target_bytes`
3. `maintenance_seal_interval` elapsed (`10m` default)

### 8.3 Publish Sequence

1. Write chunk blob to `BlobStore`.
2. CAS manifest header/segment references.
3. Persist tail checkpoint.

Readers only trust chunk references reachable from current manifest.

---

## 9. Query Architecture (Finalized Only)

### 9.1 Planner Steps

1. Read `meta/state` and snapshot `Hf = indexed_finalized_head`.
2. If `blockHash` present, execute dedicated blockHash path.
3. Resolve requested range tags to numbers.
4. Clip range to `[0, Hf]`; return empty if no overlap.
5. Resolve clipped range to log-id interval via `block_meta` endpoints.
6. Compute shard fan-out:
   - log-level shards: `[from_log_id >> 32, to_log_id >> 32]`
   - block-level shards for `topic0_block`: `[from_block >> 32, to_block >> 32]`
7. For each clause value, discover overlapping streams across shard range, load overlapping chunks/tails, and union per-value bitmaps.
8. Estimate per-clause selectivity from overlap-aware metadata:
   - sum `ChunkRef.count` for overlapping chunk refs
   - add overlap-aware tail counts
9. Intersect clauses in estimated-selectivity order.
10. Apply `topic0_block` as late block-membership filter over surviving candidates.
11. Point-read candidate logs and exact-match filter.
12. Stop early when `max_results` is reached.
13. Return stable sorted output.

Rationale for late `topic0_block` filtering:

1. Avoids materializing block-expanded log-id masks.
2. Cost is proportional to surviving candidate logs rather than `matched_blocks * logs_per_block`.

OR-list guardrail:

1. Enforce `planner.max_or_terms` for any single clause OR-list.
2. If exceeded, return `query too broad` or fall back to block-driven policy (operator-configured).

### 9.2 `blockHash` Path

1. Reject if combined with `fromBlock`/`toBlock` (`-32602`).
2. Lookup `block_hash_to_num`.
3. If missing or mismatch with `block_meta`, return `-32000 Block not found.`
4. Query single block only.

### 9.3 Caching

Use bounded caches for:

1. Manifest headers/segments.
2. Tail checkpoints.
3. Recent chunk blobs.

Recommended: LRU with size and entry caps, plus hit/miss metrics.

### 9.4 Query Limits

Public query interface should include optional `max_results`.

1. Planner should preserve limit during execution planning.
2. Executor should short-circuit candidate materialization at the limit.
3. RPC layer may impose stricter limits, but crate-level limit support is required.

---

## 10. Concurrency Model

### 10.1 Writers

Single writer only.

Requirements:

1. Renewable lease with fencing epoch.
2. Every mutating `MetaStore` op includes fence precondition.
3. Writer stops ingest immediately on lease loss.

### 10.2 Readers

1. Readers are lock-free relative to ingest.
2. Query takes state snapshot at start and uses it for full execution.
3. Readers never rely on uncommitted state.

---

## 11. Recovery Model

### 11.1 Startup

1. Read `meta/state`.
2. Load planner stats catalogs.
3. Start with lazy loading of manifests/tails.
4. Optionally warm top-K streams asynchronously.

No full logs scan is required.

### 11.2 Corruption Handling

1. Checksum mismatch in chunk referenced by manifest triggers degraded mode.
2. Operator runbook decides rebuild range.
3. Crate does not silently skip corrupt referenced data.

---

## 12. GC and Backpressure

### 12.1 GC Responsibilities

1. Delete orphan chunk blobs not referenced by any manifest.
2. Delete stale manifest segments.
3. Delete superseded tail checkpoint versions.
4. Optionally prune stale `block_hash_to_num/*` when history pruning is enabled.

### 12.2 Guardrails

Track and enforce:

1. `orphan_chunk_bytes`
2. `orphan_manifest_segments`
3. `stale_tail_keys`

Policy when threshold exceeded:

1. Throttle ingest.
2. If still rising, fail closed and alert.

---

## 13. Observability

### 13.1 Metrics

1. Ingest latency per block.
2. Query latency p50/p95/p99.
3. Candidate bitmap cardinalities per clause.
4. Chunk seal rate and chunk size distribution.
5. Tail flush lag.
6. CAS retry counts.
7. Lease loss events.
8. GC backlog gauges.

### 13.2 Logging

Structured logs with:

1. `block_num`
2. `writer_epoch`
3. `state_version`
4. `stream_id` (hashed/truncated)
5. `query_id`

---

## 14. Testing Strategy

### 14.1 Unit Tests

1. Key encoding and ordering.
2. Chunk codec encode/decode round-trips.
3. Filter semantic edge cases.
4. Manifest CAS conflict handling.

### 14.2 Integration Tests

1. Sequential finalized ingest and queries.
2. Crash injection across ingest phase boundaries:
   - `logs/*`, `block_meta/*`, `block_hash_to_num/*`
   - `manifests/*`, `tails/*`, `chunks/*`
   - final `meta/state` CAS visibility barrier
3. Restart recovery with lazy load.
4. Lease loss fencing behavior.
5. Finality violation detection and degraded mode.

Current implementation includes restart-and-retry crash matrix tests plus repeated staged crash-loop tests proving:

1. Eventual successful commit after transient boundary failures.
2. No duplicate logical logs after retry.
3. No state/head corruption across retries.

### 14.3 Differential Tests

1. Compare query output against reference node for finalized ranges.
2. Include address OR and topic OR/null combinations.
3. Include blockHash queries.

### 14.4 Performance and Profiling Harness

1. Criterion micro/mid-scale suites:
   - ingest: `10`, `100`, `1000` logs/block variants
   - query: filtered, OR-list, and mixed workload
2. Standalone stress runner:
   - `cargo run --release -p finalized-log-index --example perf_stress -- ...`
   - configurable `blocks`, `logs-per-block`, `queries`, `chunk-size`, `max-results`
3. Profiling workflow:
   - run stress example under system profiler (`perf`, `dtrace`, `Instruments`) for flamegraphs
   - use query latency percentiles and ingest logs/sec emitted by the harness as baseline signals

---

## 15. Implementation Milestones

1. M1: Domain types, keyspace, codecs, store traits, in-memory backend.
2. M2: Finalized ingest path without chunk sealing (tail-only correctness).
3. M3: Chunk sealing + manifest CAS + query planner/executor.
4. M4: Recovery, GC, lease manager, degraded mode.
5. M5: Differential test suite and performance tuning.

---

## 16. Future Extensions (Not in This Crate)

1. Tip-window indexing and serving.
2. Finalized + tip query composition.
3. Multi-writer staged publish protocol.
4. Proof-friendly index formats.
