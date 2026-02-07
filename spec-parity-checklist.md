# Spec Parity Checklist

Source of truth: `spec-v2.md`  
Implementation target: `crates/finalized-log-index`

## Status Legend

- `Done`: Implemented and covered by tests.
- `Partial`: Implemented in part; significant behavior still missing.
- `Missing`: Not implemented.

## 1. Scope and Boundary

- `Done` Finalized-only ingest/query boundary.
- `Done` Tip-window integration out of scope in crate API.

## 2. Data Model and Persistence

- `Done` Canonical keys: `logs/*`, `block_meta/*`, `block_hash_to_num/*`, `meta/state`.
- `Done` Binary codecs with decode validation: `MetaState`, `BlockMeta`, `Log`, `Manifest`, `ChunkBlob`, tails.
- `Done` Stream identity includes shard dimension.
- `Done` `ChunkRef` includes `{chunk_seq,min_local,max_local,count}`.
- `Done` `topic0_mode/*` + `topic0_stats/*` persisted with versioned codecs.

## 3. Chunking and Tail Lifecycle

- `Done` Tail persistence/reload and append idempotency.
- `Done` Seal-on-entry-count threshold.
- `Done` Seal-on-bytes threshold.
- `Done` Maintenance seal based on elapsed time.
- `Done` Periodic maintenance pass that flushes tails and seals eligible streams.
- `Partial` No autonomous scheduler thread in crate; maintenance is invoked via service method.

## 4. Query Semantics

- `Done` Address/topic OR/null matching and exact post-filter.
- `Done` `blockHash` mode validation and lookup path.
- `Done` Finalized range clipping to indexed finalized head.
- `Done` Shard fan-out for log-level and block-level streams.
- `Done` Overlap-aware clause estimation from manifest chunk refs + tail overlap.
- `Done` Clause intersection ordering by estimated selectivity.
- `Done` `max_results` early stop.
- `Done` OR-term guardrail with configurable behavior:
  - `Error`
  - `BlockScan` fallback.

## 5. Ingest Correctness and Idempotency

- `Done` Sequential finalized ingest (`B == head + 1`).
- `Done` Parent linkage verification.
- `Done` State CAS barrier and deterministic keying.
- `Done` Tail re-append idempotency via roaring set semantics.
- `Partial` Lease/fencing semantics are enforced by in-memory/fs adapters but not by a distributed lease service.

## 6. Topic0 Hybrid Strategy

- `Done` Always-on `topic0_block` append path.
- `Done` Optional `topic0_log` path controlled by mode record.
- `Done` Rolling-window hysteresis transitions implemented and tested (`<0.1%` enable, `>1.0%` disable).

## 7. Recovery and Degraded Mode

- `Done` Startup recovery plan API from persisted state.
- `Done` Service degraded mode state + fail-closed behavior for finality-parent violations.
- `Done` Service throttle mode state for guardrail-triggered backpressure.
- `Done` Backend error circuit behavior: consecutive backend failures trigger throttle/degrade transitions.
- `Partial` Operator-runbook integration hooks are represented as state/messages; no external notifier integration.

## 8. GC and Guardrails

- `Done` Orphan chunk cleanup.
- `Done` Stale tail cleanup.
- `Done` Guardrail exceedance detection.
- `Done` Configurable guardrail action (`Throttle`/`FailClosed`) wired into service state.
- `Done` `block_hash_to_num/*` pruning hook (`prune_block_hash_index_below`).
- `Partial` No background GC scheduler in crate; GC is invoked via service method.

## 9. Backend-Agnostic Store Support

- `Done` Abstract `MetaStore` and `BlobStore` async traits.
- `Done` In-memory adapters.
- `Done` Filesystem adapters.
- `Done` Feature-gated distributed adapters:
  - `ScyllaMetaStore` (`distributed-stores` feature) for metadata + CAS/fencing.
  - `MinioBlobStore` (`distributed-stores` feature) for blob storage.

## 10. Testing Coverage

- `Done` Codec roundtrips and version compatibility paths.
- `Done` End-to-end ingest/query tests (blockHash, limits, OR-guardrail, fallback scan).
- `Done` Differential-style query tests vs naive scanner.
- `Done` Filesystem adapter integration test.
- `Done` Topic0 hysteresis enable/disable tests.
- `Done` Maintenance sealing tests (bytes + periodic time-based pass).
- `Done` Degraded/throttle and GC guardrail tests.
- `Done` Pruning hook test.
- `Done` Crash-injection matrix covering ingest write/CAS boundaries with restart-and-retry validation.
- `Done` Repeated staged crash-loop test confirms eventual commit and no duplicate/corrupt state.
- `Done` Larger-scale benchmark expansion (`ingest/1000`, `query_mixed_large`) and standalone stress harness example (`examples/perf_stress.rs`).
- `Partial` Distributed adapter integration tests against live Scylla/MinIO are not yet automated in CI.

## 11. Priority Remaining Work

1. Add autonomous scheduler integration for maintenance + GC loops (optional feature flag).
2. Add flamegraph/profiler automation and CI artifact retention for hotspot tracking.
3. Expand dependency chaos tests (network partitions, long outage, retry-budget exhaustion).
