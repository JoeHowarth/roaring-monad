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
- `Done` `MetaState`/`BlockMeta`/`Log` binary codec and decode validation.
- `Done` Stream identity includes shard dimension.
- `Done` `ChunkRef` includes `{chunk_seq,min_local,max_local,count}`.
- `Partial` `topic0_stats/*` persistence exists, but rolling-window semantics are incomplete.

## 3. Chunking and Tail Lifecycle

- `Done` Tail persistence and reload.
- `Done` Seal-on-entry-count threshold.
- `Partial` Seal-on-bytes threshold not implemented.
- `Partial` Maintenance-seal interval not implemented.
- `Partial` Periodic tail flush timer not implemented as background task.

## 4. Query Semantics

- `Done` Address/topic OR/null matching and exact post-filter.
- `Done` `blockHash` mode with exclusivity validation.
- `Done` Finalized range clipping to indexed finalized head.
- `Done` Shard fan-out for log-level and block-level streams.
- `Done` `max_results` early stop.
- `Done` OR-term guardrail via `planner_max_or_terms`.
- `Partial` Cardinality estimation exists only implicitly; no explicit overlap-aware planner heuristic before materialization.
- `Partial` `query too broad` fallback policy to block-driven scan is not implemented (returns error only).

## 5. Ingest Correctness and Idempotency

- `Done` Sequential finalized ingest (`B == head + 1`).
- `Done` Parent linkage verification.
- `Done` State CAS barrier and deterministic keying.
- `Done` Tail re-append idempotency via roaring set semantics.
- `Partial` Lease/fencing enforcement is present in in-memory/fs stores, but no distributed lease backend integration.

## 6. Topic0 Hybrid Strategy

- `Done` Always-on `topic0_block` append path.
- `Done` Optional `topic0_log` path controlled by mode record.
- `Missing` Automatic hot/cold hysteresis transitions (`<0.1%` enable, `>1.0%` disable over 50k blocks).

## 7. Recovery and Degraded Mode

- `Done` Startup state recovery API.
- `Partial` Degraded mode currently surfaced as errors; no explicit service state machine with mode transitions.
- `Missing` Operator-runbook hooks/events for finality violation pathways.

## 8. GC and Guardrails

- `Done` Orphan chunk cleanup.
- `Done` Stale tail cleanup.
- `Partial` Guardrail metrics computed, but no automatic throttle/fail-closed wiring to ingest path.
- `Missing` `block_hash_to_num/*` pruning policy wiring when history pruning is enabled.

## 9. Backend-Agnostic Store Support

- `Done` Abstract `MetaStore` and `BlobStore` async traits.
- `Done` In-memory adapters.
- `Done` Filesystem adapters.
- `Missing` Production-grade distributed adapters (e.g., object store + CAS metadata service).

## 10. Testing Coverage

- `Done` Codec roundtrips.
- `Done` End-to-end ingest/query tests.
- `Done` Differential-style query tests vs naive scanner.
- `Done` Filesystem adapter integration test.
- `Done` GC/recovery behavior tests.
- `Missing` High-scale performance and contention tests.
- `Missing` Crash-injection tests at every ingest phase boundary.

## 11. Priority Next Steps

1. Implement full topic0 rolling-window hysteresis mode switching.
2. Add background tasks for periodic tail flush and maintenance seal.
3. Add explicit planner cardinality estimation based on overlap-aware `ChunkRef.count` before full set materialization.
4. Wire guardrail actions (throttle/fail-closed) into runtime control path.
5. Add benchmark suite and stress harness (ingest throughput, query latency p50/p95/p99, OR-list stress).
