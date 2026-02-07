# Finalized Log Index Production Readiness Target

Status: Aspirational target
Scope: `crates/finalized-log-index` with distributed backend pair (`ScyllaMetaStore` + `MinioBlobStore`)

## 1. Purpose

This document defines what "production-ready" means for the finalized log index system and the evidence required before production rollout.

It is intentionally strict: passing this document should imply confidence under real concurrency, failures, and sustained load.

## 2. Non-Negotiable Invariants

These must hold in all environments.

1. Single canonical writer semantics.
2. `meta/state` remains the visibility barrier for index/log publication.
3. No committed block can produce duplicate or missing query-visible logs.
4. Crash/restart is idempotent.
5. Queries are snapshot-consistent against finalized head.
6. Any invariant violation fails closed (degraded mode), never silent corruption.

## 3. Production Readiness Definition

Production-ready means all of the following are true.

1. Correctness
- Concurrency-safe CAS/fencing semantics are proven on real Scylla/MinIO.
- Crash/retry and restart/recovery matrix passes under fault injection.

2. Reliability
- Service behavior under dependency failures is deterministic.
- Backpressure/degraded transitions are automatic and observable.

3. Performance
- Meets explicit SLOs on representative hardware and data shape.
- Sustained soak tests show stable latency, throughput, and storage growth.

4. Operability
- Metrics, dashboards, alerts, and runbooks cover critical failure modes.
- Rollout, rollback, and schema migration procedures are tested.

5. Security
- TLS, authN/authZ, and secret handling are production-grade.

## 4. Current Gap Summary

Current implementation is structurally correct but not yet production-ready because:

1. Scylla conditional write paths are currently emulated by read-check-write in adapter logic, not true LWT-backed serialization under contention.
2. Distributed integration tests cover only happy path; no adversarial multi-writer/failure matrix.
3. Retry/circuit-breaker policy and dependency-level failure budgeting are incomplete.
4. Long-duration soak and production-shape perf characterization are incomplete.
5. Operational runbooks and alert thresholds are not yet formalized.

## 5. Required End-State (Must Have)

## 5.1 Distributed Correctness

1. True CAS semantics in `ScyllaMetaStore`
- `IfAbsent` and `IfVersion` are backed by LWT guarantees (`IF` conditions) with deterministic applied/not-applied handling.
- Fence checks are LWT-safe and writer epoch semantics are linearizable enough for single-writer guarantees.

2. Publish order remains strict
- Blob write first (`MinIO`), manifest/state CAS after.
- Failed CAS cannot expose partial canonical state.

3. Multi-writer safety proof via tests
- Two+ writers racing on same stream/state cannot violate invariants.
- Losing writer cannot advance canonical head nor publish conflicting manifest/state.

## 5.2 Failure Behavior

1. Dependency outage behavior
- Scylla down: ingest blocks safely, query behavior explicit, health reflects degraded/throttled state.
- MinIO down: ingest blocks safely before visibility barrier, no partial publication.

2. Retry discipline
- Exponential backoff + jitter for transient backend failures.
- Bounded retries and circuit-breaking to avoid thundering herds.

3. Fail-closed policy
- Corruption, inconsistent metadata, or finality invariant failures move service to degraded mode automatically.

## 5.3 Performance and Capacity

1. Define production SLOs
- Query p50/p95/p99 latency targets by query class.
- Ingest logs/sec and blocks/sec targets.
- Recovery startup and warmup targets.

2. Prove targets under representative load
- High cardinality topics/addresses.
- Broad OR-list and selective filters.
- Concurrent ingest + query mix.

3. Capacity model
- Storage growth model for locators, manifests, tails, chunks, and log packs.
- GC backlog and cleanup budget model.

## 5.4 Operability

1. Metrics must exist and be wired
- CAS attempts/conflicts/applied counts.
- Fence rejections / lease-loss events.
- Ingest/query latency histograms.
- MinIO and Scylla error classes.
- GC backlog gauges and cleanup rates.

2. Alerting and runbooks
- Alerts for degraded/throttled state, CAS conflict spikes, lag growth, orphan growth, and backend errors.
- Runbooks for lease loss, finality violation, corruption, and backend partial outage.

3. Upgrade and migration safety
- Schema migration plan for Scylla keyspace/table changes.
- Backward/forward codec compatibility and rollback guidance.

## 5.5 Security

1. Backend comms over TLS in production profile.
2. Credentials sourced from secret manager, never hardcoded.
3. Least-privilege credentials for Scylla and object store.
4. Auditability for administrative actions.

## 6. Verification Matrix (Exit Criteria)

All checks below must pass in CI (or controlled pre-prod pipeline where appropriate).

## 6.1 Unit/Property

1. Codec roundtrip + version compatibility.
2. Key ordering and shard fan-out logic.
3. Planner selectivity/order correctness.

## 6.2 Integration (Single-process)

1. Existing ingest/query/recovery/GC suites pass.
2. Crash-injection matrix across write and CAS boundaries passes.

## 6.3 Distributed Integration (Docker)

1. Happy-path Scylla+MinIO roundtrip.
2. Restart between ingest and query.
3. MinIO transient failure during ingest (pre/post blob write).
4. Scylla transient failure during manifest/state CAS.
5. Concurrent writer contention scenarios.

## 6.4 Soak and Performance

1. 24h soak: sustained ingest + mixed query.
2. No memory leak trend beyond threshold.
3. p99 latency and error budget within SLO.
4. GC backlog bounded and recovers after bursts.

## 7. Release Gates

A version is production-eligible only if all gates are green.

Gate A: Correctness
- Distributed CAS/fencing semantics verified.
- Multi-writer adversarial tests pass.

Gate B: Reliability
- Failure-injection suite passes.
- Degraded/throttle behavior validated.

Gate C: Performance
- SLO benchmark suite passes with margin.
- Soak stability report approved.

Gate D: Operability
- Dashboard + alerts + runbooks reviewed.
- On-call dry run complete.

Gate E: Security
- TLS + secrets + permission model reviewed and approved.

## 8. Phased Plan to Reach Readiness

## Phase 1: Correctness Core (P0)

1. Replace emulated conditional logic in `ScyllaMetaStore` with robust LWT result handling.
2. Add explicit integration tests for CAS conflict and multi-writer races.
3. Add restart-after-partial-failure scenarios on distributed backends.

Done when:
- No known correctness caveats remain in distributed adapter path.

## Phase 2: Failure Hardening (P0)

1. Implement standardized retry/backoff policy.
2. Add circuit-breaker and backend health state transitions.
3. Expand distributed fault matrix in CI.

Done when:
- Dependency failures are predictable, bounded, and observable.

## Phase 3: Performance + Capacity (P1)

1. Build production-shape benchmark profiles.
2. Run soak and collect regression baselines.
3. Tune chunk/tail/GC parameters with evidence.

Done when:
- SLOs are met consistently and documented.

## Phase 4: Operability + Security (P1)

1. Finalize alerts and runbooks.
2. Verify rollout/rollback and schema migration path.
3. Complete TLS and secret-manager deployment profile.

Done when:
- On-call and security sign-off complete.

## 9. Explicit "Not Ready" Conditions

Do not ship to production if any are true.

1. Conditional CAS behavior on distributed store is emulated rather than guaranteed.
2. Multi-writer contention tests are missing or flaky.
3. No successful soak run at target load.
4. No runbook/alert coverage for degraded mode.
5. Known corruption path without automatic fail-closed response.

## 10. Evidence Artifacts Required

For production sign-off, attach:

1. Test report for all matrix categories.
2. Soak/perf report with raw data and trend plots.
3. Incident runbook links and on-call drill notes.
4. Security review summary.
5. Release checklist signed by owners.

## 11. Ownership

1. Storage adapter correctness: backend/infrastructure owner.
2. Query/ingest correctness: index core owner.
3. Reliability/performance SLOs: platform owner.
4. Runbooks/alerts: operations owner.

