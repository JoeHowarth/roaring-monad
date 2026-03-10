# Handoff Plan: Scylla CAS Contention During Distributed Ingest

## 1) Context

This repo is running an unattended scale loop that:

1. Pulls chain data from `aws mainnet-deu-009-0 50` using `monad-archive`.
2. Mirrors blocks/receipts/traces to local FS archive.
3. Ingests from FS archive into distributed finalized-log-index storage (`Scylla + MinIO`).
4. Repeats with new keyspaces while targeting large backend size growth.

Current orchestrator:

- `scripts/scale_to_target_size.sh`
- `scripts/watch_scale_loop.sh`

Core ingest path:

- `crates/benchmarking/src/main.rs` (`ingest-distributed`)
- `crates/finalized-log-index/src/ingest/engine.rs`
- `crates/finalized-log-index/src/store/scylla.rs`

## 2) Problem Statement

Ingest frequently fails on Scylla `LOCAL_SERIAL` CAS/LWT timeouts in hot metadata writes, especially under load and/or concurrent benchmark replay.

Observed failure signature:

- `scylla put if version ... consistency: LocalSerial ... write_type: Cas`
- Example log: `logs/scale-ingest-20260223T073907Z-scale-geom-i00029.log`

Operational behavior now:

- Scaler detects this and rolls to a new keyspace iteration.
- This improves liveness but does not remove the root bottleneck.

## 3) Root Cause Hypothesis

Hot ingest path performs high-frequency conditional writes (`PutCond::IfVersion` / `IfAbsent`) that map to Scylla LWT:

- state CAS (`store_state`)
- manifest CAS (`store_manifest`)
- topic0 stats CAS updates

Code locations:

- `crates/finalized-log-index/src/ingest/engine.rs`
- `crates/finalized-log-index/src/store/scylla.rs`

Given current workload assumptions (single writer per keyspace, no intentional concurrent writers), strict per-write LWT guarantees are stronger than required and are the dominant contention source.

## 4) Goal

Reduce or eliminate LWT/CAS on hot ingest path for single-writer scale workloads while preserving:

1. Correct finalized sequence semantics.
2. Recoverability and predictable behavior.
3. Ability to retain strict CAS mode for multi-writer or safety-critical deployments.

## 5) Proposed Solution

Introduce explicit ingest modes:

1. `strict_cas` mode (current semantics, default-safe).
2. `single_writer_fast` mode (reduced LWT in hot path).

### 5.1 Single-Writer Fast Mode Semantics

1. Keep one writer lease/fence check at startup or periodic intervals.
2. Replace hot-path CAS writes with unconditional writes where safe.
3. Retain finalized sequence/parent validation in engine logic.
4. Keep strict mode unchanged for environments requiring optimistic concurrency control.

### 5.2 Hot-Path Write Reductions

1. Avoid writing manifest when unchanged (no seal event).
2. Keep tail writes as non-LWT in fast mode.
3. Reduce topic0 stats write frequency (batch/debounce by block interval).
4. Keep block metadata writes non-LWT as they already are (`PutCond::Any`).

## 6) Implementation Plan

## Phase A: Configuration and Wiring

1. Add ingest mode/config flags in `crates/finalized-log-index/src/config.rs`.
2. Surface flags in `crates/benchmarking/src/main.rs` ingest-distributed CLI.
3. Pass flags from `scripts/scale_to_target_size.sh` for scale runs.

Acceptance:

1. CLI can run both modes.
2. Existing default behavior remains backward-compatible.

## Phase B: Engine Changes

1. Update `store_state` logic:
   - `strict_cas`: current CAS behavior.
   - `single_writer_fast`: unconditional write guarded by sequence checks already in `ingest_finalized_block`.
2. Update `store_manifest` similarly, and skip manifest write if not modified.
3. Update topic0 stats persistence:
   - optional periodic flush cadence in fast mode.
   - non-LWT write policy in fast mode.

Acceptance:

1. Ingest correctness tests pass in both modes.
2. No regressions in sequence/parent validation behavior.

## Phase C: Store and Retry Tuning

1. Keep existing retry/backoff defaults in Scylla store (`max_retries=10`, `base_delay_ms=50`, `max_delay_ms=5000`).
2. Add per-mode telemetry counters:
   - CAS attempts
   - CAS conflicts
   - timeout errors by operation

Acceptance:

1. Logs/metrics make contention source explicit per operation.

## Phase D: Validation and Rollout

1. Run formatting/lint/tests:
   - `cargo +1.91.1 fmt --all`
   - `cargo +1.91.1 clippy -p finalized-log-index --all-targets --all-features -- -D warnings`
   - `cargo +1.91.1 test -p finalized-log-index`
2. Run A/B ingest benchmark on same range:
   - strict mode vs fast mode
   - measure completion rate, timeout rate, blocks/sec, logs/sec.
3. Run scale loop with fast mode enabled and no concurrent benchmark replay.

Acceptance:

1. Significant reduction in CAS timeout frequency.
2. Higher sustained ingest progress before failure or full iteration completion.
3. No data model invariants violated in validation checks.

## 7) Suggested Concrete Changes (File Targets)

1. `crates/finalized-log-index/src/config.rs`
2. `crates/finalized-log-index/src/ingest/engine.rs`
3. `crates/finalized-log-index/src/store/scylla.rs` (telemetry/retry visibility)
4. `crates/benchmarking/src/main.rs` (CLI flags)
5. `scripts/scale_to_target_size.sh` (pass new ingest mode flags)
6. `crates/finalized-log-index/tests/*` (add mode-specific tests)

## 8) Risks and Mitigations

Risk: Lost updates if multiple writers hit same keyspace in fast mode.

Mitigation:

1. Keep strict mode default.
2. Enable fast mode only in controlled single-writer scaler jobs.
3. Add startup assertion/marker in keyspace metadata: writer id + mode.

Risk: Hidden correctness drift from reduced topic0 stats write frequency.

Mitigation:

1. Guard behind config.
2. Compare query output against strict mode on sample datasets.

## 9) Operational Guidance

1. Do not run benchmark replay concurrently with scaler ingest during throughput qualification.
2. Keep scaler keyspace rollover behavior enabled.
3. Keep mirror skipping on retry/rollover enabled to avoid unnecessary archive work.

## 10) Handoff Checklist

1. Implement Phases A-D in order.
2. Commit per phase with benchmark/log evidence in commit message/body.
3. Update `OVERNIGHT_EXECUTION_PLAN.md` and `archive-integration-findings.md` with:
   - mode used
   - validation command outputs
   - throughput and timeout deltas
4. Leave scaler running in chosen mode after verification.
