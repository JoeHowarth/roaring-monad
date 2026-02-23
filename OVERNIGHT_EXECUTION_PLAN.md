# Overnight Execution Plan (Archive -> Workload -> Distributed Ingest -> Benchmark)

## Resume Contract

When context compacts, re-open this file first and continue from the first unchecked step.

- Keep all run artifacts under `./logs/` and `./data/`.
- Append progress to `./logs/progress.log`.
- Commit at each completed milestone.

## Working Variables

```bash
export RM_ROOT=/home/jhow/roaring-monad
export MA_ROOT=/home/jhow/monad-bft/monad-archive
export ARCHIVE_SRC="aws mainnet-deu-009-0 50"
export ARCHIVE_FS_ROOT="$RM_ROOT/data/archive-mainnet-deu-009-0"
export ARCHIVE_FS_BLOCKS="$ARCHIVE_FS_ROOT/blocks"
export LOG_DIR="$RM_ROOT/logs"
export DATA_DIR="$RM_ROOT/data"
export RESULTS_DIR="$RM_ROOT/logs/results"
export RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$LOG_DIR" "$DATA_DIR" "$RESULTS_DIR"
```

## Milestone A: Tooling + Docs

- [x] Write archive/workload/ingest findings (`archive-integration-findings.md`).
- [x] Write this executable plan file (`OVERNIGHT_EXECUTION_PLAN.md`).
- [x] Implement a benchmarking/ops runner crate in this workspace that can:
  - mirror archive receipts by block range (or consume pre-mirrored fs archive),
  - emit workload-gen dataset + traces,
  - ingest into finalized-log-index distributed backend,
  - replay traces as benchmark workload,
  - emit throughput/latency and backend size reports.
- [ ] Commit Milestone A.

Commit command:

```bash
git add archive-integration-findings.md OVERNIGHT_EXECUTION_PLAN.md crates Cargo.toml Cargo.lock .gitignore
git commit -m "Add archive integration findings, overnight plan, and initial e2e runner tooling"
```

## Milestone B: Distributed Backend Bootstrap (Scylla + MinIO)

- [x] Start persistent Scylla + MinIO (bind mounts in `infra/data/distributed/*`).
- [x] Verify health and connectivity from Rust runner.
- [x] Capture startup logs to `logs/distributed-startup-$RUN_ID.log`.

Suggested commands:

```bash
cd "$RM_ROOT"
docker compose -f infra/docker-compose.distributed.yml up -d | tee -a "$LOG_DIR/distributed-startup-$RUN_ID.log"
docker ps | tee -a "$LOG_DIR/distributed-startup-$RUN_ID.log"
```

## Milestone C: Finite Validation Run (small range)

Choose finite validation window (example):

```bash
export START_BLOCK=57212000
export END_BLOCK=57214000
```

- [ ] Set fs archive start marker safely (start-1) using `monad-archiver set-start-block`.
- [x] Mirror `[START_BLOCK..END_BLOCK]` from `mainnet-deu-009-0` to fs sink.
- [x] Run e2e runner on mirrored range:
  - build dataset/traces,
  - ingest to distributed finalized index,
  - run benchmark replay.
- [x] Validate output artifacts exist and contain non-trivial counts.
- [ ] Commit Milestone C.

Archiver commands:

```bash
cd /home/jhow/monad-bft
cargo run -p monad-archive --bin monad-archiver -- \
  set-start-block --block $((START_BLOCK-1)) --archive-sink "fs $ARCHIVE_FS_ROOT"

cargo run -p monad-archive --bin monad-archiver -- \
  --block-data-source "$ARCHIVE_SRC" \
  --archive-sink "fs $ARCHIVE_FS_ROOT" \
  --max-blocks-per-iteration 200 \
  --max-concurrent-blocks 32 \
  --stop-block "$END_BLOCK" \
  2>&1 | tee "$LOG_DIR/archive-mirror-$RUN_ID.log"
```

## Milestone D: Scale-out to ~100GB Database

- [x] Measure backend size after each ingest batch.
- [ ] Expand range iteratively until combined backend size reaches ~100GB.
- [ ] Record range -> size -> throughput table in `logs/results/size-growth-$RUN_ID.md`.

Size checks (example):

```bash
docker exec finalized-index-scylla sh -lc 'du -sb /var/lib/scylla || true'
docker exec finalized-index-minio sh -lc 'du -sb /data || true'
```

Range scaling strategy:

1. Start from validated range.
2. Increase block span geometrically (x2) while monitoring ingest stability.
3. If errors increase, reduce concurrency and continue.
4. Stop once combined size is ~100GB (+/-10%).

## Milestone E: Benchmark + Profile + Optimize

- [x] Run baseline benchmark from generated traces and store JSON + markdown summary.
- [x] Capture CPU profile during ingestion and query replay.
- [x] Apply targeted optimizations.
- [x] Re-run benchmark and compare baseline vs optimized.
- [ ] Commit optimization changes and benchmark evidence.

Optimization focus order:

1. Ingest throughput bottlenecks (serialization, per-log writes, excessive round trips).
2. Query latency bottlenecks from trace replay hot paths.
3. Batch sizing and chunk/tail parameters for better write amplification.

## Required Validation Before Final Claim

- [x] `cargo fmt --all`
- [x] `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- [x] `cargo test -p <changed_crate>` for each changed crate
- [x] Benchmark rerun after optimization changes

## Progress Journal

Append one line per major event:

```bash
echo "$(date -u +%FT%TZ) :: <event>" | tee -a "$LOG_DIR/progress.log"
```

## Current State Snapshot

- `mainnet-deu-009-0` AWS source access: verified.
- `monad-archiver stop_block=0` behavior: loops on block 0 due missing checkpoint update at zero; avoid with non-zero start block.
- Finite e2e run completed on `57,212,000..57,212,500` (mirror + collect/generate + ingest + benchmark).
- Release optimization evidence captured:
  - benchmark (expected, limit=100): dev `1.04 qps` -> release `2.30 qps`.
  - ingest: release baseline `6.15 blocks/s`; maintenance-tuned `6.73 blocks/s`.
- Size baseline after completed runs: `infra/data/distributed` ~`1.4G`.
- Next immediate action: run unattended scale loop to 100GB target:

```bash
RUN_ID=$(cat logs/latest-run-id.txt) \
TARGET_GB=100 \
START_BLOCK=57212000 \
END_BLOCK=57212500 \
nohup ./scripts/scale_to_target_size.sh \
  > logs/scale-to-target-$RUN_ID.out 2>&1 &
```
