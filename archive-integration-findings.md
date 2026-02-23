# Archive Receipt Integration Findings

## 1) Fetching receipts from `~/monad-bft/monad-archive`

The reliable way to mirror receipts from AWS archive bucket `mainnet-deu-009-0` onto local filesystem is `monad-archiver` with:

- source: `--block-data-source 'aws mainnet-deu-009-0 50'`
- sink: `--archive-sink 'fs <path>'`

Important behavior:

- `monad-archiver` tracks progress with sink key `latest`.
- To start from block `S`, set sink marker to `S-1` using subcommand:
  - `monad-archiver set-start-block --block <S-1> --archive-sink 'fs <path>'`
- For finite runs, use `--stop-block <E>`.
- Edge case: `stop_block=0` loops forever because block `0` does not checkpoint `latest` (new_latest_uploaded stays `0`). Work around by starting from `>= 1`.

## 2) Local FS layout produced by archive sink

`BlockDataArchive` uses these logical keys:

- `block/<12-digit-block-num>`
- `receipts/<12-digit-block-num>`
- `traces/<12-digit-block-num>`
- `block_hash/<hex-hash>`
- `latest`

With fs sink `fs /path/to/archive`, these are files under `/path/to/archive/blocks/...` because `FsCliArgs::block_store_path()` maps to `<root>/blocks`.

So receipts are physically at:

- `<root>/blocks/receipts/000123456789`

The values are encoded (RLP + monad-archive storage representation), not JSON.

## 3) Reading mirrored receipts programmatically

Use `BlockDataArchive` from `monad-archive` over `FsStorage`:

1. Create `FsStorage` rooted at `<root>/blocks`.
2. Wrap in `BlockDataArchive::new`.
3. Read by block:
   - `get_block_by_number(block_num)`
   - `get_block_receipts(block_num)`

`get_block_receipts` returns `Vec<ReceiptWithLogIndex>` where each entry has:

- `receipt: ReceiptEnvelope`
- `starting_log_index: u64`

Logs are available from `receipt.logs()`.

## 4) Mapping to `log-workload-gen`

`log-workload-gen` ingestion accepts `Message::ChainEvent(ChainEvent)` and `EndOfStream`.

Per block mapping:

- `ChainEvent.chain_id`: mainnet chain id (currently `1` in this repo defaults)
- `ChainEvent.block_number`: archive block number
- `ChainEvent.block_hash`: archive block hash bytes
- `ChainEvent.timestamp`: archive block timestamp
- `ChainEvent.logs[]`:
  - `tx_index`: receipt index in block
  - `log_index`: `starting_log_index + intra-receipt-log-index`
  - `address`: 20-byte address
  - `topics`: list of 32-byte topics

Then call:

- `run_collect(...)` or `run_collect_and_generate(...)`

Artifacts produced in dataset dir:

- `dataset_manifest.json`
- `key_stats.parquet`
- `cooccurrence.parquet`
- `range_stats.parquet`
- `trace_expected.jsonl`, `trace_stress.jsonl`, `trace_adversarial.jsonl` (when generating)

## 5) Mapping to `finalized-log-index` ingestion

`FinalizedIndexService::ingest_finalized_block` expects strictly contiguous finalized blocks.

Per block mapping:

- `Block.block_num`: archive block number
- `Block.block_hash`: archive block hash
- `Block.parent_hash`: archive parent hash
- `Block.logs[]` (flatten all receipt logs):
  - `address`, `topics`, `data`
  - `block_num`, `block_hash`
  - `tx_idx`: receipt index in block
  - `log_idx`: `starting_log_index + intra-receipt-log-index`

For distributed backend in this repo:

- metadata: `ScyllaMetaStore`
- blobs: `MinioBlobStore`

## 6) End-to-end flow that works for this workspace

1. Mirror archive range from AWS to local fs (`monad-archiver`).
2. Read mirrored data and convert into:
   - workload-gen `Message` stream
   - finalized-log-index `Block` stream
3. Generate traces/artifacts with `log-workload-gen`.
4. Ingest same blocks into distributed finalized index (Scylla + MinIO).
5. Replay generated traces as benchmark workload against finalized index query API.
6. Capture throughput/latency + backend size growth until ~100GB target.

## 7) Executed validation run (2026-02-23 UTC)

Environment:

- AWS source: `aws mainnet-deu-009-0 50`
- Block window: `57,212,000..57,212,500` (501 blocks)
- Archive root: `/home/jhow/roaring-monad/data/archive-mainnet-deu-009-0`
- Distributed backend: docker compose in `infra/docker-compose.distributed.yml`

Observed output:

- Mirror:
  - `mirror_done start_block=57212000 end_block=57212500`
- Collect/generate:
  - `blocks_seen=501`
  - `logs_seen=13354`
  - traces generated: expected/stress/adversarial `20000` each
- Ingest (dev):
  - `blocks_per_second=5.27`
  - `logs_per_second=140.41`
- Benchmark (dev, expected profile, trace-limit=100):
  - `qps=1.04`
  - `p50=12.3ms`
  - `p95=5.28s`
  - `p99=5.61s`

## 8) Profiling + optimization findings

The main optimization lever with immediate impact is running the benchmarking crate in `--release`.

Release vs dev benchmark (same dataset/keyspace, expected profile, trace-limit=100):

- dev:
  - `qps=1.04`
  - `p50=12.3ms`
  - `p95=5.28s`
- release:
  - `qps=2.30`
  - `p50=5.1ms`
  - `p95=2.47s`

Release ingest tuning sweeps:

- baseline (`entries=1950`, `chunk_bytes=32768`, maintenance every 500):
  - `blocks_per_second=6.15`
  - `logs_per_second=163.99`
- tuned (`entries=8000`, `chunk_bytes=262144`, maintenance every 2000):
  - `blocks_per_second=6.72`
  - `logs_per_second=179.02`
  - benchmark qps on this layout dropped to `2.01`
- balanced (`entries=4000`, `chunk_bytes=65536`, maintenance every 1000):
  - `blocks_per_second=6.51`
  - `logs_per_second=173.44`
  - benchmark qps `2.01`
- maintenance-only tuning (`entries=1950`, `chunk_bytes=32768`, maintenance every 2000):
  - `blocks_per_second=6.73`
  - `logs_per_second=179.50`
  - benchmark qps `2.02`

Current best tradeoff observed:

- For fastest ingest: maintenance-tuned release config (`6.73 blocks/s`).
- For best query latency/QPS: baseline release chunking (`2.30 qps` on tested sample).

Perf counter captures are written per run under `logs/*.perf.txt`.

## 9) Scaling to ~100GB

A resumable scaling script is available:

- `scripts/scale_to_target_size.sh`

What it does:

1. Repeatedly ingests a mirrored receipt range into unique keyspace/prefix namespaces.
2. Measures on-disk backend size (`infra/data/distributed/scylla` + `infra/data/distributed/minio`).
3. Appends growth rows to `logs/results/size-growth-<RUN_ID>.md`.
4. Writes resumable state to `logs/scale-state-<RUN_ID>.env`.

The script is intended for unattended overnight execution until the target byte threshold is reached.

## 10) Density scan results (coarse, AWS source)

A coarse scan was run directly against AWS receipts:

- command:
  - `benchmarking scan-density --source 'aws mainnet-deu-009-0 50' --start-block 56000000 --end-block 57220000 --step 2000 --top 30`
- sampled blocks: `611`
- average logs/sample: `27.10`
- max logs/sample: `155`

Top sampled blocks by logs (coarse step) include:

- `56094000` (`155` logs)
- `56890000` (`136` logs)
- `56662000` (`130` logs)
- `56542000` (`130` logs)
- `56968000` (`120` logs)

Implication:

- In this scanned region, logs per block are relatively modest.
- Hitting ~100GB from only finalized-log-index log ingestion is primarily a throughput/time problem, not a single-hot-range selection problem.

Follow-up fine-grained scans used to retarget overnight scaling:

- `56536000..56550000` (`step=10`):
  - avg logs/sample: `39.50`
  - max sampled: `296`
  - output: `logs/results/scan-density-20260223T0909Z-window56542-step10.json`
- `56656000..56670000` (`step=10`):
  - avg logs/sample: `46.40`
  - max sampled: `253`
  - output: `logs/results/scan-density-20260223T0932Z-window56662-step10.json`
- `56885000..56898000` (`step=10`):
  - avg logs/sample: `20.93`
  - max sampled: `684` (spiky outliers, lower sustained density)
  - output: `logs/results/scan-density-20260223T0909Z-window56890-step10.json`

Resulting scaler default retarget:

- `START_BLOCK=56656000`
- `END_BLOCK=56656500`

This keeps geometric scaling in a denser sustained region and improves bytes-per-hour accumulation.

## 11) Mirror-path optimization (archiver vs sequential mirror)

A direct mirror speed test showed large improvement by using `monad-archiver` instead of per-block sequential mirror calls:

- sequential mirror path (`benchmarking mirror`): ~`90s` for ~`501` blocks in prior runs.
- `monad-archiver` path (`set-start-block` + `--max-concurrent-blocks`): ~`5s` observed for a 501-block test window.

Operational caveat:

- `monad-archiver --stop-block` may archive up to roughly one batch beyond the requested stop block (bounded by `--max-blocks-per-iteration`), then stop.
- This is acceptable for scaling flows but causes overlap and `NoClobber` skip warnings on later mirror passes.

## 12) Scylla profile reconfiguration and impact

Scylla compose profile was increased from `--smp 1 --memory 1G` to `--smp 8 --memory 24G`.

Control ingest test (`57212000..57212500`, release settings):

- pre-change (best comparable release run): ~`6.73 blocks/s` (maintenance-tuned)
- post-change control run: ~`6.21 blocks/s`

Conclusion:

- For this workload/shape, ingest is not materially improved by just increasing Scylla shards/memory.
- Dominant constraints are likely client-side/write pattern and per-block service path overhead, not only Scylla CPU allocation.

## 13) Ingest-path code optimizations and observed impact

Additional ingest throughput work was applied in code:

- `ScyllaMetaStore::put` was optimized to remove unnecessary per-write readbacks:
  - dropped pre-read and post-read around writes,
  - reduced `IfVersion` update to use `v+1` directly,
  - switched fence checks to a short periodic refresh window instead of querying fence table for every write.
- `IngestEngine` now caches `topic0_mode` lookups in memory to avoid repeated metadata `get` calls for the same topic signatures.
- `IngestEngine` now also caches `topic0_stats` in memory and uses a fast path for long block gaps:
  - avoids repeated per-signature stats reads from metadata store on every block,
  - avoids iterating every skipped block when a signature was absent for long stretches.
- ingest block write path now uses bounded async parallelism:
  - log-locator writes: concurrency `128`,
  - stream-appends: concurrency `64`,
  to reduce time spent waiting on Scylla/MinIO round trips.
- Benchmarking ingest command added `--skip-final-maintenance`; scaler can now run with:
  - `--run-maintenance-every-blocks 0`
  - `--skip-final-maintenance`
  to avoid expensive end-of-iteration maintenance sweeps during bulk size-growth runs.
- scaler resilience hardened with bounded retry controls:
  - `MAX_RETRIES_PER_ITER`
  - `RETRY_DELAY_SECONDS`
  so transient mirror/ingest failures do not terminate unattended overnight execution immediately.
- scaler now queries source head each iteration (`benchmarking source-latest`) and caps ingest range to available blocks.
  - this prevents archiver from waiting indefinitely when geometric span overshoots current chain head.
  - on low headroom near the source tip, scaler wraps start back to `START_BLOCK` so the run can continue accumulating DB size instead of idling on tiny tip windows.

Observed in live scale run after relaunch (`iter=5`, span `8016`):

- early ingest progress reached about `9.99 blocks/s` and `323.36 logs/s` (`mapped_block=486`, `elapsed=48.63s`).
- completed ingest for the full span finished at `11.24 blocks/s` and `331.97 logs/s` (`8016` blocks, `236702` logs, `713.03s`).
- prior comparable progress points in the previous iteration (`iter=4`) were around `~6.8-7.2 blocks/s` and `~175-182 logs/s`.
- current long-span run (`iter=23`, `57212000..57233795`) is sustaining roughly `12-13 blocks/s` and `~333-350 logs/s` in-progress samples.
  - completed at `11.74 blocks/s`, `334.45 logs/s`, total backend `7.81 GiB`.
- subsequent near-head run (`iter=25`, `57233796..57238906`) completed at:
  - `17.06 blocks/s`
  - `456.89 logs/s`
  - total backend `7.91 GiB`
  indicating a clear ingest throughput uplift after bounded write-concurrency changes.

Live perf-stat sample on active ingest process (10s window):

- CPU utilization: `~0.214` cores used by process
- instructions per cycle: `~0.67`
- branch miss rate: `~10.1%`
- interpretation: ingest remains primarily I/O / remote-call limited (not CPU-saturated), so bounded write concurrency is the right next optimization direction.

Query-path trial optimization:

- executor now parallelizes per-stream postings loads for clause unions (bounded concurrency) to target OR/multi-stream trace patterns.
- in expected-profile under-load replay (`trace-limit=100` on active ingest keyspace), observed QPS remained approximately flat (`1.61 -> 1.60`), suggesting this change is neutral on simple filters and more relevant to broader OR workloads.

Interpretation:

- write-path round-trip reduction and maintenance deferral materially improved ingest headroom for scale-out.
- this directly shortens time-to-target for the ~100GB backend build.

## 14) Post-optimization continuation: keyspace rollover + CAS-timeout handling

Further reliability/performance tuning was applied during continued overnight execution:

- scaler keyspace-rollover handling:
  - new state flag `FORCE_SKIP_MIRROR` allows skipping mirror when the next iteration is only a keyspace rollover for the same block range.
  - avoids repeated long mirror phases after keyspace conflicts.
- scaler ingest-failure handling:
  - added fast path for CAS timeout partial writes (`scylla put if version` / `LocalSerial`/`Cas` patterns).
  - instead of retrying same keyspace (which then fails with invalid sequence), scaler advances immediately to a new keyspace iteration.
- store/ingest tuning:
  - `ScyllaMetaStore` default retry profile increased to `max_retries=10`, `base_delay_ms=50`, `max_delay_ms=5000`.
  - `IngestEngine` stream-append concurrency reduced from `64` to `32` to lower LWT pressure.

Validation:

- `cargo +1.91.1 fmt --all`
- `cargo +1.91.1 clippy -p finalized-log-index --all-targets --all-features -- -D warnings`
- `cargo +1.91.1 test -p finalized-log-index`

Observed behavior after deployment:

- `iter=29` progressed materially farther than earlier failing attempts:
  - reached around `raw_block=56542178` (about `6001` blocks ingested in run logs) before CAS timeout.
  - earlier repeated failures were around `~3000` ingested blocks.
- on failure, new fast path triggered exactly as intended:
  - `scale ingest partial-write timeout iter=29 rc=1; retrying range with new keyspace iter=30`
  - this removed the prior extra invalid-sequence retry cycle and reduced wasted time between attempts.

Updated benchmark evidence (under active ingest load):

- command used scaler env (`TRIEDB_TARGET=triedb_driver`, GCC15/CFLAGS haswell) for successful release build/run.
- result JSON:
  - `logs/results/benchmark-20260223T073907Z-scale-geom-i00025-expected-limit100-post-cas-tune.json`
- output summary:
  - `qps=2.13`, `p50_us=4909`, `p95_us=2517654`, `p99_us=2820084`, `max_us=2888019`.

New commits:

- `727967f` Skip mirror after keyspace-conflict iteration rollover.
- `38e51d6` Harden ingest loop against CAS timeout churn.
