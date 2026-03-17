# Optimization Log

## 2026-02-23T19:40:00Z - Baseline Before New Optimization Commits

### Context

- Ingest mode: `single-writer-fast`
- Archive source: local FS mirror at `data/archive-mainnet-deu-009-0`
- Objective: identify post-CAS bottleneck before next round of optimizations

### Hypothesis

- After removing most CAS contention, ingest is likely latency-bound on Scylla request/response round-trips.

### Commands

```bash
# Throughput + system utilization sample (10k-block window)
target/release/benchmarking ingest-distributed \
  --archive-root /home/jhow/roaring-monad/data/archive-mainnet-deu-009-0 \
  --start-block 56536000 --end-block 56546000 \
  --chain-id 1 \
  --scylla-node 127.0.0.1:9042 --scylla-keyspace fi_profile_20260223t193113z \
  --minio-endpoint http://127.0.0.1:9000 --minio-region us-east-1 \
  --minio-access-key minioadmin --minio-secret-key minioadmin \
  --minio-bucket finalized-index-bench --minio-prefix runs/profile/20260223T193113Z \
  --writer-epoch 1 \
  --target-entries-per-chunk 1950 --target-chunk-bytes 32768 \
  --maintenance-seal-seconds 1800 --run-maintenance-every-blocks 0 \
  --ingest-mode single-writer-fast \
  --topic0-stats-flush-interval-blocks 64 \
  --log-every 500 --skip-final-maintenance \
  --output-json logs/results/review-profile-20260223T193113Z.json

# CPU counter/stall profile (4k-block window)
perf stat -d -- target/release/benchmarking ingest-distributed ... \
  --start-block 56536000 --end-block 56540000

# Stack sampling (2k-block window)
perf record -F 99 -g --call-graph dwarf -- target/release/benchmarking ingest-distributed ... \
  --start-block 56536000 --end-block 56538000
perf report --stdio --call-graph none --sort comm,dso,symbol
```

### Metrics

- `logs/results/review-profile-20260223T193113Z.json`
  - `blocks_per_second`: `89.20`
  - `logs_per_second`: `3517.68`
  - `elapsed_seconds`: `112.12`
- `logs/profile-pidstat-20260223T193113Z.log` averaged:
  - `scylla`: `182.83% CPU`
  - `benchmarking`: `67.35% CPU`
  - `minio`: `13.16% CPU`
- `logs/profile-iostat-20260223T193113Z.log` averaged:
  - `nvme0n1 util`: `10.85%` (not disk-bound)
- `logs/profile-perfstat-20260223T193429Z.log`:
  - benchmark process `0.758 CPUs utilized`
  - high context switches (`40.399 K/sec`)
- `logs/profile-perfreport-20260223T193534Z.txt` hot symbols:
  - `__x64_sys_sendto`, `tcp_sendmsg`, `__x64_sys_recvfrom`, `__x64_sys_epoll_wait`, `__x64_sys_futex`

### Interpretation

- Bottleneck is Scylla write/read round-trip latency and coordination overhead, not local disk bandwidth or MinIO throughput.
- The main optimization direction is to reduce Scylla round-trips and hot partitions.

### Methodology Learnings

- Use fixed block windows and fresh keyspaces to keep runs comparable.
- Always capture both throughput and wait-state evidence (`pidstat` + `perf stat` + `perf record`) before attributing root cause.
- Local FS archive removes AWS variability; this is useful for isolating ingest/index/store bottlenecks.

## 20260223T195116Z - Profiling Run script-smoke

### Commands

`scripts/profile_ingest.sh`

### Metrics

- profile_result run_id=20260223T195116Z-script-smoke blocks=101 logs=3412 elapsed_s=1.25385196 bps=80.55177423018903 lps=2721.2143928059895 mode=single_writer_fast flush=64
- pid=1052533 cmd=benchmarking samples=1 avg_cpu=18.00 avg_wait=1.00 avg_usr=10.00 avg_sys=8.00
- pid=213044 cmd=minio samples=1 avg_cpu=15.00 avg_wait=0.00 avg_usr=10.00 avg_sys=5.00
- pid=213248 cmd=scylla samples=1 avg_cpu=225.00 avg_wait=5.00 avg_usr=108.00 avg_sys=117.00
- nvme0n1 samples=2 avg_util=14.01 avg_aqu=1.00 avg_wkB_s=68117.21 avg_rkB_s=88.73

### Artifacts

- `/home/jhow/roaring-monad/logs/results/profile-ingest-20260223T195116Z-script-smoke.json`
- `/home/jhow/roaring-monad/logs/profile-pidstat-20260223T195116Z-script-smoke.log`
- `/home/jhow/roaring-monad/logs/profile-iostat-20260223T195116Z-script-smoke.log`
- `/home/jhow/roaring-monad/logs/profile-ingest-20260223T195116Z-script-smoke.log`
- `/home/jhow/roaring-monad/logs/profile-meta-20260223T195116Z-script-smoke.env`

## 20260223T195900Z - Profiling Run bucketed-partition

### Commands

`scripts/profile_ingest.sh`

### Metrics

- profile_result run_id=20260223T195900Z-bucketed-partition blocks=4001 logs=160347 elapsed_s=38.380278798 bps=104.24624638757165 lps=4177.848755188191 mode=single_writer_fast flush=64
- pid=1080195 cmd=benchmarking samples=39 avg_cpu=76.44 avg_wait=0.92 avg_usr=39.54 avg_sys=36.90
- pid=213044 cmd=minio samples=39 avg_cpu=13.00 avg_wait=0.00 avg_usr=7.77 avg_sys=5.23
- pid=213248 cmd=scylla samples=39 avg_cpu=213.51 avg_wait=1.31 avg_usr=95.87 avg_sys=117.64
- nvme0n1 samples=40 avg_util=15.73 avg_aqu=0.49 avg_wkB_s=27237.50 avg_rkB_s=310.69

### Artifacts

- `/home/jhow/roaring-monad/logs/results/profile-ingest-20260223T195900Z-bucketed-partition.json`
- `/home/jhow/roaring-monad/logs/profile-pidstat-20260223T195900Z-bucketed-partition.log`
- `/home/jhow/roaring-monad/logs/profile-iostat-20260223T195900Z-bucketed-partition.log`
- `/home/jhow/roaring-monad/logs/profile-ingest-20260223T195900Z-bucketed-partition.log`
- `/home/jhow/roaring-monad/logs/profile-meta-20260223T195900Z-bucketed-partition.env`

### Interpretation

- Compared to prior 4k-window baseline (`review-perfstat-20260223T193429Z.json` at `93.30 blocks/s`), bucketed partitioning improved to `104.25 blocks/s` (`+11.7%`).
- Scylla remains the dominant consumer and ingest is still round-trip sensitive, but partition hot-spot pressure dropped measurably.

## 20260223T200338Z - Profiling Run prepared-statements

### Commands

`scripts/profile_ingest.sh`

### Metrics

- profile_result run_id=20260223T200338Z-prepared-statements blocks=4001 logs=160347 elapsed_s=23.676232183 bps=168.98803699318327 lps=6772.488069919009 mode=single_writer_fast flush=64
- pid=1087260 cmd=benchmarking samples=24 avg_cpu=71.21 avg_wait=0.54 avg_usr=38.38 avg_sys=32.83
- pid=213044 cmd=minio samples=24 avg_cpu=17.79 avg_wait=0.00 avg_usr=10.71 avg_sys=7.08
- pid=213248 cmd=scylla samples=24 avg_cpu=99.00 avg_wait=0.79 avg_usr=63.17 avg_sys=35.83
- nvme0n1 samples=25 avg_util=29.08 avg_aqu=0.41 avg_wkB_s=33387.86 avg_rkB_s=75.98

### Artifacts

- `/home/jhow/roaring-monad/logs/results/profile-ingest-20260223T200338Z-prepared-statements.json`
- `/home/jhow/roaring-monad/logs/profile-pidstat-20260223T200338Z-prepared-statements.log`
- `/home/jhow/roaring-monad/logs/profile-iostat-20260223T200338Z-prepared-statements.log`
- `/home/jhow/roaring-monad/logs/profile-ingest-20260223T200338Z-prepared-statements.log`
- `/home/jhow/roaring-monad/logs/profile-meta-20260223T200338Z-prepared-statements.env`

### Interpretation

- Compared to bucketed-only run (`104.25 blocks/s`), prepared statements reached `168.99 blocks/s` (`+62.1%`).
- Scylla CPU usage dropped materially (`~213%` to `~99%`) while throughput increased, indicating large coordinator/query-parse overhead was removed.

## 20260223T200811Z - Profiling Run concurrency-128x32

### Commands

`scripts/profile_ingest.sh`

### Metrics

- profile_result run_id=20260223T200811Z-concurrency-128x32 blocks=4001 logs=160347 elapsed_s=20.42971848 bps=195.842150439657 lps=7848.713145850457 mode=single_writer_fast flush=64 locator_c=128 stream_c=32
- pid=1098063 cmd=benchmarking samples=21 avg_cpu=81.81 avg_wait=0.62 avg_usr=44.00 avg_sys=37.81
- pid=213044 cmd=minio samples=21 avg_cpu=19.90 avg_wait=0.00 avg_usr=11.76 avg_sys=8.14
- pid=213248 cmd=scylla samples=21 avg_cpu=112.81 avg_wait=0.90 avg_usr=72.00 avg_sys=40.81
- nvme0n1 samples=22 avg_util=17.41 avg_aqu=0.26 avg_wkB_s=39009.92 avg_rkB_s=83.43

### Artifacts

- `/home/jhow/roaring-monad/logs/results/profile-ingest-20260223T200811Z-concurrency-128x32.json`
- `/home/jhow/roaring-monad/logs/profile-pidstat-20260223T200811Z-concurrency-128x32.log`
- `/home/jhow/roaring-monad/logs/profile-iostat-20260223T200811Z-concurrency-128x32.log`
- `/home/jhow/roaring-monad/logs/profile-ingest-20260223T200811Z-concurrency-128x32.log`
- `/home/jhow/roaring-monad/logs/profile-meta-20260223T200811Z-concurrency-128x32.env`

## 20260223T200845Z - Profiling Run concurrency-256x64

### Commands

`scripts/profile_ingest.sh`

### Metrics

- profile_result run_id=20260223T200845Z-concurrency-256x64 blocks=4001 logs=160347 elapsed_s=20.173661006 bps=198.3279087920647 lps=7948.334214216745 mode=single_writer_fast flush=64 locator_c=256 stream_c=64
- pid=1098694 cmd=benchmarking samples=20 avg_cpu=78.40 avg_wait=0.45 avg_usr=43.05 avg_sys=35.35
- pid=213044 cmd=minio samples=20 avg_cpu=19.95 avg_wait=0.00 avg_usr=11.90 avg_sys=8.05
- pid=213248 cmd=scylla samples=20 avg_cpu=105.75 avg_wait=1.00 avg_usr=68.90 avg_sys=36.85
- nvme0n1 samples=21 avg_util=18.78 avg_aqu=0.26 avg_wkB_s=37873.88 avg_rkB_s=87.41

### Artifacts

- `/home/jhow/roaring-monad/logs/results/profile-ingest-20260223T200845Z-concurrency-256x64.json`
- `/home/jhow/roaring-monad/logs/profile-pidstat-20260223T200845Z-concurrency-256x64.log`
- `/home/jhow/roaring-monad/logs/profile-iostat-20260223T200845Z-concurrency-256x64.log`
- `/home/jhow/roaring-monad/logs/profile-ingest-20260223T200845Z-concurrency-256x64.log`
- `/home/jhow/roaring-monad/logs/profile-meta-20260223T200845Z-concurrency-256x64.env`

## 20260223T200916Z - Profiling Run concurrency-512x64

### Commands

`scripts/profile_ingest.sh`

### Metrics

- profile_result run_id=20260223T200916Z-concurrency-512x64 blocks=4001 logs=160347 elapsed_s=20.319883865 bps=196.90073164697193 lps=7891.137619944266 mode=single_writer_fast flush=64 locator_c=512 stream_c=64
- pid=1099341 cmd=benchmarking samples=21 avg_cpu=77.81 avg_wait=0.43 avg_usr=42.86 avg_sys=34.95
- pid=213044 cmd=minio samples=21 avg_cpu=20.19 avg_wait=0.00 avg_usr=12.43 avg_sys=7.76
- pid=213248 cmd=scylla samples=21 avg_cpu=106.52 avg_wait=1.00 avg_usr=70.14 avg_sys=36.38
- nvme0n1 samples=22 avg_util=18.63 avg_aqu=0.27 avg_wkB_s=40247.29 avg_rkB_s=95.07

### Artifacts

- `/home/jhow/roaring-monad/logs/results/profile-ingest-20260223T200916Z-concurrency-512x64.json`
- `/home/jhow/roaring-monad/logs/profile-pidstat-20260223T200916Z-concurrency-512x64.log`
- `/home/jhow/roaring-monad/logs/profile-iostat-20260223T200916Z-concurrency-512x64.log`
- `/home/jhow/roaring-monad/logs/profile-ingest-20260223T200916Z-concurrency-512x64.log`
- `/home/jhow/roaring-monad/logs/profile-meta-20260223T200916Z-concurrency-512x64.env`

### Sweep Interpretation

- `128/32`: `195.84 blocks/s` (`logs/results/profile-ingest-20260223T200811Z-concurrency-128x32.json`)
- `256/64`: `198.33 blocks/s` (`logs/results/profile-ingest-20260223T200845Z-concurrency-256x64.json`)
- `512/64`: `196.90 blocks/s` (`logs/results/profile-ingest-20260223T200916Z-concurrency-512x64.json`)

## 2026-03-09T18:45:46Z - Query Executor Range-Aware Chunk Loading

### Change Summary

- Made query-time stream loading range-aware in `crates/finalized-history-query/src/query/executor.rs`.
- `fetch_union_log_level` and `fetch_union_block_level` now pass shard-local bounds into `load_stream_entries`.
- The executor now skips manifest `ChunkRef`s whose `min_local..=max_local` does not overlap the requested range and only admits tail entries inside the local range.
- Added regression tests that record blob reads for both address-stream scans and `topic0_block` prefilter scans.

### Hypothesis

- Narrow block-range queries were over-reading sealed chunk blobs because the executor loaded every chunk in each touched stream/shard before filtering by local range.
- Using existing `ChunkRef` range metadata should reduce unnecessary chunk blob reads without changing query results.

### Commands

```bash
cargo +nightly-2025-12-09 test -p finalized-history-query
```

### Before/After Metrics

- Test scenario `query_only_loads_overlapping_address_chunks`:
  - before: `3` address chunk blob reads were expected from the pre-change executor for a 3-chunk manifest queried over only block `2` because all manifest chunks were loaded eagerly
  - after: `1` address chunk blob read observed, plus `1` `log_packs/*` read for result materialization
- Test scenario `topic0_block_prefilter_only_loads_overlapping_chunks`:
  - before: `3` `topic0_block` chunk blob reads were expected from the pre-change executor for a 3-chunk manifest queried over only block `2`
  - after: `1` `topic0_block` chunk blob read observed, plus `1` `log_packs/*` read for result materialization
- Correctness verification:
  - `cargo +nightly-2025-12-09 test -p finalized-history-query`: all tests passed, including new regression coverage

### Interpretation

- The executor was doing avoidable blob work for narrow-range queries within a touched shard/stream.
- The change removes that over-read at the chunk-selection step while preserving the existing query planner and exact-match semantics.

### Methodology Learnings

- A recording blob store in unit/integration tests is a reliable way to validate query-execution I/O reductions without needing a full external benchmark harness.
- For query-path optimizations, test-level read counts provide a stable correctness-plus-efficiency regression signal even when end-to-end latency measurements would be noisy.

## 2026-03-09T19:09:05Z - 24-bit Local IDs / 40-bit Shards

### Change Summary

- Changed stream shard/local ID layout from high-32/low-32 to high-40/low-24.
- Centralized shard/local helpers in `crates/finalized-history-query/src/domain/keys.rs`.
- Migrated ingest and query code to use the helpers instead of ad hoc bit shifts and casts.
- Added rollover coverage for ingest/query behavior at the new 24-bit shard boundary.
- Updated onboarding docs to describe the 24-bit local layout.

### Hypothesis

- Flat per-shard manifests for hot streams would become too large under a 32-bit local ID space.
- Shrinking the local space to 24 bits should bound manifest growth much more aggressively while keeping the current overall indexing design intact.

### Commands

```bash
cargo +nightly-2025-12-09 test -p finalized-history-query
```

### Before/After Metrics

- Local ID capacity per shard:
  - before: `2^32 = 4,294,967,296`
  - after: `2^24 = 16,777,216`
- For a hot stream with `7,776,000,000` entries/year at `1950` entries/chunk:
  - before: about `2,202,547` chunk refs in one full shard manifest (`~44.1 MB` serialized at `20` bytes/ref)
  - after: about `8,604` chunk refs in one full shard manifest (`~172 KB` serialized)
- Verification:
  - `cargo +nightly-2025-12-09 test -p finalized-history-query`: all tests passed
  - includes new rollover test `ingest_and_query_across_24_bit_log_shard_boundary`

### Interpretation

- This is a physical-layout optimization: the system now trades more shard fanout for much smaller per-shard manifests.
- The change materially improves the growth profile for hot streams without requiring a redesign of manifests/chunks/tails.

### Methodology Learnings

- Once shard/local math is centralized behind helpers, alternative layouts become cheap to reason about and test mechanically.
- Boundary-focused tests are essential for bit-layout changes; small happy-path tests do not exercise the failure modes that matter.
- Selected defaults: `log_locator_write_concurrency=256`, `stream_append_concurrency=64`.

## 2026-03-09T19:14:49Z - Full-Shard Query Range Short-Circuits

### Change Summary

- Added a full-shard range helper to the query planner/executor path.
- `overlapping_chunk_refs(...)` now immediately returns the entire slice when the requested local range spans the whole shard.
- Planner overlap estimation now short-circuits to `manifest.approx_count` for full-shard ranges instead of summing chunk refs individually.
- Executor tail loading now bypasses per-entry range checks for full-shard ranges.

### Hypothesis

- After switching chunk-range selection to binary search, interior shards in wide queries still paid unnecessary per-chunk and per-tail-entry range checks even though the requested range covered the whole shard.
- Detecting the whole-shard case should remove that avoidable work on both the planner and executor paths.

### Commands

```bash
cargo +nightly-2025-12-09 test -p finalized-history-query
```

### Before/After Metrics

- Planner full-shard overlap estimation:
  - before: binary-search to the full `chunk_refs` slice, then sum `count` across every chunk ref in the shard
  - after: constant-time shortcut via `manifest.approx_count`
- Executor full-shard tail handling:
  - before: check every tail entry against `local_from..=local_to` even when the range was `0..=MAX_LOCAL_ID`
  - after: insert the tail entries directly without range comparisons
- Correctness verification:
  - `cargo +nightly-2025-12-09 test -p finalized-history-query`: all tests passed, including new full-shard helper coverage

### Interpretation

- Wide queries still need to read the relevant chunk blobs, but the full-shard case no longer spends extra CPU on range filtering that cannot exclude anything.
- The change is small but matches the new 24-bit layout better because wide multi-shard scans now encounter many interior shards that are fully covered by the query range.

### Methodology Learnings

- Once a range helper is shared between planner and executor, the next optimization step is usually to recognize degenerate cases such as empty or full-domain ranges and short-circuit them explicitly.

## 20260223T201235Z - Profiling Run stream-cache

### Commands

`scripts/profile_ingest.sh`

### Metrics

- profile_result run_id=20260223T201235Z-stream-cache blocks=4001 logs=160347 elapsed_s=19.769206942 bps=202.38545793659586 lps=8110.947519060069 mode=single_writer_fast flush=64 locator_c=256 stream_c=64
- pid=1107211 cmd=benchmarking samples=20 avg_cpu=58.50 avg_wait=0.25 avg_usr=32.30 avg_sys=26.20
- pid=213044 cmd=minio samples=20 avg_cpu=19.65 avg_wait=0.00 avg_usr=12.00 avg_sys=7.65
- pid=213248 cmd=scylla samples=20 avg_cpu=76.00 avg_wait=0.50 avg_usr=49.40 avg_sys=26.60
- nvme0n1 samples=21 avg_util=25.59 avg_aqu=0.36 avg_wkB_s=36339.14 avg_rkB_s=381.30

### Artifacts

- `/home/jhow/roaring-monad/logs/results/profile-ingest-20260223T201235Z-stream-cache.json`
- `/home/jhow/roaring-monad/logs/profile-pidstat-20260223T201235Z-stream-cache.log`
- `/home/jhow/roaring-monad/logs/profile-iostat-20260223T201235Z-stream-cache.log`
- `/home/jhow/roaring-monad/logs/profile-ingest-20260223T201235Z-stream-cache.log`
- `/home/jhow/roaring-monad/logs/profile-meta-20260223T201235Z-stream-cache.env`

### Interpretation

- Compared to the tuned-concurrency run (`198.33 blocks/s`), stream-state cache reached `202.39 blocks/s` (`+2.0%`).
- Scylla CPU dropped again (`~105.75%` to `~76.00%`), consistent with fewer manifest/tail read round-trips.

## 20260223T202211Z - Profiling Run locator-pages

### Commands

`scripts/profile_ingest.sh`

### Metrics

- profile_result run_id=20260223T202211Z-locator-pages blocks=4001 logs=160347 elapsed_s=18.674556901 bps=214.248724679821 lps=8586.388466942079 mode=single_writer_fast flush=64 locator_c=256 stream_c=64
- pid=1123951 cmd=benchmarking samples=19 avg_cpu=61.32 avg_wait=0.26 avg_usr=34.00 avg_sys=27.32
- pid=213044 cmd=minio samples=19 avg_cpu=20.68 avg_wait=0.00 avg_usr=12.68 avg_sys=8.00
- pid=213248 cmd=scylla samples=19 avg_cpu=79.74 avg_wait=0.53 avg_usr=51.32 avg_sys=28.42
- nvme0n1 samples=20 avg_util=20.18 avg_aqu=0.41 avg_wkB_s=42980.08 avg_rkB_s=98.75

### Artifacts

- `/home/jhow/roaring-monad/logs/results/profile-ingest-20260223T202211Z-locator-pages.json`
- `/home/jhow/roaring-monad/logs/profile-pidstat-20260223T202211Z-locator-pages.log`
- `/home/jhow/roaring-monad/logs/profile-iostat-20260223T202211Z-locator-pages.log`
- `/home/jhow/roaring-monad/logs/profile-ingest-20260223T202211Z-locator-pages.log`
- `/home/jhow/roaring-monad/logs/profile-meta-20260223T202211Z-locator-pages.env`

## 20260223T202258Z - Profiling Run lps96

### Commands

`scripts/profile_ingest.sh`

### Metrics

- profile_result run_id=20260223T202258Z-lps96 blocks=4001 logs=160347 elapsed_s=18.398428448 bps=217.46422588799578 lps=8715.25524330479 mode=single_writer_fast flush=64 locator_c=256 stream_c=96
- pid=1124883 cmd=benchmarking samples=19 avg_cpu=61.95 avg_wait=0.21 avg_usr=34.05 avg_sys=27.89
- pid=213044 cmd=minio samples=19 avg_cpu=20.95 avg_wait=0.00 avg_usr=12.95 avg_sys=8.00
- pid=213248 cmd=scylla samples=19 avg_cpu=80.32 avg_wait=0.47 avg_usr=52.26 avg_sys=28.05
- nvme0n1 samples=20 avg_util=19.57 avg_aqu=0.28 avg_wkB_s=39447.90 avg_rkB_s=99.34

### Artifacts

- `/home/jhow/roaring-monad/logs/results/profile-ingest-20260223T202258Z-lps96.json`
- `/home/jhow/roaring-monad/logs/profile-pidstat-20260223T202258Z-lps96.log`
- `/home/jhow/roaring-monad/logs/profile-iostat-20260223T202258Z-lps96.log`
- `/home/jhow/roaring-monad/logs/profile-ingest-20260223T202258Z-lps96.log`
- `/home/jhow/roaring-monad/logs/profile-meta-20260223T202258Z-lps96.env`

## 20260223T202323Z - Profiling Run lps128

### Commands

`scripts/profile_ingest.sh`

### Metrics

- profile_result run_id=20260223T202323Z-lps128 blocks=4001 logs=160347 elapsed_s=18.699052729 bps=213.96805806076614 lps=8575.140266400816 mode=single_writer_fast flush=64 locator_c=256 stream_c=128
- pid=1125295 cmd=benchmarking samples=19 avg_cpu=61.05 avg_wait=0.26 avg_usr=33.58 avg_sys=27.47
- pid=213044 cmd=minio samples=19 avg_cpu=20.68 avg_wait=0.00 avg_usr=12.84 avg_sys=7.84
- pid=213248 cmd=scylla samples=19 avg_cpu=80.95 avg_wait=0.53 avg_usr=52.89 avg_sys=28.05
- nvme0n1 samples=20 avg_util=19.91 avg_aqu=0.31 avg_wkB_s=41283.73 avg_rkB_s=364.34

### Artifacts

- `/home/jhow/roaring-monad/logs/results/profile-ingest-20260223T202323Z-lps128.json`
- `/home/jhow/roaring-monad/logs/profile-pidstat-20260223T202323Z-lps128.log`
- `/home/jhow/roaring-monad/logs/profile-iostat-20260223T202323Z-lps128.log`
- `/home/jhow/roaring-monad/logs/profile-ingest-20260223T202323Z-lps128.log`
- `/home/jhow/roaring-monad/logs/profile-meta-20260223T202323Z-lps128.env`

## 20260223T202346Z - Profiling Run lpc384s96

### Commands

`scripts/profile_ingest.sh`

### Metrics

- profile_result run_id=20260223T202346Z-lpc384s96 blocks=4001 logs=160347 elapsed_s=18.92381786 bps=211.42668089493014 lps=8473.290177820387 mode=single_writer_fast flush=64 locator_c=384 stream_c=96
- pid=1125745 cmd=benchmarking samples=19 avg_cpu=60.58 avg_wait=0.26 avg_usr=33.32 avg_sys=27.26
- pid=213044 cmd=minio samples=19 avg_cpu=20.58 avg_wait=0.00 avg_usr=12.79 avg_sys=7.79
- pid=213248 cmd=scylla samples=19 avg_cpu=79.53 avg_wait=0.47 avg_usr=51.89 avg_sys=27.63
- nvme0n1 samples=20 avg_util=20.77 avg_aqu=0.30 avg_wkB_s=38885.79 avg_rkB_s=109.34

### Artifacts

- `/home/jhow/roaring-monad/logs/results/profile-ingest-20260223T202346Z-lpc384s96.json`
- `/home/jhow/roaring-monad/logs/profile-pidstat-20260223T202346Z-lpc384s96.log`
- `/home/jhow/roaring-monad/logs/profile-iostat-20260223T202346Z-lpc384s96.log`
- `/home/jhow/roaring-monad/logs/profile-ingest-20260223T202346Z-lpc384s96.log`
- `/home/jhow/roaring-monad/logs/profile-meta-20260223T202346Z-lpc384s96.env`

## 20260223T202411Z - Profiling Run lpc128s96

### Commands

`scripts/profile_ingest.sh`

### Metrics

- profile_result run_id=20260223T202411Z-lpc128s96 blocks=4001 logs=160347 elapsed_s=18.782728281 bps=213.01484747811008 lps=8536.938702467512 mode=single_writer_fast flush=64 locator_c=128 stream_c=96
- pid=1126270 cmd=benchmarking samples=19 avg_cpu=60.89 avg_wait=0.26 avg_usr=34.00 avg_sys=26.89
- pid=213044 cmd=minio samples=19 avg_cpu=20.84 avg_wait=0.00 avg_usr=12.42 avg_sys=8.42
- pid=213248 cmd=scylla samples=19 avg_cpu=81.74 avg_wait=0.47 avg_usr=54.42 avg_sys=27.32
- nvme0n1 samples=20 avg_util=19.57 avg_aqu=0.28 avg_wkB_s=36865.23 avg_rkB_s=446.94

### Artifacts

- `/home/jhow/roaring-monad/logs/results/profile-ingest-20260223T202411Z-lpc128s96.json`
- `/home/jhow/roaring-monad/logs/profile-pidstat-20260223T202411Z-lpc128s96.log`
- `/home/jhow/roaring-monad/logs/profile-iostat-20260223T202411Z-lpc128s96.log`
- `/home/jhow/roaring-monad/logs/profile-ingest-20260223T202411Z-lpc128s96.log`
- `/home/jhow/roaring-monad/logs/profile-meta-20260223T202411Z-lpc128s96.env`

## 20260223T202725Z - Profiling Run def96

### Commands

`scripts/profile_ingest.sh`

### Metrics

- profile_result run_id=20260223T202725Z-def96 blocks=4001 logs=160347 elapsed_s=18.700864714 bps=213.94732602951441 lps=8574.309394365046 mode=single_writer_fast flush=64 locator_c=256 stream_c=96
- pid=1135742 cmd=benchmarking samples=19 avg_cpu=61.16 avg_wait=0.26 avg_usr=33.79 avg_sys=27.37
- pid=213044 cmd=minio samples=19 avg_cpu=20.79 avg_wait=0.00 avg_usr=12.89 avg_sys=7.89
- pid=213248 cmd=scylla samples=19 avg_cpu=79.79 avg_wait=0.53 avg_usr=51.32 avg_sys=28.47
- nvme0n1 samples=20 avg_util=20.51 avg_aqu=2.86 avg_wkB_s=82484.95 avg_rkB_s=62.94

### Artifacts

- `/home/jhow/roaring-monad/logs/results/profile-ingest-20260223T202725Z-def96.json`
- `/home/jhow/roaring-monad/logs/profile-pidstat-20260223T202725Z-def96.log`
- `/home/jhow/roaring-monad/logs/profile-iostat-20260223T202725Z-def96.log`
- `/home/jhow/roaring-monad/logs/profile-ingest-20260223T202725Z-def96.log`
- `/home/jhow/roaring-monad/logs/profile-meta-20260223T202725Z-def96.env`

## 20260223T202753Z - Profiling Run a64

### Commands

`scripts/profile_ingest.sh`

### Metrics

- profile_result run_id=20260223T202753Z-a64 blocks=4001 logs=160347 elapsed_s=18.602680421 bps=215.07653249170443 lps=8619.564297787385 mode=single_writer_fast flush=64 locator_c=256 stream_c=64
- pid=1136296 cmd=benchmarking samples=19 avg_cpu=61.37 avg_wait=0.26 avg_usr=33.74 avg_sys=27.63
- pid=213044 cmd=minio samples=19 avg_cpu=21.26 avg_wait=0.00 avg_usr=12.84 avg_sys=8.42
- pid=213248 cmd=scylla samples=19 avg_cpu=79.79 avg_wait=0.47 avg_usr=51.53 avg_sys=28.26
- nvme0n1 samples=20 avg_util=20.13 avg_aqu=0.28 avg_wkB_s=37130.92 avg_rkB_s=29.14

### Artifacts

- `/home/jhow/roaring-monad/logs/results/profile-ingest-20260223T202753Z-a64.json`
- `/home/jhow/roaring-monad/logs/profile-pidstat-20260223T202753Z-a64.log`
- `/home/jhow/roaring-monad/logs/profile-iostat-20260223T202753Z-a64.log`
- `/home/jhow/roaring-monad/logs/profile-ingest-20260223T202753Z-a64.log`
- `/home/jhow/roaring-monad/logs/profile-meta-20260223T202753Z-a64.env`

## 20260223T202815Z - Profiling Run a96

### Commands

`scripts/profile_ingest.sh`

### Metrics

- profile_result run_id=20260223T202815Z-a96 blocks=4001 logs=160347 elapsed_s=18.633774831 bps=214.71763162790577 lps=8605.180724478832 mode=single_writer_fast flush=64 locator_c=256 stream_c=96
- pid=1136690 cmd=benchmarking samples=19 avg_cpu=61.16 avg_wait=0.21 avg_usr=33.26 avg_sys=27.89
- pid=213044 cmd=minio samples=19 avg_cpu=21.00 avg_wait=0.00 avg_usr=12.79 avg_sys=8.21
- pid=213248 cmd=scylla samples=19 avg_cpu=79.58 avg_wait=0.53 avg_usr=52.11 avg_sys=27.47
- nvme0n1 samples=20 avg_util=21.72 avg_aqu=0.40 avg_wkB_s=37972.74 avg_rkB_s=175.94

### Artifacts

- `/home/jhow/roaring-monad/logs/results/profile-ingest-20260223T202815Z-a96.json`
- `/home/jhow/roaring-monad/logs/profile-pidstat-20260223T202815Z-a96.log`
- `/home/jhow/roaring-monad/logs/profile-iostat-20260223T202815Z-a96.log`
- `/home/jhow/roaring-monad/logs/profile-ingest-20260223T202815Z-a96.log`
- `/home/jhow/roaring-monad/logs/profile-meta-20260223T202815Z-a96.env`

## 20260223T202952Z - Profiling Run locator-pages-c256-s96

### Commands

`scripts/profile_ingest.sh`

### Metrics

- profile_result run_id=20260223T202952Z-locator-pages-c256-s96 blocks=4001 logs=160347 elapsed_s=21.668991967 bps=184.641722424983 lps=7399.836607267869 mode=single_writer_fast flush=64 locator_c=256 stream_c=64
- pid=1138410 cmd=benchmarking samples=22 avg_cpu=53.32 avg_wait=0.23 avg_usr=29.41 avg_sys=23.91
- pid=213044 cmd=minio samples=22 avg_cpu=18.82 avg_wait=0.00 avg_usr=11.09 avg_sys=7.73
- pid=213248 cmd=scylla samples=22 avg_cpu=69.27 avg_wait=0.41 avg_usr=44.59 avg_sys=24.68
- nvme0n1 samples=23 avg_util=30.89 avg_aqu=0.42 avg_wkB_s=33199.83 avg_rkB_s=25.85

### Artifacts

- `/home/jhow/roaring-monad/logs/results/profile-ingest-20260223T202952Z-locator-pages-c256-s96.json`
- `/home/jhow/roaring-monad/logs/profile-pidstat-20260223T202952Z-locator-pages-c256-s96.log`
- `/home/jhow/roaring-monad/logs/profile-iostat-20260223T202952Z-locator-pages-c256-s96.log`
- `/home/jhow/roaring-monad/logs/profile-ingest-20260223T202952Z-locator-pages-c256-s96.log`
- `/home/jhow/roaring-monad/logs/profile-meta-20260223T202952Z-locator-pages-c256-s96.env`

## 2026-02-23T20:30:00Z - Methodology Learnings (Post Locator-Page Rollout)

### Context

- Scope: post-change profiling after moving log locator metadata to paged writes.
- Goal: isolate the next limiting factor and decide whether to retune write concurrency defaults.

### Findings

- New best run in this batch: `217.46 blocks/s` (`run_id=20260223T202258Z-lps96`).
- Repeated A/B runs (`stream_c=64` vs `96`) are very close (`~214-215 blocks/s`) and currently within short-run noise.
- Across steady runs, Scylla remains the largest sustained CPU consumer (`~79-81% avg CPU`), while benchmarking process is lower (`~61% avg CPU`) and MinIO lower (`~20-21% avg CPU`).

### Interpretation

- The dominant bottleneck has shifted from per-log locator write fanout to remaining Scylla metadata pressure (stream tail/manifest/topic0 metadata writes), not MinIO bandwidth.
- Small tuning deltas should be treated as directional only unless confirmed with repeated medians under quiet host conditions.
- A single noisy run (`run_id=20260223T202952Z-locator-pages-c256-s96`) demonstrates why one-off numbers should not drive defaults.

### Methodology Updates

- Always run at least 2-3 back-to-back A/B comparisons before changing defaults.
- Keep run tags short enough for Scylla keyspace limits, or enforce automatic truncation in tooling.
- Prefer comparing medians and not single maxima when differences are <2%.
- Continue collecting `pidstat` + `iostat` with each run to distinguish DB CPU pressure from disk saturation.

## 20260223T203327Z - Profiling Run ase0

### Commands

`scripts/profile_ingest.sh`

### Metrics

- profile_result run_id=20260223T203327Z-ase0 blocks=4001 logs=160347 elapsed_s=20.28723852 bps=197.21757577087925 lps=7903.835696609141 mode=single_writer_fast flush=64 assume_empty=null locator_c=256 stream_c=64
- pid=1143792 cmd=benchmarking samples=21 avg_cpu=56.38 avg_wait=0.24 avg_usr=30.19 avg_sys=26.19
- pid=213044 cmd=minio samples=21 avg_cpu=16.10 avg_wait=0.00 avg_usr=9.38 avg_sys=6.71
- pid=213248 cmd=scylla samples=21 avg_cpu=73.10 avg_wait=0.48 avg_usr=47.43 avg_sys=25.67
- nvme0n1 samples=22 avg_util=27.13 avg_aqu=0.58 avg_wkB_s=46479.85 avg_rkB_s=18.65

### Artifacts

- `/home/jhow/roaring-monad/logs/results/profile-ingest-20260223T203327Z-ase0.json`
- `/home/jhow/roaring-monad/logs/profile-pidstat-20260223T203327Z-ase0.log`
- `/home/jhow/roaring-monad/logs/profile-iostat-20260223T203327Z-ase0.log`
- `/home/jhow/roaring-monad/logs/profile-ingest-20260223T203327Z-ase0.log`
- `/home/jhow/roaring-monad/logs/profile-meta-20260223T203327Z-ase0.env`

## 20260223T203745Z - Profiling Run ase0n

### Commands

`scripts/profile_ingest.sh`

### Metrics

- profile_result run_id=20260223T203745Z-ase0n blocks=4001 logs=160347 elapsed_s=20.622122791 bps=194.01494407482312 lps=7775.484688219361 mode=single_writer_fast flush=64 assume_empty=false locator_c=256 stream_c=64
- pid=1167408 cmd=benchmarking samples=21 avg_cpu=43.81 avg_wait=0.10 avg_usr=24.57 avg_sys=19.24
- pid=213044 cmd=minio samples=21 avg_cpu=19.43 avg_wait=0.00 avg_usr=11.67 avg_sys=7.76
- pid=213248 cmd=scylla samples=21 avg_cpu=52.33 avg_wait=0.29 avg_usr=33.05 avg_sys=19.29
- nvme0n1 samples=22 avg_util=28.78 avg_aqu=0.43 avg_wkB_s=38759.71 avg_rkB_s=11.78

### Artifacts

- `/home/jhow/roaring-monad/logs/results/profile-ingest-20260223T203745Z-ase0n.json`
- `/home/jhow/roaring-monad/logs/profile-pidstat-20260223T203745Z-ase0n.log`
- `/home/jhow/roaring-monad/logs/profile-iostat-20260223T203745Z-ase0n.log`
- `/home/jhow/roaring-monad/logs/profile-ingest-20260223T203745Z-ase0n.log`
- `/home/jhow/roaring-monad/logs/profile-meta-20260223T203745Z-ase0n.env`

## 20260223T203809Z - Profiling Run ase1n

### Commands

`scripts/profile_ingest.sh`

### Metrics

- profile_result run_id=20260223T203809Z-ase1n blocks=4001 logs=160347 elapsed_s=18.862704119 bps=212.11168742078067 lps=8500.742999964988 mode=single_writer_fast flush=64 assume_empty=true locator_c=256 stream_c=64
- pid=1167837 cmd=benchmarking samples=19 avg_cpu=44.89 avg_wait=0.11 avg_usr=25.42 avg_sys=19.47
- pid=213044 cmd=minio samples=19 avg_cpu=20.68 avg_wait=0.00 avg_usr=12.21 avg_sys=8.47
- pid=213248 cmd=scylla samples=19 avg_cpu=52.95 avg_wait=0.26 avg_usr=33.00 avg_sys=19.95
- nvme0n1 samples=20 avg_util=23.98 avg_aqu=0.40 avg_wkB_s=40970.54 avg_rkB_s=19.36

### Artifacts

- `/home/jhow/roaring-monad/logs/results/profile-ingest-20260223T203809Z-ase1n.json`
- `/home/jhow/roaring-monad/logs/profile-pidstat-20260223T203809Z-ase1n.log`
- `/home/jhow/roaring-monad/logs/profile-iostat-20260223T203809Z-ase1n.log`
- `/home/jhow/roaring-monad/logs/profile-ingest-20260223T203809Z-ase1n.log`
- `/home/jhow/roaring-monad/logs/profile-meta-20260223T203809Z-ase1n.env`

## 20260223T203841Z - Profiling Run ase0m

### Commands

`scripts/profile_ingest.sh`

### Metrics

- profile_result run_id=20260223T203841Z-ase0m blocks=4001 logs=160347 elapsed_s=18.601446528 bps=215.09079920088246 lps=8620.136060850762 mode=single_writer_fast flush=64 assume_empty=false locator_c=256 stream_c=64
- pid=1168402 cmd=benchmarking samples=19 avg_cpu=48.11 avg_wait=0.11 avg_usr=26.53 avg_sys=21.58
- pid=213044 cmd=minio samples=19 avg_cpu=21.05 avg_wait=0.00 avg_usr=12.79 avg_sys=8.26
- pid=213248 cmd=scylla samples=19 avg_cpu=57.42 avg_wait=0.26 avg_usr=35.74 avg_sys=21.68
- nvme0n1 samples=20 avg_util=21.57 avg_aqu=0.30 avg_wkB_s=39989.58 avg_rkB_s=284.55

### Artifacts

- `/home/jhow/roaring-monad/logs/results/profile-ingest-20260223T203841Z-ase0m.json`
- `/home/jhow/roaring-monad/logs/profile-pidstat-20260223T203841Z-ase0m.log`
- `/home/jhow/roaring-monad/logs/profile-iostat-20260223T203841Z-ase0m.log`
- `/home/jhow/roaring-monad/logs/profile-ingest-20260223T203841Z-ase0m.log`
- `/home/jhow/roaring-monad/logs/profile-meta-20260223T203841Z-ase0m.env`

## 20260223T203905Z - Profiling Run ase1m

### Commands

`scripts/profile_ingest.sh`

### Metrics

- profile_result run_id=20260223T203905Z-ase1m blocks=4001 logs=160347 elapsed_s=18.031499708 bps=221.88947479642442 lps=8892.604752607414 mode=single_writer_fast flush=64 assume_empty=true locator_c=256 stream_c=64
- pid=1168926 cmd=benchmarking samples=18 avg_cpu=46.67 avg_wait=0.11 avg_usr=26.44 avg_sys=20.22
- pid=213044 cmd=minio samples=18 avg_cpu=20.83 avg_wait=0.00 avg_usr=12.28 avg_sys=8.56
- pid=213248 cmd=scylla samples=18 avg_cpu=55.50 avg_wait=0.28 avg_usr=35.39 avg_sys=20.11
- nvme0n1 samples=19 avg_util=22.93 avg_aqu=0.37 avg_wkB_s=39468.93 avg_rkB_s=1943.71

### Artifacts

- `/home/jhow/roaring-monad/logs/results/profile-ingest-20260223T203905Z-ase1m.json`
- `/home/jhow/roaring-monad/logs/profile-pidstat-20260223T203905Z-ase1m.log`
- `/home/jhow/roaring-monad/logs/profile-iostat-20260223T203905Z-ase1m.log`
- `/home/jhow/roaring-monad/logs/profile-ingest-20260223T203905Z-ase1m.log`
- `/home/jhow/roaring-monad/logs/profile-meta-20260223T203905Z-ase1m.env`

## 20260223T203929Z - Profiling Run ase0p

### Commands

`scripts/profile_ingest.sh`

### Metrics

- profile_result run_id=20260223T203929Z-ase0p blocks=4001 logs=160347 elapsed_s=18.526159436 bps=215.96489082487673 lps=8655.166795575233 mode=single_writer_fast flush=64 assume_empty=false locator_c=256 stream_c=64
- pid=1169363 cmd=benchmarking samples=19 avg_cpu=48.32 avg_wait=0.11 avg_usr=27.00 avg_sys=21.32
- pid=213044 cmd=minio samples=19 avg_cpu=20.53 avg_wait=0.00 avg_usr=12.00 avg_sys=8.53
- pid=213248 cmd=scylla samples=19 avg_cpu=58.16 avg_wait=0.32 avg_usr=36.89 avg_sys=21.26
- nvme0n1 samples=20 avg_util=22.13 avg_aqu=0.33 avg_wkB_s=40074.05 avg_rkB_s=2010.97

### Artifacts

- `/home/jhow/roaring-monad/logs/results/profile-ingest-20260223T203929Z-ase0p.json`
- `/home/jhow/roaring-monad/logs/profile-pidstat-20260223T203929Z-ase0p.log`
- `/home/jhow/roaring-monad/logs/profile-iostat-20260223T203929Z-ase0p.log`
- `/home/jhow/roaring-monad/logs/profile-ingest-20260223T203929Z-ase0p.log`
- `/home/jhow/roaring-monad/logs/profile-meta-20260223T203929Z-ase0p.env`

## 20260223T203954Z - Profiling Run ase1p

### Commands

`scripts/profile_ingest.sh`

### Metrics

- profile_result run_id=20260223T203954Z-ase1p blocks=4001 logs=160347 elapsed_s=18.15743029 bps=220.35056371404633 lps=8830.930227407196 mode=single_writer_fast flush=64 assume_empty=true locator_c=256 stream_c=64
- pid=1169822 cmd=benchmarking samples=18 avg_cpu=46.83 avg_wait=0.11 avg_usr=26.56 avg_sys=20.28
- pid=213044 cmd=minio samples=18 avg_cpu=20.72 avg_wait=0.00 avg_usr=12.17 avg_sys=8.56
- pid=213248 cmd=scylla samples=18 avg_cpu=55.78 avg_wait=0.28 avg_usr=34.78 avg_sys=21.00
- nvme0n1 samples=19 avg_util=22.10 avg_aqu=0.33 avg_wkB_s=42286.94 avg_rkB_s=2049.45

### Artifacts

- `/home/jhow/roaring-monad/logs/results/profile-ingest-20260223T203954Z-ase1p.json`
- `/home/jhow/roaring-monad/logs/profile-pidstat-20260223T203954Z-ase1p.log`
- `/home/jhow/roaring-monad/logs/profile-iostat-20260223T203954Z-ase1p.log`
- `/home/jhow/roaring-monad/logs/profile-ingest-20260223T203954Z-ase1p.log`
- `/home/jhow/roaring-monad/logs/profile-meta-20260223T203954Z-ase1p.env`

## 20260223T204058Z - Profiling Run ase1s96

### Commands

`scripts/profile_ingest.sh`

### Metrics

- profile_result run_id=20260223T204058Z-ase1s96 blocks=4001 logs=160347 elapsed_s=17.864289409 bps=223.9663671136173 lps=8975.839806940065 mode=single_writer_fast flush=64 assume_empty=true locator_c=256 stream_c=96
- pid=1171099 cmd=benchmarking samples=18 avg_cpu=47.22 avg_wait=0.11 avg_usr=26.44 avg_sys=20.78
- pid=213044 cmd=minio samples=18 avg_cpu=20.83 avg_wait=0.00 avg_usr=12.22 avg_sys=8.61
- pid=213248 cmd=scylla samples=18 avg_cpu=55.39 avg_wait=0.28 avg_usr=34.89 avg_sys=20.50
- nvme0n1 samples=19 avg_util=21.56 avg_aqu=0.31 avg_wkB_s=38597.95 avg_rkB_s=1864.25

### Artifacts

- `/home/jhow/roaring-monad/logs/results/profile-ingest-20260223T204058Z-ase1s96.json`
- `/home/jhow/roaring-monad/logs/profile-pidstat-20260223T204058Z-ase1s96.log`
- `/home/jhow/roaring-monad/logs/profile-iostat-20260223T204058Z-ase1s96.log`
- `/home/jhow/roaring-monad/logs/profile-ingest-20260223T204058Z-ase1s96.log`
- `/home/jhow/roaring-monad/logs/profile-meta-20260223T204058Z-ase1s96.env`

## 20260223T204121Z - Profiling Run ase1s128

### Commands

`scripts/profile_ingest.sh`

### Metrics

- profile_result run_id=20260223T204121Z-ase1s128 blocks=4001 logs=160347 elapsed_s=18.292066626 bps=218.72870254654845 lps=8765.931333974357 mode=single_writer_fast flush=64 assume_empty=true locator_c=256 stream_c=128
- pid=1171598 cmd=benchmarking samples=19 avg_cpu=46.53 avg_wait=0.11 avg_usr=26.26 avg_sys=20.26
- pid=213044 cmd=minio samples=19 avg_cpu=20.68 avg_wait=0.00 avg_usr=11.84 avg_sys=8.84
- pid=213248 cmd=scylla samples=19 avg_cpu=54.32 avg_wait=0.26 avg_usr=33.95 avg_sys=20.37
- nvme0n1 samples=20 avg_util=22.74 avg_aqu=0.33 avg_wkB_s=41917.68 avg_rkB_s=1900.65

### Artifacts

- `/home/jhow/roaring-monad/logs/results/profile-ingest-20260223T204121Z-ase1s128.json`
- `/home/jhow/roaring-monad/logs/profile-pidstat-20260223T204121Z-ase1s128.log`
- `/home/jhow/roaring-monad/logs/profile-iostat-20260223T204121Z-ase1s128.log`
- `/home/jhow/roaring-monad/logs/profile-ingest-20260223T204121Z-ase1s128.log`
- `/home/jhow/roaring-monad/logs/profile-meta-20260223T204121Z-ase1s128.env`

## 20260223T204241Z - Profiling Run defopt

### Commands

`scripts/profile_ingest.sh`

### Metrics

- profile_result run_id=20260223T204241Z-defopt blocks=4001 logs=160347 elapsed_s=18.097233932 bps=221.0835100564914 lps=8860.304320676887 mode=single_writer_fast flush=64 assume_empty=true locator_c=256 stream_c=96
- pid=1174490 cmd=benchmarking samples=18 avg_cpu=46.67 avg_wait=0.11 avg_usr=26.44 avg_sys=20.22
- pid=213044 cmd=minio samples=18 avg_cpu=21.28 avg_wait=0.00 avg_usr=12.61 avg_sys=8.67
- pid=213248 cmd=scylla samples=18 avg_cpu=56.78 avg_wait=0.33 avg_usr=36.22 avg_sys=20.56
- nvme0n1 samples=19 avg_util=20.81 avg_aqu=0.54 avg_wkB_s=53341.47 avg_rkB_s=371.87

### Artifacts

- `/home/jhow/roaring-monad/logs/results/profile-ingest-20260223T204241Z-defopt.json`
- `/home/jhow/roaring-monad/logs/profile-pidstat-20260223T204241Z-defopt.log`
- `/home/jhow/roaring-monad/logs/profile-iostat-20260223T204241Z-defopt.log`
- `/home/jhow/roaring-monad/logs/profile-ingest-20260223T204241Z-defopt.log`
- `/home/jhow/roaring-monad/logs/profile-meta-20260223T204241Z-defopt.env`

## 2026-02-23T20:45:00Z - Post Assume-Empty + Concurrency Tuning

### Summary

- Default profile after latest changes (`run_id=20260223T204241Z-defopt`) reached `221.08 blocks/s` / `8860.30 logs/s`.
- Relative to the pre-page baseline (`202.39 blocks/s`), current path is now `+9.2%` faster end-to-end on the same 4,001-block window.

### Bottleneck Snapshot

- Scylla remains the largest single backend consumer, but no longer dominates as heavily as earlier runs.
- Current run averages: benchmarking process `~46.67%` CPU, Scylla `~56.78%` CPU, MinIO `~21.28%` CPU.
- Practical interpretation: ingest pressure is now split between app-side stream/index work and Scylla meta writes, with less acute DB hotspot behavior than earlier stages.

## 2026-03-09T20:01:47Z - Always-On `topic0`

### Summary

- Removed adaptive `topic0_mode` / `topic0_stats` state and now write `topic0` unconditionally for every observed `topic0`.
- The planner now treats `topic0` like the other exact log-level clauses, even when the estimated overlap is zero.
- The executor skips `topic0_block` prefilter reads when `Topic0Log` is already in the clause plan.

### Hypothesis

- Always-on `topic0` should make topic0 query semantics simpler and avoid partial-coverage ambiguity.
- Indexed topic0 queries should no longer spend work on redundant `topic0_block` reads.

### Commands

```bash
cargo +nightly-2025-12-09 test -p finalized-history-query
cargo +nightly-2025-12-09 test -p benchmarking
```

### Metrics

- Correctness verification only in this change set.
- `finalized-history-query`: `40` tests passed.
- `benchmarking`: crate compiled and test target completed with no test failures.
- No before/after performance benchmark has been run yet for always-on `topic0`.

### Interpretation

- The semantic simplification is implemented and covered by regression tests.
- Performance impact still needs a dedicated ingest/query benchmark before deciding whether `topic0_block` remains worthwhile.

## 2026-03-09T20:01:47Z - Remove `topic0_block`

### Summary

- Removed `topic0_block` from ingest and query execution.
- `topic0` now behaves like the other topic clauses and is backed only by the `topic0` stream.
- The executor no longer carries a separate block-level topic0 prefilter path.

### Hypothesis

- Removing `topic0_block` should reduce ingest/storage overhead and simplify the query model without changing indexed topic0 query results.

### Commands

```bash
cargo +nightly-2025-12-09 test -p finalized-history-query
cargo +nightly-2025-12-09 test -p benchmarking
```

### Metrics

- Correctness verification only in this change set.
- `finalized-history-query`: `40` tests passed after removing the block-level topic0 path.
- `benchmarking`: crate compiled and test target completed with no test failures.
- No before/after performance benchmark has been run yet for the removal.

### Interpretation

- The codebase now has one exact topic0 path instead of a hybrid topic0 design.
- A dedicated ingest benchmark is still needed to quantify the write-volume reduction.

## 2026-03-10T13:50:59-04:00 - Finalized History Query Benchmark Smoke Run

### Summary

- Ran the new Criterion benchmark split for `finalized-history-query` to validate result shape after introducing execution, clause-loading, materialization, and end-to-end query benches.
- Goal was not absolute performance gating; it was to check whether relative scaling across shards, OR width, cache warmth, and locality looks coherent.

### Hypothesis

- Candidate execution should scale sharply with shard fanout and dense many-shard intersections.
- Clause loading should scale with shard count and OR width, while high-shard IDs should behave similarly to low-shard IDs when shard count is held constant.
- Materialization should show a clear warm-vs-cold and same-block-vs-cross-block locality gap.
- End-to-end query results should broadly track the microbench cost curves without exposing obvious inversions.

### Commands

```bash
cargo bench -p finalized-history-query --bench execution_bench
cargo bench -p finalized-history-query --bench query_clause_loading_bench
cargo bench -p finalized-history-query --bench materialize_bench
cargo bench -p finalized-history-query --bench query_end_to_end_bench
```

### Environment

- Local Criterion run on the development machine.
- Plot output used the plotters backend because `gnuplot` was not installed.
- Four benches were launched concurrently, so there was initial cargo file-lock contention before the timed Criterion sections started.

### Workload Shape

- Execution: single-clause iteration over `1`, `8`, `64`, and high shard IDs; sparse/dense intersections; many-shard union-like intersections; empty clause set; edge clipping.
- Clause loading: manifest-only, tail-only, manifest+tail, high-shard manifest-only, OR widths `1/4/16/64/256`, sparse vs dense chunks.
- Materialization: cold vs warm caches, same-block vs cross-block locality, multi-bucket block lookup, high-shard point lookup.
- End-to-end: narrow indexed query, indexed intersection, indexed OR widths `4/16/64/256`, pagination-heavy resume walk.

### Metrics

- `execution_single_clause/one_shard`: `54.6 us`
- `execution_single_clause/eight_shards`: `451.8 us`
- `execution_single_clause/sixty_four_shards`: `19.87 ms`
- `execution_single_clause/high_shard`: `64.7 us`
- `execution_intersection/sparse`: `136.3 us`
- `execution_intersection/dense`: `1.794 ms`
- `execution_intersection/union_like_many_shards`: `41.47 ms`
- `execution_clip_and_empty/empty_clause_set_full_range`: `99.2 us`
- `execution_clip_and_empty/first_edge`: `13.28 ms`
- `execution_clip_and_empty/last_edge`: `13.18 ms`
- `execution_clip_and_empty/both_edges`: `1.785 ms`

- `clause_loading_storage_shape/manifest_only_one_shard`: `4.72 us`
- `clause_loading_storage_shape/tail_only_many_shards`: `412 us`
- `clause_loading_storage_shape/manifest_plus_tail_many_shards_narrow`: `525 us`
- `clause_loading_storage_shape/high_shard_manifest_only`: `4.84 us`
- `clause_loading_or_width/1`: `241 us`
- `clause_loading_or_width/4`: `1.437 ms`
- `clause_loading_or_width/16`: `6.301 ms`
- `clause_loading_or_width/64`: `23.18 ms`
- `clause_loading_or_width/256`: `89.98 ms`
- `clause_loading_density/sparse_chunks`: `15.8 us`
- `clause_loading_density/dense_chunks`: `7.45 us`

- `materialize_cache_shape/cold_cache`: `403 ns`
- `materialize_cache_shape/warm_bucket_cold_header`: `186 ns`
- `materialize_cache_shape/warm_bucket_warm_header`: `186 ns`
- `materialize_locality/same_block_warm`: `1.49 us`
- `materialize_locality/cross_block_warm`: `6.13 us`
- `materialize_boundary_cases/block_spans_multiple_directory_buckets_warm`: `193 ns`
- `materialize_boundary_cases/high_shard_cold`: `408 ns`

- `query_end_to_end_narrow/address_and_topics`: `198 us`
- `query_end_to_end_indexed_mix/multi_clause_intersection`: `662 us`
- `query_end_to_end_indexed_mix/4`: `3.38 ms`
- `query_end_to_end_indexed_mix/16`: `9.39 ms`
- `query_end_to_end_indexed_mix/64`: `10.15 ms`
- `query_end_to_end_indexed_mix/256`: `12.68 ms`
- `query_end_to_end_pagination/resume_token_walk`: `835 us`

### Bottleneck Evidence

- Execution cost grows dramatically with shard fanout: `1 shard -> 8 shards` is about `8.3x`, and `8 shards -> 64 shards` is about `44x`.
- Many-shard union/intersection work (`41.47 ms`) is substantially more expensive than dense two-set intersection (`1.794 ms`), which is consistent with shard-heavy regression sensitivity.
- Clause-loading OR width scales near-linearly on a log-log view from `1` through `256`, reaching `89.98 ms` at width `256`.
- High-shard-only clause loading (`4.84 us`) is essentially identical to low-shard manifest-only loading (`4.72 us`), which suggests shard-ID width itself is not causing aliasing or pathological overhead.
- Materialization shows strong locality effects: same-block warm (`1.49 us`) is roughly `4x` faster than cross-block warm (`6.13 us`), while cold and high-shard cold point loads both remain around `0.4 us`.
- End-to-end indexed OR queries increase from `3.38 ms` at width `4` to `12.68 ms` at width `256`, but not as steeply as clause-loading microbenches, implying other fixed query costs amortize part of the loading overhead.

### Interpretation

- Most measured shapes look expected.
- High-shard-ID behavior looks healthy: high-shard execution and clause-loading cases were close to their low-shard counterparts when shard count was held constant.
- Materialization numbers are suspiciously fast in absolute terms because the benches use in-memory stores and very small synthetic payloads; these results are still useful for relative warm/cold and locality comparisons, but not for absolute latency expectations.
- The most suspicious result is `execution_clip_and_empty/{first_edge,last_edge}` being much slower than `both_edges`. That likely reflects the synthetic fixture keeping nearly full bitmaps alive for many shards and only clipping one side, so the executor still iterates a very large surviving set. This is plausible, but it is the first result worth double-checking if future changes affect clipping behavior.
- `clause_loading_density/dense_chunks` being faster than `sparse_chunks` is plausible here because the dense case uses one contiguous bitmap that deserializes/merges efficiently, while the sparse case pays more per-entry overhead. It is not obviously a bug.
- The OR-width results are large but not surprising; `256` terms already reaches `~90 ms` in clause loading alone, so very wide indexed OR filters are clearly expensive even before full query execution.

## 2026-03-16T22:38:01Z - Zero-copy bytes cache rollout and service wiring

### Change Summary

- Added per-table byte-budget config for the immutable bytes cache and implemented bounded per-table LRU eviction.
- Wired `FinalizedHistoryService` to use a service-owned bytes cache on the normal query path.
- Extended immutable read-path caching to block-log blobs and sealed stream-page meta/blob reads.

### Hypothesis

- Enabling the configured bytes cache in the actual service query path should reduce repeated immutable artifact fetches and slightly improve warm materialization latency without changing query semantics.

### Commands

```bash
cargo bench -p finalized-history-query --bench materialize_bench -- materialize_cache_shape/warm_bucket_warm_header --warm-up-time 0.1 --measurement-time 0.2 --sample-size 10
```

Baseline was run from a detached `HEAD` worktree at `8747535` after applying the same no-op benchmark helper fix needed to compile the bench with the already-landed `LogMaterializer::new(..., cache)` signature. Current was run from the working tree after the cache rollout.

### Environment

- Local Criterion run on the development machine.
- Plot output used the plotters backend because `gnuplot` was not installed.
- The detached baseline worktree required `/tmp/monad-bft -> /Users/jh/work/monad-bft` so the workspace's relative path dependencies resolved.

### Workload Shape

- Benchmark: `materialize_cache_shape/warm_bucket_warm_header`
- Store backend: in-memory meta/blob stores
- Access pattern: repeated warm point materialization from the same block after priming the materializer once

### Before / After Metrics

- Baseline `8747535`: `222.39 ns` median
- Current working tree: `217.99 ns` median
- Delta: `-4.40 ns` (`-1.98%`)

### Bottleneck Evidence

- The measured delta is small, which is consistent with this benchmark already operating on warm in-memory data and mostly exercising internal decode/materialization overhead rather than remote storage latency.
- The larger practical win from this change is reduction of repeated immutable blob/meta fetches in the actual service query path, which the new tests validate directly.

### Interpretation

- This change does not produce a dramatic microbenchmark shift on the warm same-block materialization path; that path was already cheap.
- The measured improvement is directionally positive and does not indicate regression.
- The more important behavior change is architectural: cache budgets now exist per immutable table, disabled tables bypass the cache cleanly, and repeated service queries can reuse cached immutable artifacts instead of always falling through the no-op cache.

### Methodology Learnings

- Criterion with `0.2s` measurement windows is enough to catch directionality on sub-microsecond materialization benches, but the absolute delta is noisy; treat anything near `~2%` as suggestive rather than decisive.
- Detached worktrees are useful for baseline comparisons in this repo, but the workspace's relative external dependencies must resolve identically or the baseline run will fail before the benchmark starts.

## 2026-03-16T23:11:06Z - Split stream-page tables and switch block payload caching to point reads

### Change Summary

- Split the stream-page cache into distinct `StreamPageMeta` and `StreamPageBlobs` tables.
- Removed whole-block `block_logs/*` caching from `LogMaterializer`.
- Added point-log payload caching keyed per resolved log record while keeping cold reads on exact `read_range(...)`.

### Hypothesis

- This should remove the enabled-cache full-blob over-fetch regression for sparse block-range queries and make stream-page cache budgets tunable independently, even if the warm same-log microbench gets a bit slower.

### Commands

```bash
cargo bench -p finalized-history-query --bench materialize_bench -- materialize_cache_shape/warm_bucket_warm_header --warm-up-time 0.1 --measurement-time 0.2 --sample-size 10
```

Baseline was the pre-change `HEAD` at `b9d326b`. Current was the working tree after the structural cache change.

### Environment

- Local Criterion run on the development machine.
- Plot output used the plotters backend because `gnuplot` was not installed.

### Workload Shape

- Benchmark: `materialize_cache_shape/warm_bucket_warm_header`
- Store backend: in-memory meta/blob stores
- Access pattern: repeated warm point materialization of the same log from the same block after one priming read

### Before / After Metrics

- Baseline `b9d326b`: `223.45 ns` median
- Current working tree: `269.29 ns` median
- Delta: `+45.84 ns` (`+20.51%`)

### Bottleneck Evidence

- This benchmark favors the old whole-block cache shape because it repeatedly reads the exact same log after the first warm-up in an in-memory backend.
- The new design now performs point-payload cache lookups and exact range reads on the cold path instead of promoting the entire block blob into cache, so it gives up some same-log warm-path speed in exchange for fixing sparse-query over-fetch behavior.

### Interpretation

- The regression on this particular warm microbench is expected and acceptable for the architectural change being made.
- The important win here is not raw same-log warm latency; it is eliminating the pathological “small nonzero cache budget forces full blob fetches” regime and separating stream-page metadata residency from blob residency.

### Methodology Learnings

- This benchmark is useful for catching warm-path regressions, but it underweights the sparse one-log-per-block workload that motivated the design change.
- For the next pass, the better measurement will be a same-block contiguous-log benchmark so the coalescing work can be judged against the exact workload it is meant to help.

## 2026-03-16T23:16:10Z - Coalesce contiguous same-block log reads into one range read

### Change Summary

- Added same-block contiguous-run loading in `LogMaterializer`.
- Updated the indexed query executor to group adjacent resolved log IDs from the same block and materialize the whole run with one `read_range(...)`.
- Added unit and end-to-end tests proving that contiguous same-block logs collapse to one range read and populate the point-payload cache.

### Hypothesis

- Query-time grouping of contiguous same-block log IDs should reduce redundant range reads for locality-heavy matches even if existing single-log materialization microbenches do not improve.

### Commands

```bash
cargo bench -p finalized-history-query --bench materialize_bench -- materialize_locality/same_block_warm --warm-up-time 0.1 --measurement-time 0.2 --sample-size 10
```

Baseline was commit `6d350d8`. Current was the working tree after contiguous-run coalescing.

### Environment

- Local Criterion run on the development machine.
- Plot output used the plotters backend because `gnuplot` was not installed.

### Workload Shape

- Benchmark: `materialize_locality/same_block_warm`
- Store backend: in-memory meta/blob stores
- Access pattern: repeated `load_by_id(...)` calls within one block

### Before / After Metrics

- Baseline `6d350d8`: `2.4946 us` median
- Current working tree: `2.8332 us` median
- Delta: `+338.6 ns` (`+13.57%`)

### Bottleneck Evidence

- This benchmark drives `LogMaterializer::load_by_id(...)` directly, so it does not exercise the new query-executor grouping path that detects and batches contiguous same-block runs.
- The new code adds run-building and cache-plumbing surface area, so it is plausible for this proxy benchmark to regress while the grouped query path improves on real contiguous candidate sets.

### Interpretation

- The benchmark regression is real for this proxy shape, but it is not the workload the change was designed to optimize.
- The relevant correctness/performance evidence for this commit is the new coverage proving a single `read_range(...)` for contiguous same-block query results.

### Methodology Learnings

- Existing materialization benches are not sufficient to judge query-level contiguous-run coalescing because they bypass the query executor entirely.
- If this path needs further tuning, the next useful benchmark should live closer to `query_logs` and seed a block with several contiguous matching logs in one result page.

## 2026-03-17T00:27:05Z - Add grouped query benchmark for storage access patterns

### Change Summary

- Extracted same-block contiguous-run collection in the query executor into a helper for readability.
- Added `query_end_to_end_storage_patterns/*` Criterion cases on the real `query_logs(...)` path.
- Added a counting blob-store bench harness that includes `get_blob` calls, `read_range` calls, and `read_range` bytes in the measured result.

### Hypothesis

- A query-level benchmark is needed to judge the grouped contiguous-run path accurately because materializer-only microbenches do not exercise the executor grouping logic.

### Commands

```bash
cargo bench -p finalized-history-query --bench query_end_to_end_bench -- query_end_to_end_storage_patterns/same_block_contiguous/warm --warm-up-time 0.05 --measurement-time 0.1 --sample-size 10
```

### Environment

- Local Criterion run on the development machine.
- Plot output used the plotters backend because `gnuplot` was not installed.

### Workload Shape

- Benchmark: `query_end_to_end_storage_patterns/same_block_contiguous/warm`
- Store backend: in-memory meta store plus counting blob store
- Access pattern: one block with contiguous matching logs queried through `FinalizedHistoryService::query_logs(...)` after one warm-up run
- Measured result payload includes query result length plus blob access counters snapshot

### Metrics

- `query_end_to_end_storage_patterns/same_block_contiguous/warm`: `20.019 us` median

### Bottleneck Evidence

- This benchmark finally exercises the grouped query path directly rather than `load_by_id(...)` in isolation.
- The counting blob store is wired into the benchmark result path, so the fixture can be extended to compare latency against storage-access counts without changing the production query path.

### Interpretation

- This is the right benchmark layer for future tuning of contiguous-run coalescing.
- One benchmark case is now in place; the remaining storage-pattern cases in the bench file provide the fuller cold/warm sparse/non-contiguous/mixed matrix.

### Methodology Learnings

- The earlier bench failure surfaced an unrelated fixture bug: one-byte synthetic block hashes were colliding in larger seeded benches. Bench fixtures now use full-width block hashes so longer query benches remain valid.
