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
- Selected defaults: `log_locator_write_concurrency=256`, `stream_append_concurrency=64`.

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
