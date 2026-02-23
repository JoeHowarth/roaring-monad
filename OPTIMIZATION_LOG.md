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
