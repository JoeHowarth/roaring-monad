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
