#!/usr/bin/env bash
set -euo pipefail

RM_ROOT="${RM_ROOT:-/home/jhow/roaring-monad}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-pfill}"
ARCHIVE_ROOT="${ARCHIVE_ROOT:-$RM_ROOT/data/archive-mainnet-deu-009-0}"
START_BLOCK="${START_BLOCK:-56536000}"
END_BLOCK="${END_BLOCK:-56601535}"
WORKERS="${WORKERS:-1}"
TARGET_GB="${TARGET_GB:-100}"
TARGET_BYTES="${TARGET_BYTES:-$((TARGET_GB * 1024 * 1024 * 1024))}"
INGEST_TIMEOUT_SECONDS="${INGEST_TIMEOUT_SECONDS:-7200}"
MAX_JOBS="${MAX_JOBS:-100000}"
TARGET_ENTRIES_PER_CHUNK="${TARGET_ENTRIES_PER_CHUNK:-1950}"
TARGET_CHUNK_BYTES="${TARGET_CHUNK_BYTES:-32768}"
MAINTENANCE_SEAL_SECONDS="${MAINTENANCE_SEAL_SECONDS:-1800}"
RUN_MAINTENANCE_EVERY_BLOCKS="${RUN_MAINTENANCE_EVERY_BLOCKS:-0}"
SKIP_FINAL_MAINTENANCE="${SKIP_FINAL_MAINTENANCE:-true}"
BACKOFF_SECONDS_ON_FAIL="${BACKOFF_SECONDS_ON_FAIL:-20}"

LOG_DIR="$RM_ROOT/logs"
RESULTS_DIR="$LOG_DIR/results"
PROGRESS_LOG="$LOG_DIR/progress.log"
TABLE_FILE="$RESULTS_DIR/parallel-fill-$RUN_ID.md"
COUNTER_FILE="$LOG_DIR/parallel-fill-$RUN_ID.counter"
LOCK_FILE="$LOG_DIR/parallel-fill-$RUN_ID.lock"
STOP_FILE="$LOG_DIR/parallel-fill-$RUN_ID.stop"
PID_FILE="$LOG_DIR/parallel-fill-$RUN_ID.pid"

mkdir -p "$LOG_DIR" "$RESULTS_DIR"

if [[ ! -f "$TABLE_FILE" ]]; then
  {
    echo "# Parallel Fill ($RUN_ID)"
    echo
    echo "| Job | Worker | Timestamp UTC | Keyspace | Range | Ingest Sec | Blocks/s | Logs/s | Total Bytes | Total GiB |"
    echo "| --- | ---: | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |"
  } >"$TABLE_FILE"
fi

: >"$COUNTER_FILE"
echo 0 >"$COUNTER_FILE"
rm -f "$STOP_FILE"

total_bytes() {
  local s=0
  if [[ -d "$RM_ROOT/infra/data/distributed/scylla" ]]; then
    s=$((s + $(du -sb "$RM_ROOT/infra/data/distributed/scylla" | awk '{print $1}')))
  fi
  if [[ -d "$RM_ROOT/infra/data/distributed/minio" ]]; then
    s=$((s + $(du -sb "$RM_ROOT/infra/data/distributed/minio" | awk '{print $1}')))
  fi
  echo "$s"
}

next_job_idx() {
  flock "$LOCK_FILE" bash -c '
    c=$(cat "$1")
    n=$((c + 1))
    echo "$n" > "$1"
    echo "$n"
  ' _ "$COUNTER_FILE"
}

worker_loop() {
  local wid="$1"
  while true; do
    if [[ -f "$STOP_FILE" ]]; then
      return 0
    fi

    local cur_total
    cur_total="$(total_bytes)"
    if (( cur_total >= TARGET_BYTES )); then
      touch "$STOP_FILE"
      echo "$(date -u +%FT%TZ) :: parallel-fill target reached bytes=$cur_total target=$TARGET_BYTES run_id=$RUN_ID" | tee -a "$PROGRESS_LOG"
      return 0
    fi

    local idx
    idx="$(next_job_idx)" || {
      echo "$(date -u +%FT%TZ) :: parallel-fill worker=$wid failed to allocate next job index" | tee -a "$PROGRESS_LOG"
      sleep "$BACKOFF_SECONDS_ON_FAIL"
      continue
    }
    if (( idx > MAX_JOBS )); then
      touch "$STOP_FILE"
      echo "$(date -u +%FT%TZ) :: parallel-fill max jobs reached idx=$idx run_id=$RUN_ID" | tee -a "$PROGRESS_LOG"
      return 0
    fi

    local job_tag
    job_tag="$(printf 'j%06d' "$idx")"
    local keyspace
    keyspace="fi_pf_${RUN_ID//[-:T]/_}_w${wid}_${job_tag}"
    keyspace="${keyspace,,}"
    local prefix
    prefix="runs/$RUN_ID/parallel/w$wid/$job_tag"
    local ingest_log
    ingest_log="$LOG_DIR/parallel-fill-$RUN_ID-w${wid}-${job_tag}.log"
    local ingest_json
    ingest_json="$RESULTS_DIR/parallel-fill-$RUN_ID-w${wid}-${job_tag}.json"

    echo "$(date -u +%FT%TZ) :: parallel-fill start worker=$wid job=$idx keyspace=$keyspace range=$START_BLOCK-$END_BLOCK" | tee -a "$PROGRESS_LOG"

    extra=()
    if [[ "$SKIP_FINAL_MAINTENANCE" == "true" ]]; then
      extra+=(--skip-final-maintenance)
    fi

    set +e
    timeout "${INGEST_TIMEOUT_SECONDS}s" env \
      TRIEDB_TARGET=triedb_driver \
      CC=/usr/bin/gcc-15 \
      CXX=/usr/bin/g++-15 \
      ASMFLAGS='-march=haswell' \
      CFLAGS='-march=haswell' \
      CXXFLAGS='-march=haswell' \
      cargo +1.91.1 run --release -p benchmarking -- ingest-distributed \
        --archive-root "$ARCHIVE_ROOT" \
        --start-block "$START_BLOCK" \
        --end-block "$END_BLOCK" \
        --chain-id 1 \
        --scylla-node 127.0.0.1:9042 \
        --scylla-keyspace "$keyspace" \
        --minio-endpoint http://127.0.0.1:9000 \
        --minio-region us-east-1 \
        --minio-access-key minioadmin \
        --minio-secret-key minioadmin \
        --minio-bucket finalized-index-bench \
        --minio-prefix "$prefix" \
        --writer-epoch 1 \
        --target-entries-per-chunk "$TARGET_ENTRIES_PER_CHUNK" \
        --target-chunk-bytes "$TARGET_CHUNK_BYTES" \
        --maintenance-seal-seconds "$MAINTENANCE_SEAL_SECONDS" \
        --run-maintenance-every-blocks "$RUN_MAINTENANCE_EVERY_BLOCKS" \
        --log-every 500 \
        "${extra[@]}" \
        --output-json "$ingest_json" \
        >"$ingest_log" 2>&1
    rc=$?
    set -e

    if (( rc != 0 )); then
      echo "$(date -u +%FT%TZ) :: parallel-fill failed worker=$wid job=$idx rc=$rc log=$ingest_log" | tee -a "$PROGRESS_LOG"
      sleep "$BACKOFF_SECONDS_ON_FAIL"
      continue
    fi

    ingest_elapsed="$(jq -r '.elapsed_seconds // 0' "$ingest_json" 2>/dev/null || echo 0)"
    ingest_bps="$(jq -r '.blocks_per_second // 0' "$ingest_json" 2>/dev/null || echo 0)"
    ingest_lps="$(jq -r '.logs_per_second // 0' "$ingest_json" 2>/dev/null || echo 0)"
    cur_total="$(total_bytes)"
    cur_gib="$(awk -v b="$cur_total" 'BEGIN { printf "%.2f", b/(1024*1024*1024) }')"

    {
      echo "| $idx | $wid | $(date -u +%FT%TZ) | $keyspace | $START_BLOCK-$END_BLOCK | $ingest_elapsed | $ingest_bps | $ingest_lps | $cur_total | $cur_gib |"
    } >>"$TABLE_FILE"

    echo "$(date -u +%FT%TZ) :: parallel-fill done worker=$wid job=$idx bps=$ingest_bps lps=$ingest_lps total_bytes=$cur_total" | tee -a "$PROGRESS_LOG"
  done
}

worker_pids=()
for w in $(seq 1 "$WORKERS"); do
  ( worker_loop "$w" ) &
  worker_pids+=($!)
  echo $! >>"$PID_FILE"
done

for pid in "${worker_pids[@]}"; do
  if ! wait "$pid"; then
    echo "$(date -u +%FT%TZ) :: parallel-fill worker process exited non-zero pid=$pid run_id=$RUN_ID" | tee -a "$PROGRESS_LOG"
  fi
done

echo "$(date -u +%FT%TZ) :: parallel-fill finished run_id=$RUN_ID" | tee -a "$PROGRESS_LOG"
