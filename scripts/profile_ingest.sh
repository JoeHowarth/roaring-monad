#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="$ROOT_DIR/logs"
RESULTS_DIR="$LOG_DIR/results"

RUN_TAG="${RUN_TAG:-manual}"
RUN_TS="${RUN_TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
RUN_ID="${RUN_TS}-${RUN_TAG}"

ARCHIVE_ROOT="${ARCHIVE_ROOT:-$ROOT_DIR/data/archive-mainnet-deu-009-0}"
START_BLOCK="${START_BLOCK:-56536000}"
END_BLOCK="${END_BLOCK:-56540000}"
CHAIN_ID="${CHAIN_ID:-1}"

SCYLLA_NODE="${SCYLLA_NODE:-127.0.0.1:9042}"
KEYSPACE_PREFIX="${KEYSPACE_PREFIX:-fi_profile}"
RAW_KEYSPACE="$(echo "${KEYSPACE_PREFIX}_${RUN_TS}_${RUN_TAG}" | tr '[:upper:]-' '[:lower:]_')"
if (( ${#RAW_KEYSPACE} > 48 )); then
  SCYLLA_KEYSPACE="${RAW_KEYSPACE:0:48}"
  echo "warning: truncated scylla keyspace to 48 chars: $SCYLLA_KEYSPACE" >&2
else
  SCYLLA_KEYSPACE="$RAW_KEYSPACE"
fi

MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://127.0.0.1:9000}"
MINIO_REGION="${MINIO_REGION:-us-east-1}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"
MINIO_BUCKET="${MINIO_BUCKET:-finalized-index-bench}"
MINIO_PREFIX="${MINIO_PREFIX:-runs/profile/${RUN_ID}}"

WRITER_EPOCH="${WRITER_EPOCH:-1}"
TARGET_ENTRIES_PER_CHUNK="${TARGET_ENTRIES_PER_CHUNK:-1950}"
TARGET_CHUNK_BYTES="${TARGET_CHUNK_BYTES:-32768}"
MAINTENANCE_SEAL_SECONDS="${MAINTENANCE_SEAL_SECONDS:-1800}"
RUN_MAINTENANCE_EVERY_BLOCKS="${RUN_MAINTENANCE_EVERY_BLOCKS:-0}"
INGEST_MODE="${INGEST_MODE:-single-writer-fast}"
TOPIC0_STATS_FLUSH_INTERVAL_BLOCKS="${TOPIC0_STATS_FLUSH_INTERVAL_BLOCKS:-64}"
ASSUME_EMPTY_STREAMS="${ASSUME_EMPTY_STREAMS:-true}"
LOG_LOCATOR_WRITE_CONCURRENCY="${LOG_LOCATOR_WRITE_CONCURRENCY:-256}"
STREAM_APPEND_CONCURRENCY="${STREAM_APPEND_CONCURRENCY:-96}"
LOG_EVERY="${LOG_EVERY:-500}"
SKIP_FINAL_MAINTENANCE="${SKIP_FINAL_MAINTENANCE:-true}"
APPEND_OPT_LOG="${APPEND_OPT_LOG:-false}"

mkdir -p "$LOG_DIR" "$RESULTS_DIR"

RESULT_JSON="$RESULTS_DIR/profile-ingest-${RUN_ID}.json"
INGEST_LOG="$LOG_DIR/profile-ingest-${RUN_ID}.log"
PIDSTAT_LOG="$LOG_DIR/profile-pidstat-${RUN_ID}.log"
IOSTAT_LOG="$LOG_DIR/profile-iostat-${RUN_ID}.log"
META_LOG="$LOG_DIR/profile-meta-${RUN_ID}.env"

LIB_PATHS="$(find "$ROOT_DIR/target/release/build" -maxdepth 3 -type d -path '*/out/build' | paste -sd: -)"
if [[ -z "$LIB_PATHS" ]]; then
  echo "missing release shared library paths under $ROOT_DIR/target/release/build" >&2
  exit 1
fi

SCYLLA_PID="$(pgrep -f '/usr/bin/scylla' | head -n1 || true)"
MINIO_PID="$(pgrep -f '^minio server' | head -n1 || true)"

{
  echo "run_ts=$RUN_TS"
  echo "run_id=$RUN_ID"
  echo "run_tag=$RUN_TAG"
  echo "archive_root=$ARCHIVE_ROOT"
  echo "start_block=$START_BLOCK"
  echo "end_block=$END_BLOCK"
  echo "scylla_node=$SCYLLA_NODE"
  echo "scylla_keyspace=$SCYLLA_KEYSPACE"
  echo "minio_prefix=$MINIO_PREFIX"
  echo "ingest_mode=$INGEST_MODE"
  echo "topic0_stats_flush_interval_blocks=$TOPIC0_STATS_FLUSH_INTERVAL_BLOCKS"
  echo "assume_empty_streams=$ASSUME_EMPTY_STREAMS"
  echo "log_locator_write_concurrency=$LOG_LOCATOR_WRITE_CONCURRENCY"
  echo "stream_append_concurrency=$STREAM_APPEND_CONCURRENCY"
  echo "lib_paths=$LIB_PATHS"
  echo "scylla_pid=$SCYLLA_PID"
  echo "minio_pid=$MINIO_PID"
} >"$META_LOG"

echo "profiling run_id=$RUN_ID keyspace=$SCYLLA_KEYSPACE range=${START_BLOCK}..${END_BLOCK}"

START_SECS="$(date +%s)"
extra_args=()
if [[ "$SKIP_FINAL_MAINTENANCE" == "true" ]]; then
  extra_args+=(--skip-final-maintenance)
fi
if [[ "$ASSUME_EMPTY_STREAMS" == "true" ]]; then
  extra_args+=(--assume-empty-streams)
fi

env LD_LIBRARY_PATH="$LIB_PATHS:${LD_LIBRARY_PATH:-}" \
  "$ROOT_DIR/target/release/benchmarking" ingest-distributed \
  --archive-root "$ARCHIVE_ROOT" \
  --start-block "$START_BLOCK" \
  --end-block "$END_BLOCK" \
  --chain-id "$CHAIN_ID" \
  --scylla-node "$SCYLLA_NODE" \
  --scylla-keyspace "$SCYLLA_KEYSPACE" \
  --minio-endpoint "$MINIO_ENDPOINT" \
  --minio-region "$MINIO_REGION" \
  --minio-access-key "$MINIO_ACCESS_KEY" \
  --minio-secret-key "$MINIO_SECRET_KEY" \
  --minio-bucket "$MINIO_BUCKET" \
  --minio-prefix "$MINIO_PREFIX" \
  --writer-epoch "$WRITER_EPOCH" \
  --target-entries-per-chunk "$TARGET_ENTRIES_PER_CHUNK" \
  --target-chunk-bytes "$TARGET_CHUNK_BYTES" \
  --maintenance-seal-seconds "$MAINTENANCE_SEAL_SECONDS" \
  --run-maintenance-every-blocks "$RUN_MAINTENANCE_EVERY_BLOCKS" \
  --ingest-mode "$INGEST_MODE" \
  --topic0-stats-flush-interval-blocks "$TOPIC0_STATS_FLUSH_INTERVAL_BLOCKS" \
  --log-locator-write-concurrency "$LOG_LOCATOR_WRITE_CONCURRENCY" \
  --stream-append-concurrency "$STREAM_APPEND_CONCURRENCY" \
  --log-every "$LOG_EVERY" \
  "${extra_args[@]}" \
  --output-json "$RESULT_JSON" \
  >"$INGEST_LOG" 2>&1 &
BENCH_PID=$!
echo "bench_pid=$BENCH_PID" >>"$META_LOG"

PID_LIST="$BENCH_PID"
if [[ -n "$SCYLLA_PID" ]]; then
  PID_LIST="$PID_LIST,$SCYLLA_PID"
fi
if [[ -n "$MINIO_PID" ]]; then
  PID_LIST="$PID_LIST,$MINIO_PID"
fi

pidstat -urdh -p "$PID_LIST" 1 >"$PIDSTAT_LOG" &
PIDSTAT_PID=$!
iostat -xz 1 >"$IOSTAT_LOG" &
IOSTAT_PID=$!

wait "$BENCH_PID"
BENCH_STATUS=$?
END_SECS="$(date +%s)"

kill "$PIDSTAT_PID" "$IOSTAT_PID" 2>/dev/null || true
wait "$PIDSTAT_PID" "$IOSTAT_PID" 2>/dev/null || true

if [[ "$BENCH_STATUS" -ne 0 ]]; then
  echo "ingest profiling failed run_id=$RUN_ID; see $INGEST_LOG" >&2
  exit "$BENCH_STATUS"
fi

WALL_SECONDS="$((END_SECS - START_SECS))"
echo "wall_seconds=$WALL_SECONDS" >>"$META_LOG"

BENCH_AVG_LINE="$(
  awk '
    $1 ~ /^[0-9]/ && $2 ~ /AM|PM/ && $4 ~ /^[0-9]+$/ {
      pid=$4; usr=$5+0; sys=$6+0; wait=$8+0; cpu=$9+0; cmd=$20;
      n[pid]++; su[pid]+=usr; ss[pid]+=sys; sw[pid]+=wait; sc[pid]+=cpu; c[pid]=cmd;
    }
    END {
      for (pid in n) {
        printf("pid=%s cmd=%s samples=%d avg_cpu=%.2f avg_wait=%.2f avg_usr=%.2f avg_sys=%.2f\n",
          pid, c[pid], n[pid], sc[pid]/n[pid], sw[pid]/n[pid], su[pid]/n[pid], ss[pid]/n[pid]);
      }
    }' "$PIDSTAT_LOG" | sort
)"

NVME_LINE="$(
  awk '
    $1 == "nvme0n1" {
      n++; util+=$NF; aqu+=$(NF-1); wk+=$9; rk+=$3;
    }
    END {
      if (n > 0) {
        printf("nvme0n1 samples=%d avg_util=%.2f avg_aqu=%.2f avg_wkB_s=%.2f avg_rkB_s=%.2f",
          n, util/n, aqu/n, wk/n, rk/n);
      }
    }' "$IOSTAT_LOG"
)"

SUMMARY_LINE="$(
  jq -r '"profile_result run_id='"$RUN_ID"' blocks=\(.blocks_ingested) logs=\(.logs_ingested) elapsed_s=\(.elapsed_seconds) bps=\(.blocks_per_second) lps=\(.logs_per_second) mode=\(.ingest_mode) flush=\(.topic0_stats_flush_interval_blocks) assume_empty=\(.assume_empty_streams) locator_c=\(.log_locator_write_concurrency) stream_c=\(.stream_append_concurrency)"' "$RESULT_JSON"
)"

echo "$SUMMARY_LINE"
echo "$BENCH_AVG_LINE"
echo "$NVME_LINE"

if [[ "$APPEND_OPT_LOG" == "true" ]]; then
  {
    echo
    echo "## ${RUN_TS} - Profiling Run ${RUN_TAG}"
    echo
    echo "### Commands"
    echo
    echo "\`scripts/profile_ingest.sh\`"
    echo
    echo "### Metrics"
    echo
    echo "- $SUMMARY_LINE"
    while IFS= read -r line; do
      [[ -n "$line" ]] && echo "- $line"
    done <<<"$BENCH_AVG_LINE"
    [[ -n "$NVME_LINE" ]] && echo "- $NVME_LINE"
    echo
    echo "### Artifacts"
    echo
    echo "- \`$RESULT_JSON\`"
    echo "- \`$PIDSTAT_LOG\`"
    echo "- \`$IOSTAT_LOG\`"
    echo "- \`$INGEST_LOG\`"
    echo "- \`$META_LOG\`"
  } >>"$ROOT_DIR/OPTIMIZATION_LOG.md"
fi
