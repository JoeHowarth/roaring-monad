#!/usr/bin/env bash
set -euo pipefail

RM_ROOT="${RM_ROOT:-/home/jhow/roaring-monad}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
ARCHIVE_ROOT="${ARCHIVE_ROOT:-$RM_ROOT/data/archive-mainnet-deu-009-0}"
START_BLOCK="${START_BLOCK:-57212000}"
END_BLOCK="${END_BLOCK:-57212500}"
TARGET_GB="${TARGET_GB:-100}"
TARGET_BYTES="${TARGET_BYTES:-$((TARGET_GB * 1024 * 1024 * 1024))}"
INGEST_TIMEOUT_SECONDS="${INGEST_TIMEOUT_SECONDS:-3600}"
MAX_ITERS="${MAX_ITERS:-10000}"

LOG_DIR="$RM_ROOT/logs"
RESULTS_DIR="$LOG_DIR/results"
STATE_FILE="${STATE_FILE:-$LOG_DIR/scale-state-$RUN_ID.env}"
TABLE_FILE="${TABLE_FILE:-$RESULTS_DIR/size-growth-$RUN_ID.md}"
PROGRESS_LOG="$LOG_DIR/progress.log"

mkdir -p "$LOG_DIR" "$RESULTS_DIR"

to_keyspace_safe() {
  local s
  s="$(echo "$1" | tr '[:upper:]-' '[:lower:]_')"
  echo "$s"
}

if [[ ! -f "$TABLE_FILE" ]]; then
  {
    echo "# Size Growth ($RUN_ID)"
    echo
    echo "| Iter | Timestamp UTC | Keyspace | Range | Scylla Bytes | MinIO Bytes | Total Bytes | Total GiB |"
    echo "| --- | --- | --- | --- | ---: | ---: | ---: | ---: |"
  } >"$TABLE_FILE"
fi

ITER=0
if [[ -f "$STATE_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$STATE_FILE"
fi

while (( ITER < MAX_ITERS )); do
  scylla_bytes=0
  minio_bytes=0
  if [[ -d "$RM_ROOT/infra/data/distributed/scylla" ]]; then
    scylla_bytes="$(du -sb "$RM_ROOT/infra/data/distributed/scylla" | awk '{print $1}')"
  fi
  if [[ -d "$RM_ROOT/infra/data/distributed/minio" ]]; then
    minio_bytes="$(du -sb "$RM_ROOT/infra/data/distributed/minio" | awk '{print $1}')"
  fi
  total_bytes="$((scylla_bytes + minio_bytes))"
  if (( total_bytes >= TARGET_BYTES )); then
    echo "$(date -u +%FT%TZ) :: size target reached bytes=$total_bytes target=$TARGET_BYTES run_id=$RUN_ID" | tee -a "$PROGRESS_LOG"
    break
  fi

  iter_tag="$(printf 'i%05d' "$ITER")"
  keyspace_raw="fi_scale_${RUN_ID}_${iter_tag}"
  keyspace="$(to_keyspace_safe "$keyspace_raw")"
  prefix="runs/$RUN_ID/scale/$iter_tag"
  ingest_log="$LOG_DIR/scale-ingest-$RUN_ID-$iter_tag.log"
  ingest_json="$RESULTS_DIR/scale-ingest-$RUN_ID-$iter_tag.json"

  echo "$(date -u +%FT%TZ) :: scale iteration start iter=$ITER keyspace=$keyspace" | tee -a "$PROGRESS_LOG"

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
      --target-entries-per-chunk 1950 \
      --target-chunk-bytes 32768 \
      --maintenance-seal-seconds 600 \
      --run-maintenance-every-blocks 500 \
      --log-every 250 \
      --output-json "$ingest_json" \
      >"$ingest_log" 2>&1
  rc=$?
  set -e

  if (( rc != 0 )); then
    echo "$(date -u +%FT%TZ) :: scale iteration failed iter=$ITER rc=$rc log=$ingest_log" | tee -a "$PROGRESS_LOG"
    {
      echo "ITER=$ITER"
      echo "LAST_RC=$rc"
    } >"$STATE_FILE"
    exit "$rc"
  fi

  scylla_bytes="$(du -sb "$RM_ROOT/infra/data/distributed/scylla" | awk '{print $1}')"
  minio_bytes="$(du -sb "$RM_ROOT/infra/data/distributed/minio" | awk '{print $1}')"
  total_bytes="$((scylla_bytes + minio_bytes))"
  total_gib="$(awk -v b="$total_bytes" 'BEGIN { printf "%.2f", b / (1024*1024*1024) }')"

  {
    echo "| $ITER | $(date -u +%FT%TZ) | $keyspace | $START_BLOCK-$END_BLOCK | $scylla_bytes | $minio_bytes | $total_bytes | $total_gib |"
  } >>"$TABLE_FILE"

  {
    echo "ITER=$((ITER + 1))"
    echo "LAST_RC=0"
    echo "LAST_TOTAL_BYTES=$total_bytes"
    echo "LAST_TABLE_FILE=$TABLE_FILE"
  } >"$STATE_FILE"

  echo "$(date -u +%FT%TZ) :: scale iteration done iter=$ITER total_bytes=$total_bytes total_gib=$total_gib" | tee -a "$PROGRESS_LOG"

  ITER=$((ITER + 1))
done
