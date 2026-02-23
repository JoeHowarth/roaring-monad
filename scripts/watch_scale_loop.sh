#!/usr/bin/env bash
set -euo pipefail

RM_ROOT="${RM_ROOT:-/home/jhow/roaring-monad}"
RUN_ID="${RUN_ID:-20260223T073907Z-scale-geom}"
CHECK_INTERVAL_SECONDS="${CHECK_INTERVAL_SECONDS:-60}"
RUN_MAINTENANCE_EVERY_BLOCKS="${RUN_MAINTENANCE_EVERY_BLOCKS:-0}"
SKIP_FINAL_MAINTENANCE="${SKIP_FINAL_MAINTENANCE:-true}"
MAX_RETRIES_PER_ITER="${MAX_RETRIES_PER_ITER:-5}"
RETRY_DELAY_SECONDS="${RETRY_DELAY_SECONDS:-15}"
SOURCE_LATEST_TIMEOUT_SECONDS="${SOURCE_LATEST_TIMEOUT_SECONDS:-120}"
MIN_AVAILABLE_SPAN_BEFORE_WRAP="${MIN_AVAILABLE_SPAN_BEFORE_WRAP:-2000}"

LOG_DIR="$RM_ROOT/logs"
PID_FILE="$LOG_DIR/scale-to-target-$RUN_ID.pid"
OUT_FILE="$LOG_DIR/scale-to-target-$RUN_ID.out"
PROGRESS_LOG="$LOG_DIR/progress.log"

mkdir -p "$LOG_DIR"

while true; do
  pid=""
  if [[ -f "$PID_FILE" ]]; then
    pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  fi

  if [[ -n "$pid" ]] && ps -p "$pid" >/dev/null 2>&1; then
    sleep "$CHECK_INTERVAL_SECONDS"
    continue
  fi

  echo "$(date -u +%FT%TZ) :: watchdog restarting scaler run_id=$RUN_ID previous_pid=${pid:-none}" | tee -a "$PROGRESS_LOG"
  (
    cd "$RM_ROOT"
    RUN_ID="$RUN_ID" \
    RUN_MAINTENANCE_EVERY_BLOCKS="$RUN_MAINTENANCE_EVERY_BLOCKS" \
    SKIP_FINAL_MAINTENANCE="$SKIP_FINAL_MAINTENANCE" \
    MAX_RETRIES_PER_ITER="$MAX_RETRIES_PER_ITER" \
    RETRY_DELAY_SECONDS="$RETRY_DELAY_SECONDS" \
    SOURCE_LATEST_TIMEOUT_SECONDS="$SOURCE_LATEST_TIMEOUT_SECONDS" \
    MIN_AVAILABLE_SPAN_BEFORE_WRAP="$MIN_AVAILABLE_SPAN_BEFORE_WRAP" \
    nohup setsid ./scripts/scale_to_target_size.sh >> "$OUT_FILE" 2>&1 &
    echo $! > "$PID_FILE"
    echo "$(date -u +%FT%TZ) :: watchdog started scaler run_id=$RUN_ID pid=$(cat "$PID_FILE")" | tee -a "$PROGRESS_LOG"
  )

  sleep "$CHECK_INTERVAL_SECONDS"
done
