#!/usr/bin/env bash
set -euo pipefail

RM_ROOT="${RM_ROOT:-/home/jhow/roaring-monad}"
MA_ROOT="${MA_ROOT:-/home/jhow/monad-bft}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
ARCHIVE_ROOT="${ARCHIVE_ROOT:-$RM_ROOT/data/archive-mainnet-deu-009-0}"
ARCHIVE_SOURCE="${ARCHIVE_SOURCE:-aws mainnet-deu-009-0 50}"
MIRROR_BEFORE_INGEST="${MIRROR_BEFORE_INGEST:-true}"
MIRROR_METHOD="${MIRROR_METHOD:-archiver}"
ARCHIVER_MAX_BLOCKS_PER_ITERATION="${ARCHIVER_MAX_BLOCKS_PER_ITERATION:-200}"
ARCHIVER_MAX_CONCURRENT_BLOCKS="${ARCHIVER_MAX_CONCURRENT_BLOCKS:-128}"
START_BLOCK="${START_BLOCK:-56656000}"
END_BLOCK="${END_BLOCK:-56656500}"
INITIAL_SPAN="${INITIAL_SPAN:-0}"
GROWTH_FACTOR="${GROWTH_FACTOR:-2}"
MAX_SPAN="${MAX_SPAN:-65536}"
TARGET_GB="${TARGET_GB:-100}"
TARGET_BYTES="${TARGET_BYTES:-$((TARGET_GB * 1024 * 1024 * 1024))}"
INGEST_TIMEOUT_SECONDS="${INGEST_TIMEOUT_SECONDS:-3600}"
MIRROR_TIMEOUT_SECONDS="${MIRROR_TIMEOUT_SECONDS:-7200}"
MAX_ITERS="${MAX_ITERS:-10000}"
TARGET_ENTRIES_PER_CHUNK="${TARGET_ENTRIES_PER_CHUNK:-1950}"
TARGET_CHUNK_BYTES="${TARGET_CHUNK_BYTES:-32768}"
MAINTENANCE_SEAL_SECONDS="${MAINTENANCE_SEAL_SECONDS:-1800}"
RUN_MAINTENANCE_EVERY_BLOCKS="${RUN_MAINTENANCE_EVERY_BLOCKS:-2000}"
SKIP_FINAL_MAINTENANCE="${SKIP_FINAL_MAINTENANCE:-true}"
MAX_RETRIES_PER_ITER="${MAX_RETRIES_PER_ITER:-5}"
RETRY_DELAY_SECONDS="${RETRY_DELAY_SECONDS:-15}"
SOURCE_LATEST_TIMEOUT_SECONDS="${SOURCE_LATEST_TIMEOUT_SECONDS:-120}"
MIN_AVAILABLE_SPAN_BEFORE_WRAP="${MIN_AVAILABLE_SPAN_BEFORE_WRAP:-2000}"
BENCH_BIN="${BENCH_BIN:-$RM_ROOT/target/release/benchmarking}"
BENCH_USE_CARGO_RUN="${BENCH_USE_CARGO_RUN:-true}"

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

if [[ "$BENCH_USE_CARGO_RUN" == "true" || ! -x "$BENCH_BIN" ]]; then
  BENCH_CMD=(cargo +1.91.1 run --release -p benchmarking --)
else
  if "$BENCH_BIN" --help >/dev/null 2>&1; then
    BENCH_CMD=("$BENCH_BIN")
  else
    echo "$(date -u +%FT%TZ) :: benchmark binary unavailable; falling back to cargo run (BENCH_BIN=$BENCH_BIN)" | tee -a "$PROGRESS_LOG"
    BENCH_CMD=(cargo +1.91.1 run --release -p benchmarking --)
  fi
fi

if [[ ! -f "$TABLE_FILE" ]]; then
  {
    echo "# Size Growth ($RUN_ID)"
    echo
    echo "| Iter | Timestamp UTC | Keyspace | Range | Span | Mirror Sec | Ingest Sec | Blocks/s | Logs/s | Scylla Bytes | MinIO Bytes | Total Bytes | Total GiB |"
    echo "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
  } >"$TABLE_FILE"
fi

ITER=0
CUR_START_BLOCK="$START_BLOCK"
if (( INITIAL_SPAN > 0 )); then
  CUR_SPAN="$INITIAL_SPAN"
else
  CUR_SPAN="$((END_BLOCK - START_BLOCK + 1))"
fi
if [[ -f "$STATE_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$STATE_FILE"
fi
ATTEMPT="${ATTEMPT:-0}"

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
  desired_end_block="$((CUR_START_BLOCK + CUR_SPAN - 1))"
  source_latest_log="$LOG_DIR/source-latest-$RUN_ID.log"

  set +e
  latest_source_raw="$(
    timeout "${SOURCE_LATEST_TIMEOUT_SECONDS}s" env \
      TRIEDB_TARGET=triedb_driver \
      CC=/usr/bin/gcc-15 \
      CXX=/usr/bin/g++-15 \
      ASMFLAGS='-march=haswell' \
      CFLAGS='-march=haswell' \
      CXXFLAGS='-march=haswell' \
      "${BENCH_CMD[@]}" source-latest \
        --source "$ARCHIVE_SOURCE" \
        2>>"$source_latest_log"
  )"
  latest_source_rc=$?
  set -e

  latest_source="$(echo "$latest_source_raw" | tail -n 1 | tr -dc '0-9')"
  if (( latest_source_rc != 0 )) || [[ -z "$latest_source" ]]; then
    if (( ATTEMPT < MAX_RETRIES_PER_ITER )); then
      ATTEMPT="$((ATTEMPT + 1))"
      echo "$(date -u +%FT%TZ) :: source-latest retry iter=$ITER attempt=$ATTEMPT rc=$latest_source_rc" | tee -a "$PROGRESS_LOG"
      {
        echo "ITER=$ITER"
        echo "CUR_START_BLOCK=$CUR_START_BLOCK"
        echo "CUR_SPAN=$CUR_SPAN"
        echo "ATTEMPT=$ATTEMPT"
        echo "LAST_RC=$latest_source_rc"
        echo "LAST_TABLE_FILE=$TABLE_FILE"
      } >"$STATE_FILE"
      sleep "$RETRY_DELAY_SECONDS"
      continue
    fi

    echo "$(date -u +%FT%TZ) :: source-latest failed iter=$ITER rc=$latest_source_rc latest='$latest_source_raw'" | tee -a "$PROGRESS_LOG"
    {
      echo "ITER=$ITER"
      echo "CUR_START_BLOCK=$CUR_START_BLOCK"
      echo "CUR_SPAN=$CUR_SPAN"
      echo "ATTEMPT=$ATTEMPT"
      echo "LAST_RC=$latest_source_rc"
      echo "LAST_TABLE_FILE=$TABLE_FILE"
    } >"$STATE_FILE"
    exit "$latest_source_rc"
  fi

  if (( CUR_START_BLOCK > latest_source )); then
    echo "$(date -u +%FT%TZ) :: source head reached iter=$ITER start=$CUR_START_BLOCK latest_source=$latest_source; wrapping to START_BLOCK=$START_BLOCK" | tee -a "$PROGRESS_LOG"
    CUR_START_BLOCK="$START_BLOCK"
    desired_end_block="$((CUR_START_BLOCK + CUR_SPAN - 1))"
  fi

  if (( CUR_START_BLOCK <= latest_source )); then
    available_span="$((latest_source - CUR_START_BLOCK + 1))"
    if (( available_span < MIN_AVAILABLE_SPAN_BEFORE_WRAP )); then
      echo "$(date -u +%FT%TZ) :: low headroom iter=$ITER start=$CUR_START_BLOCK latest_source=$latest_source available_span=$available_span; wrapping to START_BLOCK=$START_BLOCK" | tee -a "$PROGRESS_LOG"
      CUR_START_BLOCK="$START_BLOCK"
      desired_end_block="$((CUR_START_BLOCK + CUR_SPAN - 1))"
    fi
  fi

  cur_end_block="$desired_end_block"
  if (( cur_end_block > latest_source )); then
    cur_end_block="$latest_source"
  fi
  if (( cur_end_block < CUR_START_BLOCK )); then
    echo "$(date -u +%FT%TZ) :: no source blocks available iter=$ITER start=$CUR_START_BLOCK latest_source=$latest_source; sleeping" | tee -a "$PROGRESS_LOG"
    sleep "$RETRY_DELAY_SECONDS"
    continue
  fi

  keyspace_raw="fi_scale_${RUN_ID}_${iter_tag}"
  keyspace="$(to_keyspace_safe "$keyspace_raw")"
  prefix="runs/$RUN_ID/scale/$iter_tag"
  mirror_log="$LOG_DIR/scale-mirror-$RUN_ID-$iter_tag.log"
  ingest_log="$LOG_DIR/scale-ingest-$RUN_ID-$iter_tag.log"
  ingest_json="$RESULTS_DIR/scale-ingest-$RUN_ID-$iter_tag.json"
  mirror_elapsed="0"

  echo "$(date -u +%FT%TZ) :: scale iteration start iter=$ITER attempt=$ATTEMPT keyspace=$keyspace range=${CUR_START_BLOCK}-${cur_end_block} span=$CUR_SPAN" | tee -a "$PROGRESS_LOG"

  if [[ "$MIRROR_BEFORE_INGEST" == "true" ]]; then
    mirror_started="$(date +%s)"

    if [[ "$MIRROR_METHOD" == "archiver" ]]; then
      set +e
      timeout "${MIRROR_TIMEOUT_SECONDS}s" bash -lc "
        set -euo pipefail
        cd '$MA_ROOT'
        cargo run -p monad-archive --bin monad-archiver -- \
          set-start-block --block '$((CUR_START_BLOCK - 1))' --archive-sink 'fs $ARCHIVE_ROOT' >/dev/null 2>&1
        cargo run -p monad-archive --bin monad-archiver -- \
          --block-data-source '$ARCHIVE_SOURCE' \
          --archive-sink 'fs $ARCHIVE_ROOT' \
          --max-blocks-per-iteration '$ARCHIVER_MAX_BLOCKS_PER_ITERATION' \
          --max-concurrent-blocks '$ARCHIVER_MAX_CONCURRENT_BLOCKS' \
          --stop-block '$cur_end_block'
      " >"$mirror_log" 2>&1
      mirror_rc=$?
      set -e
    else
      set +e
      timeout "${MIRROR_TIMEOUT_SECONDS}s" env \
        TRIEDB_TARGET=triedb_driver \
        CC=/usr/bin/gcc-15 \
        CXX=/usr/bin/g++-15 \
        ASMFLAGS='-march=haswell' \
        CFLAGS='-march=haswell' \
        CXXFLAGS='-march=haswell' \
        "${BENCH_CMD[@]}" mirror \
          --archive-root "$ARCHIVE_ROOT" \
          --source "$ARCHIVE_SOURCE" \
          --start-block "$CUR_START_BLOCK" \
          --end-block "$cur_end_block" \
          --log-every 500 \
          >"$mirror_log" 2>&1
      mirror_rc=$?
      set -e
    fi

    mirror_elapsed="$(( $(date +%s) - mirror_started ))"
    if (( mirror_rc != 0 )); then
      if (( ATTEMPT < MAX_RETRIES_PER_ITER )); then
        ATTEMPT="$((ATTEMPT + 1))"
        echo "$(date -u +%FT%TZ) :: scale mirror retry iter=$ITER attempt=$ATTEMPT rc=$mirror_rc log=$mirror_log" | tee -a "$PROGRESS_LOG"
        {
          echo "ITER=$ITER"
          echo "CUR_START_BLOCK=$CUR_START_BLOCK"
          echo "CUR_SPAN=$CUR_SPAN"
          echo "ATTEMPT=$ATTEMPT"
          echo "LAST_RC=$mirror_rc"
          echo "LAST_TABLE_FILE=$TABLE_FILE"
        } >"$STATE_FILE"
        sleep "$RETRY_DELAY_SECONDS"
        continue
      fi

      echo "$(date -u +%FT%TZ) :: scale mirror failed iter=$ITER rc=$mirror_rc log=$mirror_log" | tee -a "$PROGRESS_LOG"
      {
        echo "ITER=$ITER"
        echo "CUR_START_BLOCK=$CUR_START_BLOCK"
        echo "CUR_SPAN=$CUR_SPAN"
        echo "ATTEMPT=$ATTEMPT"
        echo "LAST_RC=$mirror_rc"
      } >"$STATE_FILE"
      exit "$mirror_rc"
    fi
  fi

  ingest_extra_args=()
  if [[ "$SKIP_FINAL_MAINTENANCE" == "true" ]]; then
    ingest_extra_args+=(--skip-final-maintenance)
  fi

  set +e
  timeout "${INGEST_TIMEOUT_SECONDS}s" env \
    TRIEDB_TARGET=triedb_driver \
    CC=/usr/bin/gcc-15 \
    CXX=/usr/bin/g++-15 \
    ASMFLAGS='-march=haswell' \
    CFLAGS='-march=haswell' \
    CXXFLAGS='-march=haswell' \
    "${BENCH_CMD[@]}" ingest-distributed \
      --archive-root "$ARCHIVE_ROOT" \
      --start-block "$CUR_START_BLOCK" \
      --end-block "$cur_end_block" \
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
      --log-every 250 \
      "${ingest_extra_args[@]}" \
      --output-json "$ingest_json" \
      >"$ingest_log" 2>&1
  rc=$?
  set -e

  if (( rc != 0 )); then
    if grep -q "invalid finalized sequence" "$ingest_log"; then
      next_iter="$((ITER + 1))"
      echo "$(date -u +%FT%TZ) :: scale iteration keyspace conflict iter=$ITER rc=$rc; retrying range with iter=$next_iter" | tee -a "$PROGRESS_LOG"
      {
        echo "ITER=$next_iter"
        echo "CUR_START_BLOCK=$CUR_START_BLOCK"
        echo "CUR_SPAN=$CUR_SPAN"
        echo "ATTEMPT=0"
        echo "LAST_RC=$rc"
        echo "LAST_TABLE_FILE=$TABLE_FILE"
      } >"$STATE_FILE"
      ATTEMPT=0
      ITER="$next_iter"
      continue
    fi

    if (( ATTEMPT < MAX_RETRIES_PER_ITER )); then
      ATTEMPT="$((ATTEMPT + 1))"
      echo "$(date -u +%FT%TZ) :: scale ingest retry iter=$ITER attempt=$ATTEMPT rc=$rc log=$ingest_log" | tee -a "$PROGRESS_LOG"
      {
        echo "ITER=$ITER"
        echo "CUR_START_BLOCK=$CUR_START_BLOCK"
        echo "CUR_SPAN=$CUR_SPAN"
        echo "ATTEMPT=$ATTEMPT"
        echo "LAST_RC=$rc"
        echo "LAST_TABLE_FILE=$TABLE_FILE"
      } >"$STATE_FILE"
      sleep "$RETRY_DELAY_SECONDS"
      continue
    fi

    echo "$(date -u +%FT%TZ) :: scale iteration failed iter=$ITER rc=$rc log=$ingest_log" | tee -a "$PROGRESS_LOG"
    {
      echo "ITER=$ITER"
      echo "CUR_START_BLOCK=$CUR_START_BLOCK"
      echo "CUR_SPAN=$CUR_SPAN"
      echo "ATTEMPT=$ATTEMPT"
      echo "LAST_RC=$rc"
      echo "LAST_TABLE_FILE=$TABLE_FILE"
    } >"$STATE_FILE"
    exit "$rc"
  fi

  scylla_bytes="$(du -sb "$RM_ROOT/infra/data/distributed/scylla" | awk '{print $1}')"
  minio_bytes="$(du -sb "$RM_ROOT/infra/data/distributed/minio" | awk '{print $1}')"
  total_bytes="$((scylla_bytes + minio_bytes))"
  total_gib="$(awk -v b="$total_bytes" 'BEGIN { printf "%.2f", b / (1024*1024*1024) }')"
  ingest_elapsed="$(jq -r '.elapsed_seconds // 0' "$ingest_json" 2>/dev/null || echo 0)"
  ingest_bps="$(jq -r '.blocks_per_second // 0' "$ingest_json" 2>/dev/null || echo 0)"
  ingest_lps="$(jq -r '.logs_per_second // 0' "$ingest_json" 2>/dev/null || echo 0)"

  {
    echo "| $ITER | $(date -u +%FT%TZ) | $keyspace | $CUR_START_BLOCK-$cur_end_block | $CUR_SPAN | $mirror_elapsed | $ingest_elapsed | $ingest_bps | $ingest_lps | $scylla_bytes | $minio_bytes | $total_bytes | $total_gib |"
  } >>"$TABLE_FILE"

  next_start_block="$((cur_end_block + 1))"
  next_span="$((CUR_SPAN * GROWTH_FACTOR))"
  if (( next_span > MAX_SPAN )); then
    next_span="$MAX_SPAN"
  fi

  {
    echo "ITER=$((ITER + 1))"
    echo "CUR_START_BLOCK=$next_start_block"
    echo "CUR_SPAN=$next_span"
    echo "ATTEMPT=0"
    echo "LAST_RC=0"
    echo "LAST_TOTAL_BYTES=$total_bytes"
    echo "LAST_TABLE_FILE=$TABLE_FILE"
  } >"$STATE_FILE"

  echo "$(date -u +%FT%TZ) :: scale iteration done iter=$ITER total_bytes=$total_bytes total_gib=$total_gib next_start=$next_start_block next_span=$next_span" | tee -a "$PROGRESS_LOG"

  CUR_START_BLOCK="$next_start_block"
  CUR_SPAN="$next_span"
  ATTEMPT=0
  ITER=$((ITER + 1))
done
