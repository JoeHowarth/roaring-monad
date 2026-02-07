#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT/infra/docker-compose.distributed.yml"

if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE=(docker-compose)
else
  echo "docker compose (or docker-compose) is required" >&2
  exit 1
fi

cleanup() {
  if [[ "${KEEP_CONTAINERS:-0}" != "1" ]]; then
    "${COMPOSE[@]}" -f "$COMPOSE_FILE" down -v || true
  fi
}
trap cleanup EXIT

echo "Starting Scylla + MinIO..."
"${COMPOSE[@]}" -f "$COMPOSE_FILE" up -d

wait_healthy() {
  local container="$1"
  local timeout="${2:-180}"
  local elapsed=0
  while [[ "$elapsed" -lt "$timeout" ]]; do
    local has_health status run_state
    has_health="$(docker inspect --format '{{if .State.Health}}yes{{else}}no{{end}}' "$container" 2>/dev/null || true)"
    status="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}nohealth{{end}}' "$container" 2>/dev/null || true)"
    run_state="$(docker inspect --format '{{.State.Status}}' "$container" 2>/dev/null || true)"

    if [[ "$has_health" == "yes" && "$status" == "healthy" ]]; then
      return 0
    fi
    if [[ "$has_health" == "no" && "$run_state" == "running" ]]; then
      return 0
    fi
    sleep 2
    elapsed=$((elapsed + 2))
  done
  echo "Container $container did not become healthy (health=$status state=$run_state)" >&2
  docker logs "$container" || true
  return 1
}

echo "Waiting for Scylla health..."
wait_healthy "finalized-index-scylla" 300
echo "Waiting for Scylla CQL readiness..."
for i in {1..120}; do
  if docker exec finalized-index-scylla cqlsh -e "DESCRIBE KEYSPACES" >/dev/null 2>&1; then
    break
  fi
  sleep 2
  if [[ "$i" -eq 120 ]]; then
    echo "Scylla CQL was not ready in time" >&2
    docker logs finalized-index-scylla || true
    exit 1
  fi
done
echo "Waiting for MinIO health..."
wait_healthy "finalized-index-minio" 120

echo "Running distributed integration test..."
cd "$ROOT"
RUST_BACKTRACE=1 cargo test -p finalized-log-index --features distributed-stores --test distributed_stores_integration --test distributed_meta_cas -- --nocapture

echo "Distributed integration test passed"
