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

echo "Waiting for Scylla (9042)..."
for i in {1..120}; do
  if nc -z 127.0.0.1 9042 >/dev/null 2>&1; then
    break
  fi
  sleep 1
  if [[ "$i" -eq 120 ]]; then
    echo "Scylla did not become reachable" >&2
    exit 1
  fi
done

echo "Waiting for MinIO (9000)..."
for i in {1..120}; do
  if curl -fsS http://127.0.0.1:9000/minio/health/live >/dev/null 2>&1; then
    break
  fi
  sleep 1
  if [[ "$i" -eq 120 ]]; then
    echo "MinIO did not become healthy" >&2
    exit 1
  fi
done

echo "Running distributed integration test..."
cd "$ROOT"
RUST_BACKTRACE=1 cargo test -p finalized-log-index --features distributed-stores --test distributed_stores_integration -- --nocapture

echo "Distributed integration test passed"
