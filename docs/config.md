# Config

This document describes the `Config` struct fields, their purposes, and defaults.

All fields live in `src/config.rs`.

## Upstream Observation

| Field | Type | Default | Purpose |
|-------|------|---------|---------|
| `observe_upstream_finalized_block` | `Arc<dyn Fn() -> Option<u64>>` | returns `None` | Callback that returns the latest observed upstream finalized block number. Used as the lease clock. See [write-authority.md](write-authority.md). |

## Lease Config

| Field | Type | Default | Purpose |
|-------|------|---------|---------|
| `publication_lease_blocks` | `u64` | `10` | Lease duration in upstream finalized blocks |
| `publication_lease_renew_threshold_blocks` | `u64` | `2` | Renew when the gap enters this window |

Required invariant: `publication_lease_renew_threshold_blocks < publication_lease_blocks`.

See [write-authority.md](write-authority.md) for the lease clock model.

## Query Config

| Field | Type | Default | Purpose |
|-------|------|---------|---------|
| `planner_max_or_terms` | `usize` | `128` | Maximum number of OR terms in a query clause |

## Cache Config

| Field | Type | Default | Purpose |
|-------|------|---------|---------|
| `bytes_cache` | `BytesCacheConfig` | all tables disabled | Per-table byte budgets for the immutable bytes cache |

See [caching.md](caching.md) for cache design details.

## Ingest Mode

| Field | Type | Default | Purpose |
|-------|------|---------|---------|
| `ingest_mode` | `IngestMode` | `StrictCas` | Publication CAS mode |

- `StrictCas` — uses `compare_and_set` for head publication (lease-backed mode)
- `SingleWriterFast` — uses `PutCond::Any` for head publication (single-writer mode)

## Stream Config

| Field | Type | Default | Purpose |
|-------|------|---------|---------|
| `assume_empty_streams` | `bool` | `false` | Skip stream fragment loading during startup when streams are known to be empty |
| `stream_append_concurrency` | `usize` | `96` | Maximum concurrent stream fragment write operations |

## Runtime Health

| Field | Type | Default | Purpose |
|-------|------|---------|---------|
| `backend_error_throttle_after` | `u64` | `3` | Consecutive backend errors before throttling |
| `backend_error_degraded_after` | `u64` | `10` | Consecutive backend errors before degraded state |

## GC Guardrails

The GC worker (`src/gc/worker.rs`) periodically cleans orphaned artifacts left behind by failed ingest attempts or compaction. Guardrails cap how much debris is tolerable before the service degrades or fails closed.

After each GC pass, the collected stats are compared against these thresholds via `GcStats::check_guardrails`. If any threshold is exceeded, the service reacts according to `gc_guardrail_action`.

| Field | Type | Default | Purpose |
|-------|------|---------|---------|
| `gc_guardrail_action` | `GuardrailAction` | `FailClosed` | Action when any guardrail threshold is exceeded |
| `max_orphan_chunk_bytes` | `u64` | `32 GiB` | Total bytes of orphaned chunk objects (artifacts written but never published) |
| `max_orphan_manifest_segments` | `u64` | `500,000` | Count of manifest segment records left by abandoned compaction or ingest |
| `max_stale_tail_keys` | `u64` | `1,000,000` | Count of stale tail keys (metadata entries above the published head from failed writers) |

`GuardrailAction` is either `Throttle` (set service to throttled state) or `FailClosed` (set service to degraded state).

## Backend-Specific Config

Backend implementations have their own configuration that is not part of the main `Config` struct. These are set at construction time on each store.

### ScyllaMetaStore

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `max_retries` | `10` | Maximum retry attempts for retryable errors |
| `base_delay_ms` | `50` | Base delay for exponential backoff |
| `max_delay_ms` | `5000` | Maximum backoff delay |
| `fence_check_interval_ms` | `1000` | Minimum interval between remote fence checks (cached locally between checks) |

### MinioBlobStore

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `max_retries` | `4` | Maximum retry attempts for retryable errors |
| `base_delay_ms` | `25` | Base delay for exponential backoff |
| `max_delay_ms` | `1000` | Maximum backoff delay |

See [backend-stores.md](backend-stores.md) for full implementation details.
