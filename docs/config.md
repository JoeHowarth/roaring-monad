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

| Field | Type | Default | Purpose |
|-------|------|---------|---------|
| `gc_guardrail_action` | `GuardrailAction` | `FailClosed` | Action when GC guardrails are hit |
| `max_orphan_chunk_bytes` | `u64` | `32 GiB` | GC guardrail: max orphan chunk bytes before action |
| `max_orphan_manifest_segments` | `u64` | `500,000` | GC guardrail: max orphan manifest segments |
| `max_stale_tail_keys` | `u64` | `1,000,000` | GC guardrail: max stale tail keys |

`GuardrailAction` is either `Throttle` or `FailClosed`.

## Vestigial Fields

These fields are vestigial from prior designs and are not used by any current code path:

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `target_entries_per_chunk` | `u32` | `1,950` | From chunk-based storage model |
| `target_chunk_bytes` | `usize` | `32 KiB` | From chunk-based storage model |
| `maintenance_seal_seconds` | `u64` | `600` | From time-based sealing model |
| `tail_flush_seconds` | `u64` | `5` | From tail-flush model |
