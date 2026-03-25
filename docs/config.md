# Config

Read this after the architecture docs when you need the tunable surface.

This doc describes the `Config` struct fields, their purposes, and defaults.

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

`BytesCacheConfig` currently includes independent byte budgets for:

- `block_records`
- `block_log_header`
- `log_dir_buckets`
- `log_dir_sub_buckets`
- `point_log_payloads`
- `point_tx_payloads`
- `point_trace_payloads`
- `bitmap_page_meta`
- `bitmap_page_blobs`

## Stream Config

| Field | Type | Default | Purpose |
|-------|------|---------|---------|
| `assume_empty_streams` | `bool` | `false` | Skip stream fragment loading when deriving family state from the published head and streams are known to be empty |
| `stream_append_concurrency` | `usize` | `96` | Maximum concurrent stream fragment write operations |

## Backend-Specific Config

Backend implementations have their own configuration that is not part of the main `Config` struct. These are set at construction time on each store.

### ScyllaMetaStore

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `max_retries` | `10` | Maximum retry attempts for retryable errors |
| `base_delay_ms` | `50` | Base delay for exponential backoff |
| `max_delay_ms` | `5000` | Maximum backoff delay |

### MinioBlobStore

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `max_retries` | `4` | Maximum retry attempts for retryable errors |
| `base_delay_ms` | `25` | Base delay for exponential backoff |
| `max_delay_ms` | `1000` | Maximum backoff delay |

See [backend-stores.md](backend-stores.md) for full implementation details.
