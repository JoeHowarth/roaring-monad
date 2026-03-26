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
- `log_block_headers`
- `log_dir_buckets`
- `log_dir_sub_buckets`
- `log_block_blobs`
- `block_tx_blobs`
- `block_trace_blobs`
- `log_bitmap_page_meta`
- `log_bitmap_page_blobs`
- `log_bitmap_fragments`
- `tx_bitmap_fragments`
- `trace_bitmap_fragments`
- `log_dir_fragments`
- `tx_dir_fragments`
- `trace_dir_fragments`

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
