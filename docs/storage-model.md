# Storage Model

This document describes the immutable artifact model and the block-keyed log-payload layout.

## Design Principles

- `log_id` is the primary query and pagination identity
- log payload bytes are stored in block-keyed objects
- no per-log locator records — directory buckets handle `log_id -> block_num` resolution
- published data-path artifacts are immutable once created
- summaries never replace source fragments — they are acceleration artifacts

## Immutable Artifact Model

All read-path data artifacts are immutable once published. The only shared mutable state is:

- `publication_state` — ownership, epoch, indexed finalized head
- `open_stream_page/*` — write/recovery inventory markers

This means cached artifacts are safe to reuse indefinitely until eviction, with no invalidation required. See [caching.md](caching.md) for cache design details.

### Publication Ordering

All artifacts for a block must be durable before `publication_state.indexed_finalized_head` is advanced. This ensures readers never observe a head that references artifacts that don't yet exist.

The artifact write order for a block:

1. log blob (`block_logs/<block_num>`)
2. block log header (`block_log_headers/<block_num>`)
3. block meta (`block_meta/<block_num>`)
4. block hash lookup (`block_hash_to_num/<block_hash>`)
5. directory fragments (`log_dir_frag/...`)
6. stream fragments (`stream_frag_meta/...`, `stream_frag_blob/...`)
7. compaction (directory sub-buckets, stream pages)
8. head advance via CAS on `publication_state`

### Fragment Retention

Compacted summaries (directory sub-buckets, directory buckets, stream pages) are acceleration artifacts built from immutable source fragments. They are never the only authoritative source for data in the frontier zone.

Source fragments are retained alongside summaries. This means recovery can always reconstruct state from fragments if a summary is missing or corrupt.

## Directory Layout

Directory buckets map global `log_id` space to block numbers.

### Tiered Structure

Three tiers of directory objects exist:

| Tier | Key pattern | Scope | Written by |
|------|------------|-------|------------|
| Fragment | `log_dir_frag/<sub_bucket_start>/<block_num>` | One block's contribution to one sub-bucket | Ingest, per block |
| Sub-bucket | `log_dir_sub/<sub_bucket_start>` | 10,000 `log_id` range | Compaction when sub-bucket seals |
| Bucket | `log_dir/<bucket_start>` | 1,000,000 `log_id` range | Optional compaction when bucket seals |

Queries prefer the most compacted tier available and fall back to fragments for the frontier (recent, not-yet-compacted data).

### Bucket Format

A bucket is keyed by a large aligned `log_id` range (1,000,000 for full buckets, 10,000 for sub-buckets).

Example key: `log_dir/120000000`

Example value:

```text
{
  start_block: 5001,
  first_log_ids: [120000000, 120000003, 120000003, 120000008]
}
```

This encodes:

- block `5001` starts at log `120000000`
- block `5002` starts at log `120000003`
- block `5003` starts at log `120000003`
- sentinel `120000008` closes the last block range

Derived counts:

- block `5001` has `3` logs
- block `5002` has `0` logs
- block `5003` has `5` logs

The final sentinel is required so the last block in the bucket has a closed range.

Blocks can span bucket boundaries. When that happens, every covered bucket stores the same block boundary entry so `log_id` lookup stays local to the bucket containing the queried `log_id`.

### Directory Fragment Writes

When ingest writes block `N` with `first_log_id = X`:

1. compute every covered sub-bucket from `sub_bucket(X)` through `sub_bucket(max(X, X + count - 1))`
2. write one immutable `log_dir_frag/<sub_bucket_start>/<block_num>` record for each covered sub-bucket
3. once `next_log_id` crosses a sub-bucket end, compact its immutable fragments into `log_dir_sub/<sub_bucket_start>`
4. once `next_log_id` crosses a 1M-bucket end, optionally compact the sealed sub-buckets into `log_dir/<bucket_start>`

## Stream Layout

Stream indexes use roaring bitmaps to map stream identifiers (address/topic) to `log_local_id` sets within a shard.

### Stream ID Construction

A stream ID encodes the index kind, the indexed value, and the shard:

```text
<index_kind>/<hex_value>/<shard_hex>
```

### Tiered Structure

| Tier | Key pattern | Scope | Written by |
|------|------------|-------|------------|
| Fragment meta | `stream_frag_meta/<stream_id>/<page_start_local>/<block_num>` | One block's bitmap contribution to one shard-local page | Ingest |
| Fragment blob | `stream_frag_blob/<stream_id>/<page_start_local>/<block_num>` | Roaring bitmap blob for one shard-local page | Ingest |
| Page meta | `stream_page_meta/<stream_id>/<page_start_local>` | Compacted page bitmap meta for one shard-local page | Compaction |
| Page blob | `stream_page_blob/<stream_id>/<page_start_local>` | Compacted roaring bitmap for one shard-local page | Compaction |

Stream pages span `STREAM_PAGE_LOCAL_ID_SPAN` (4,096) local IDs.

### Open-Page Markers

`open_stream_page/<shard>/<page_start_local>/<stream_id>` markers track which stream pages have active (unsealed) fragments. `page_start_local` is the aligned start of the 4,096-local-ID page within that shard. They are used during:

- compaction: to discover which pages need sealing when `next_log_id` crosses a page boundary
- startup repair: to clean up stale markers left by interrupted ingest

## Block Headers

Each block gets a small header object: `block_log_headers/<block_num> -> BlockLogHeader`

The header contains byte offsets for local log ordinals:

```text
{
  offsets: [0, 41, 97, 124]
}
```

This means:

- local log `0` lives at bytes `[0, 41)`
- local log `1` lives at bytes `[41, 97)`
- local log `2` lives at bytes `[97, 124)`

Empty blocks do not need `block_log_headers` or `block_logs` objects. They are represented by equal adjacent `first_log_ids` in the directory bucket and by `count == 0` in `BlockMeta`.

## Block Log Objects

Each block gets one payload object: `block_logs/<block_num> -> concatenated encoded log bytes`

Reads use byte slices via `read_range(key, start, end_exclusive)` rather than fetching the full blob.

For backends that do not support range reads natively, the blob-store adapter polyfills `read_range(...)` by loading the whole object and slicing locally.

## Lookup Flow

Given a candidate `log_id`:

1. compute `bucket_start = floor(log_id / bucket_size) * bucket_size`
2. load `log_dir/<bucket_start>` (or `log_dir_sub/...` or fall back to fragments)
3. find the last index `i` where `first_log_ids[i] <= log_id`
4. verify that `log_id < first_log_ids[i + 1]`
5. compute `block_num = start_block + i`
6. compute `local_ordinal = log_id - first_log_ids[i]`
7. load cached `block_log_headers/<block_num>`
8. read `offsets[local_ordinal]..offsets[local_ordinal + 1]`
9. issue `read_range(block_logs/<block_num>, start, end)`
10. decode one log

### Worked Example

Directory bucket:

```text
{
  start_block: 5001,
  first_log_ids: [120000000, 120000003, 120000003, 120000008]
}
```

Header for block `5003`:

```text
{
  offsets: [0, 30, 75, 110, 160, 201]
}
```

Resolve `log_id = 120000006`:

1. bucket lookup finds the object above
2. `120000006` falls in `[120000003, 120000008)`, which is entry `2`
3. `block_num = 5001 + 2 = 5003`
4. `local_ordinal = 120000006 - 120000003 = 3`
5. byte range is `offsets[3]..offsets[4] = 110..160`
6. fetch bytes `[110, 160)` from `block_logs/5003`

The duplicate `120000003` entries matter here. The lookup must choose the last index where `first_log_ids[i] <= 120000006`, not an arbitrary duplicate position.

## Efficiency

Good cases:

- repeated pagination through nearby `log_id`s
- many matches from the same block
- machines where directory buckets and block headers are effectively memory-resident

The executor iterates candidate `log_id`s in ascending order, which improves cache locality for the current directory bucket, block header, and block payload object.

### Pressure Points

- very large blocks produce very large `block_logs/<block_num>` objects
- cold random point lookups can bounce across many blocks
- too-small directory buckets create avoidable cross-bucket lookups

Mitigations:

- large buckets (100,000 or 1,000,000)
- byte-range reads instead of fetching full block blobs
- aggressive caching of directory buckets and block headers

## Block Metadata

`BlockMeta` is the authoritative per-block sequencing record:

```text
block_meta/<block_num> -> BlockMeta { block_hash, parent_hash, first_log_id, count }
```

This intentionally duplicates sequencing information also derivable from directory buckets. The duplication is deliberate — `BlockMeta` is the authoritative per-block record for finalized-state reads; directory buckets are lookup accelerators for `log_id -> block_num`.
