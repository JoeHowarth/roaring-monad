# Storage Model

Read this after [overview.md](overview.md).

This doc describes the published artifact model and the logs/traces storage
layouts, plus the shared block-header state. It focuses on what gets stored and
how reads resolve IDs back to payload bytes.

## Design Principles

- family primary IDs (`log_id`, `tx_id`, `trace_id`) are the query and pagination identities
- family payload bytes are stored in block-keyed objects
- no per-item locator records — directory buckets handle primary-ID to block resolution
- published data-path artifacts are immutable by convention
- summaries never replace source fragments — they are acceleration artifacts

## Published Artifact Model

All read-path data artifacts are treated as immutable once published. The
only shared mutable state is:

- `publication_state` table entry `state` — ownership session, lease validity, indexed finalized head
- `open_bitmap_page`, `tx_open_bitmap_page`, and `trace_open_bitmap_page` table rows — write/recovery inventory markers

This means cached artifacts are safe to reuse indefinitely until eviction, with no invalidation required. See [caching.md](caching.md) for cache design details.

### Publication Ordering

All artifacts for a block must be durable before `publication_state.indexed_finalized_head` is advanced. This ensures readers never observe a head that references artifacts that don't yet exist.

The artifact write order for a block preserves shared visibility boundaries:

1. shared block hash lookup (`block_hash_index`)
2. shared full block header (`block_header`)
3. family payload blobs and headers (`block_log_blob` / `block_log_header`, `block_tx_blob` / `block_tx_header`, `block_trace_blob` / `block_trace_header`)
4. family directory fragments (`log_dir_by_block`, `tx_dir_by_block`, `trace_dir_by_block`)
5. family stream fragments (`bitmap_by_block`, `tx_bitmap_by_block`, `trace_bitmap_by_block`)
6. family compaction (directory summaries, stream pages)
7. shared block metadata (`block_record`)
8. head advance via CAS on `publication_state`

Writers use unconditional writes for normal artifacts. Correctness
depends on the publication boundary and on rewrites remaining
deterministic and reader-compatible within one artifact version/keyspace.

### Fragment Retention

Compacted summaries (directory sub-buckets, directory buckets, stream pages) are acceleration artifacts built from immutable source fragments. They are never the only authoritative source for data in the frontier zone.

Source fragments are retained alongside summaries. This means recovery can always reconstruct state from fragments if a summary is missing or corrupt.

## Logs Directory Layout

Directory buckets map global `log_id` space to block numbers.

### Tiered Structure

Three tiers of directory objects exist:

| Tier | Storage layout | Scope | Written by |
|------|----------------|-------|------------|
| Fragment | `log_dir_by_block` table, partition `<sub_bucket_start>`, clustering `<block_num>` | One block's contribution to one sub-bucket | Ingest, per block |
| Sub-bucket | `log_dir_sub_bucket` table, key `<sub_bucket_start>` | 10,000 `log_id` range | Compaction when sub-bucket seals |
| Bucket | `log_dir_bucket` table, key `<bucket_start>` | 1,000,000 `log_id` range | Optional compaction when bucket seals |

Queries prefer the most compacted tier available and fall back to fragments for the frontier (recent, not-yet-compacted data).

### Bucket Format

A bucket is keyed by a large aligned `log_id` range (1,000,000 for full buckets, 10,000 for sub-buckets).

Example location: `log_dir_bucket` table, key `120000000`

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
2. write one `log_dir_by_block` row for each covered sub-bucket, keyed by partition `<sub_bucket_start>` and clustering `<block_num>`
3. once `next_log_id` crosses a sub-bucket end, compact its retained fragments into `log_dir_sub_bucket`, key `<sub_bucket_start>`
4. once `next_log_id` crosses a 1M-bucket end, optionally compact the sealed sub-buckets into `log_dir_bucket`, key `<bucket_start>`

## Logs Stream Layout

Stream indexes use roaring bitmaps to map stream identifiers (address/topic) to `log_local_id` sets within a shard.

### Stream ID Construction

A stream ID encodes the index kind, the indexed value, and the shard:

```text
<index_kind>/<hex_value>/<shard_hex>
```

### Tiered Structure

| Tier | Storage layout | Scope | Written by |
|------|----------------|-------|------------|
| By-block | `bitmap_by_block` table, partition `<stream_id>/<page_start_local>`, clustering `<block_num>` | One block's bitmap contribution to one shard-local page | Ingest |
| Page meta | `bitmap_page_meta` table, key `<stream_id>/<page_start_local>` | Compacted page bitmap meta for one shard-local page | Compaction |
| Page blob | `bitmap_page_blob` blob table, key `<stream_id>/<page_start_local>` | Compacted roaring bitmap for one shard-local page | Compaction |

Stream pages span `STREAM_PAGE_LOCAL_ID_SPAN` (4,096) local IDs.

### Open-Page Markers

`open_bitmap_page` rows with partition `<shard>` and clustering `<page_start_local>/<stream_id>` track which stream pages have active (unsealed) fragments. `page_start_local` is the aligned start of the 4,096-local-ID page within that shard. They are used during:

- compaction: to discover which pages need sealing when `next_log_id` crosses a page boundary
- ownership-transition recovery: to clean up stale markers left by interrupted ingest

## Block Headers

The shared block query surface persists one full EVM header per block in the
`block_header` table keyed by `<block_num>`. This is the authoritative block
object for `query_blocks`, `get_block`, and `get_block_header_by`.

`block_record` remains separate and compact. It stores only shared block
identity plus family primary windows used by indexed-family queries.

Every block-keyed payload family uses the shared `BucketedOffsets` structure in
its header. The common pattern is:

- one authoritative block-keyed blob
- one compact block-keyed header
- `BucketedOffsets` for random access into the blob

For payloads smaller than 4 GB this collapses to a single bucket and behaves
like a simple dense offset array. Larger payloads spill into additional buckets
without changing the family-owned lookup shape.

### Logs Block Headers

Each block gets a small header record in the `block_log_header` table keyed by `<block_num>`.

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

Empty blocks do not need `block_log_header` or `block_log_blob` entries. They are represented by equal adjacent `first_log_ids` in the directory bucket and by `count == 0` in `BlockRecord.logs`.

## Logs Block Payload Objects

Each block gets one payload blob in the `block_log_blob` blob table keyed by `<block_num>`.

Reads use byte slices via `read_range(key, start, end_exclusive)` rather than fetching the full blob.

For backends that do not support range reads natively, the blob-store adapter polyfills `read_range(...)` by loading the whole object and slicing locally.

## Logs Lookup Flow

Given a candidate `log_id`:

1. compute `bucket_start = floor(log_id / bucket_size) * bucket_size`
2. load `log_dir_bucket`, key `<bucket_start>` (or `log_dir_sub_bucket`, or fall back to `log_dir_by_block` fragments)
3. find the last index `i` where `first_log_ids[i] <= log_id`
4. verify that `log_id < first_log_ids[i + 1]`
5. compute `block_num = start_block + i`
6. compute `local_ordinal = log_id - first_log_ids[i]`
7. load cached `block_log_header`, key `<block_num>`
8. read `offsets[local_ordinal]..offsets[local_ordinal + 1]`
9. issue `read_range(block_log_blob, <block_num>, start, end)`
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
6. fetch bytes `[110, 160)` from `block_log_blob`, key `5003`

The duplicate `120000003` entries matter here. The lookup must choose the last index where `first_log_ids[i] <= 120000006`, not an arbitrary duplicate position.

## Txs Storage Layout

Transactions follow the same block-keyed artifact model as logs and traces.

Each block with transactions is expected to persist:

- `block_tx_blob`, keyed by `<block_num>`, containing authoritative transaction envelopes in block order
- `block_tx_header`, keyed by `<block_num>`, containing `BucketedOffsets` for `tx_idx -> byte range`

Each transaction envelope is RLP-encoded as:

- `tx_hash`
- `sender`
- `signed_tx_bytes`

Transaction point lookup by hash should be handled by a metadata index:

- `tx_hash_index`, keyed by `<tx_hash>`, pointing to a compact `TxLocation { block_num, tx_idx }`

That keeps responsibilities separated:

- header = blob navigation
- blob = authoritative bytes
- metadata index = point lookup acceleration

## Traces Storage Layout

Traces use the same artifact pattern as logs, but the family owns its own ID space, directory tables, stream tables, and payload/header codecs.

### Trace Directory Layout

Three tiers of trace directory objects exist:

| Tier | Storage layout | Scope | Written by |
|------|----------------|-------|------------|
| Fragment | `trace_dir_by_block` table, partition `<trace_sub_bucket_start>`, clustering `<block_num>` | One block's contribution to one sub-bucket | Ingest, per block |
| Sub-bucket | `trace_dir_sub_bucket` table, key `<trace_sub_bucket_start>` | Fixed trace-ID range | Compaction when sub-bucket seals |
| Bucket | `trace_dir_bucket` table, key `<trace_bucket_start>` | Larger aligned trace-ID range | Optional compaction when bucket seals |

Queries prefer the most compacted trace directory tier available and fall back to fragments for the frontier.

### Trace Stream Layout

Trace stream indexes mirror the logs shape with family-owned tables:

| Tier | Storage layout | Scope | Written by |
|------|----------------|-------|------------|
| By-block | `trace_bitmap_by_block` table, partition `<stream_id>/<page_start>`, clustering `<block_num>` | One block's bitmap contribution to one page | Ingest |
| Page meta | `trace_bitmap_page_meta` table, key `<stream_id>/<page_start>` | Compacted page bitmap meta | Compaction |
| Page blob | `trace_bitmap_page_blob` blob table, key `<stream_id>/<page_start>` | Compacted roaring bitmap | Compaction |

Trace stream pages use the same shared sharded-page mechanics as logs, but with traces-owned stream IDs and filters.

### Trace Block Headers and Payloads

Each block with traces persists:

- `block_trace_header`, keyed by `<block_num>`, containing trace offsets and transaction starts for the block
- `block_trace_blob`, keyed by `<block_num>`, containing the flattened concatenation of per-frame RLP bytes for that block

Materialization resolves `trace_id -> block_num` through the trace directory, then slices the block trace payload with the trace header.

## Efficiency

Good cases:

- repeated pagination through nearby `log_id`s
- many matches from the same block
- machines where directory buckets and block headers are effectively memory-resident

The executor iterates candidate `log_id`s in ascending order, which improves cache locality for the current directory bucket, block header, and block payload object.

### Pressure Points

- very large blocks produce very large `block_log_blob` entries
- cold random point lookups can bounce across many blocks
- too-small directory buckets create avoidable cross-bucket lookups

Mitigations:

- large buckets (100,000 or 1,000,000)
- byte-range reads instead of fetching full block blobs
- aggressive caching of directory buckets and block headers

## Block Metadata

The authoritative shared block header lives alongside the sequencing record:

```text
block_header table, key <block_num> -> EvmBlockHeader { full stored header }
```

`BlockRecord` is the authoritative per-block sequencing record:

```text
block_record table, key <block_num> -> BlockRecord {
  block_hash,
  parent_hash,
  logs: Option<PrimaryWindowRecord { first_primary_id, count }>,
  txs: Option<PrimaryWindowRecord { first_primary_id, count }>,
  traces: Option<PrimaryWindowRecord { first_primary_id, count }>
}
```

This intentionally duplicates sequencing information also derivable from directory buckets. The duplication is deliberate: `BlockRecord` is the authoritative per-block record for finalized-state reads, while directory buckets are lookup accelerators for `log_id -> block_num`, `tx_id -> block_num`, and `trace_id -> block_num`.
