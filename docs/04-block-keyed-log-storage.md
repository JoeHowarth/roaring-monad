# Block-Keyed Log Storage

This document describes the current log-payload layout.

The crate keeps the query model and roaring indexes based on monotonic global `log_id`, while storing log payload bytes by `block_num`.

## Goals

- keep `log_id` as the primary query and pagination identity
- store log payload bytes in block-keyed objects
- avoid per-log locator records
- support efficient single-log materialization
- support backends with and without native byte-range reads

## Non-Goals

- changing `query_logs` request or pagination semantics
- changing roaring stream index semantics
- removing `first_log_id` and `count` from block metadata
- designing non-log families

Keep `BlockMeta` as the authoritative log sequencing record:

- `block_meta/<block_num> -> BlockMeta { block_hash, parent_hash, first_log_id, count }`

This intentionally duplicates sequencing information that is also derivable from directory buckets. The duplication is deliberate:

- `BlockMeta` remains the authoritative per-block record for finalized-state reads
- directory buckets are lookup accelerators for `log_id -> block_num`

The logs family stores:

- `log_dir/<bucket_start_log_id> -> LogDirectoryBucket`
- `block_log_headers/<block_num> -> BlockLogHeader`
- `block_logs/<block_num> -> encoded log bytes`

## Directory Buckets

Directory buckets map global `log_id` space to block numbers.

A bucket is keyed by a large aligned `log_id` range, for example `1_000_000`.

Example key:

- `log_dir/120000000`

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

## Block Headers

Each block gets a small header object:

- `block_log_headers/<block_num> -> BlockLogHeader`

The header contains byte offsets for local log ordinals.

Example:

```text
{
  offsets: [0, 41, 97, 124]
}
```

This means:

- local log `0` lives at bytes `[0, 41)`
- local log `1` lives at bytes `[41, 97)`
- local log `2` lives at bytes `[97, 124)`

This object should stay small enough to cache aggressively.

Empty blocks do not need `block_log_headers/<block_num>` or `block_logs/<block_num>` objects. They are represented by equal adjacent `first_log_ids` in the directory bucket and by `count == 0` in `BlockMeta`.

## Block Log Objects

Each block gets one payload object:

- `block_logs/<block_num> -> concatenated encoded log bytes`

Reads use byte slices rather than explicit chunk objects.

For backends that do not support range reads natively, the blob-store adapter can polyfill `read_range(...)` by storing internal chunks behind the same interface.

The query and materialization layers should not know whether the backend is:

- native byte-range capable
- emulated via chunked blobs

This requires extending the current blob-store abstraction. Today the trait only supports full-object reads; the block-keyed layout requires a logical `read_range(key, start, end)` capability.

## Lookup Flow

Given a candidate `log_id`:

1. compute `bucket_start = floor(log_id / bucket_size) * bucket_size`
2. load `log_dir/<bucket_start>`
3. find the last index `i` where `first_log_ids[i] <= log_id`
4. verify that `log_id < first_log_ids[i + 1]`
5. compute `block_num = start_block + i`
6. compute `local_ordinal = log_id - first_log_ids[i]`
7. load cached `block_log_headers/<block_num>`
8. read `offsets[local_ordinal]..offsets[local_ordinal + 1]`
9. issue `read_range(block_logs/<block_num>, start, end)`
10. decode one log

## Worked Example

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

## Why This Is Efficient

Good cases:

- repeated pagination through nearby `log_id`s
- many matches from the same block
- machines where directory buckets and block headers are effectively memory-resident

The current executor already iterates candidate `log_id`s in ascending order, which improves cache locality for:

- current directory bucket
- current block header
- current block payload object

## Bad Cases

Main pressure points:

- very large blocks produce very large `block_logs/<block_num>` objects
- cold random point lookups can bounce across many blocks
- too-small directory buckets create avoidable cross-bucket lookups

The design mitigates those issues by:

- keeping buckets large, e.g. `100_000` or `1_000_000`
- using byte-range reads instead of fetching the full block blob
- caching directory buckets and block headers

## Why Not Locator Pages

Locator pages were removed because they required:

- per-log locator maintenance
- extra metadata churn during ingest
- a storage model that was less aligned with future block-keyed artifact families

The current layout keeps the query identity (`log_id`) while moving payload storage toward the more general block-keyed model.

## Backend Abstraction

The blob-store boundary should expose logical range reads:

- `read_range(key, start, end)`

Backends may implement that by:

- native range GETs
- internal chunk objects hidden behind the same adapter

The logs family should only depend on the logical range-read API.

In-memory and simple test backends can implement this by loading the whole object and slicing locally. Remote/object-store backends can implement it with native range GETs.

## Implemented Shape

The crate implements the layout directly:

1. the blob-store abstraction exposes logical range reads
2. ingest writes block headers and block-keyed payload blobs
3. ingest writes immutable `log_dir_frag/<sub_bucket_start>/<block_num>` records as the authoritative `log_id -> block_num` frontier
4. ingest eagerly compacts sealed sub-buckets into immutable `log_dir_sub/<sub_bucket_start>` summaries and may compact full 1M buckets into immutable `log_dir/<bucket_start>`
5. materialization resolves `log_id -> summary-or-fragment -> block_num -> header -> byte range`
6. locator-page and packed-log code paths are gone

## Directory Fragment Writes

Directory publication is immutable.

When ingest writes block `N` with `first_log_id = X`:

1. compute every covered sub-bucket from `sub_bucket(X)` through `sub_bucket(max(X, X + count - 1))`
2. write one immutable `log_dir_frag/<sub_bucket_start>/<block_num>` record for each covered sub-bucket
3. once `next_log_id` crosses a sub-bucket end, compact its immutable fragments into `log_dir_sub/<sub_bucket_start>`
4. once `next_log_id` crosses a 1M-bucket end, optionally compact the sealed sub-buckets into `log_dir/<bucket_start>`

The grouped summary objects are acceleration artifacts built from immutable source fragments. They are never the only authoritative source for the frontier.

## Open Parameters

These are tuning choices, not architectural unknowns:

- bucket size
- header encoding format
- whether block headers are separate objects or inlined at the front of `block_logs/<block_num>`
- cache sizing
