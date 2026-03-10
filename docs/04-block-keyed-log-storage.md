# Block-Keyed Log Storage Design

This document describes a proposed replacement for the current locator-page plus packed-log layout.

The goal is to keep the query model and roaring indexes based on monotonic global `log_id`, while storing log payload bytes by `block_num`.

This project is not deployed in production yet, so backward compatibility is not a constraint for this redesign. The document therefore targets the clean end state rather than a compatibility-preserving rollout.

## Goals

- keep `log_id` as the primary query and pagination identity
- store log payload bytes in block-keyed objects
- avoid per-log locator records
- support efficient single-log materialization
- support efficient broad block scans
- support backends with and without native byte-range reads

## Non-Goals

- changing `query_logs` request or pagination semantics
- changing roaring stream index semantics
- removing `first_log_id` and `count` from block metadata
- designing non-log families

## Current Layout

Today the logs family stores:

- `block_meta/<block_num> -> BlockMeta { block_hash, parent_hash, first_log_id, count }`
- `log_locator_pages/<page_start_log_id> -> { slot -> LogLocator }`
- `log_packs/<first_log_id> -> packed encoded logs`

That makes `log_id -> bytes` cheap, but it pushes the storage model toward per-log indirection.

## Proposed Layout

Keep `BlockMeta` as the authoritative log sequencing record:

- `block_meta/<block_num> -> BlockMeta { block_hash, parent_hash, first_log_id, count }`

Replace locator pages and log packs with:

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

## Block Log Objects

Each block gets one payload object:

- `block_logs/<block_num> -> concatenated encoded log bytes`

Reads use byte slices rather than explicit chunk objects.

For backends that do not support range reads natively, the blob-store adapter can polyfill `read_range(...)` by storing internal chunks behind the same interface.

The query and materialization layers should not know whether the backend is:

- native byte-range capable
- emulated via chunked blobs

## Lookup Flow

Given a candidate `log_id`:

1. compute `bucket_start = floor(log_id / bucket_size) * bucket_size`
2. load `log_dir/<bucket_start>`
3. binary-search `first_log_ids` to find the containing block
4. compute `block_num = start_block + entry_index`
5. compute `local_ordinal = log_id - first_log_ids[entry_index]`
6. load cached `block_log_headers/<block_num>`
7. read `offsets[local_ordinal]..offsets[local_ordinal + 1]`
8. issue `read_range(block_logs/<block_num>, start, end)`
9. decode one log

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

## Why This Is Efficient

Good cases:

- repeated pagination through nearby `log_id`s
- many matches from the same block
- broad block scans
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

Locator pages optimize direct per-log lookup, but they also require:

- per-log locator maintenance
- extra metadata churn during ingest
- a storage model that is less aligned with future block-keyed artifact families

The proposed layout keeps the query identity (`log_id`) while moving payload storage toward the more general block-keyed model.

## Backend Abstraction

The blob-store boundary should expose logical range reads:

- `read_range(key, start, end)`

Backends may implement that by:

- native range GETs
- internal chunk objects hidden behind the same adapter

The logs family should only depend on the logical range-read API.

## Implementation Shape

Because there is no production compatibility requirement, the implementation can move directly to the new layout:

1. add directory bucket, block-header, and range-read abstractions
2. switch ingest to write block-keyed artifacts instead of locator pages and packed-log blobs
3. switch materialization to `log_id -> bucket -> block_num -> header -> byte range`
4. remove dead locator-page and packed-log code paths

## Open Parameters

These are tuning choices, not architectural unknowns:

- bucket size
- header encoding format
- whether block headers are separate objects or inlined at the front of `block_logs/<block_num>`
- cache sizing
