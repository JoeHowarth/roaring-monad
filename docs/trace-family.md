# Trace Family

> Status: target-state design
>
> This document describes the intended end-state design for the trace family.
> It is not fully implemented today, and it is not a concrete implementation
> plan. Implementation sequencing and rollout steps belong in `docs/plans/`.

This document describes the trace family design: storage layout, ingest
behavior, zero-copy accessors, indexing, and how native-transfer queries are
served from the same underlying data.

## Overview

The trace family stores RLP-encoded call frames produced by the execution
engine. The authoritative representation is the raw RLP blob — the same
`Vec<Vec<CallFrame>>` encoding that the engine emits. The family never
re-encodes or restructures the trace data; it stores, indexes, and queries
over the original bytes.

The design assumes that the execution engine's emitted RLP shape is stable in
normal operation. The per-block trace metadata carries an explicit encoding
version so readers can interpret the blob without relying on blob-store-specific
attributes.

Native transfers (`eth_queryTransfers` in the reference queryX spec) are not a
separate family. A transfer is a trace with non-zero value. Transfer queries
are trace queries with an implicit `has_value` filter and a different
materialization surface. One family, one set of artifacts, one set of bitmap
streams.

## Ingest Envelope

`FinalizedBlock` carries a `trace_rlp: Vec<u8>` field containing the raw
RLP-encoded call frames for the block. The outer RLP list has one entry per
transaction; each entry is an RLP list of call frames for that transaction.

The trace family decodes this blob at ingest time to build indexes and the
offset header, then stores the raw bytes as-is.

## Trace Identity

Each call frame receives a `TraceId` using the same 40-bit shard + 24-bit
local scheme as `LogId`. Traces are assigned IDs in flat order across the
block: all frames from transaction 0, then all frames from transaction 1, and
so on.

`TraceSequencingState` holds `next_trace_id: TraceId`, advanced monotonically
during ingest, mirroring `LogSequencingState`.

## BucketedOffsets (Shared Substrate)

`BucketedOffsets` is a shared utility for storing byte offsets into per-block
blobs. It is not trace-specific — every family that stores a block-keyed blob
with per-entity random access uses the same structure and codec.

The problem: byte offsets into a block blob can exceed u32 range (traces with
large input/output payloads on high-throughput blocks), but storing u64 per
entity doubles the dominant per-entity metadata cost across the full history.

The solution: bucket the offset array by u32 range.

- The offset space is divided into u32-sized buckets (0–4G, 4G–8G, etc.)
- Each bucket stores one `Vec<u32>` of low-order byte offsets for the entities
  whose full offset falls in that bucket's range
- A small prefix array records the cumulative entity count at each bucket
  boundary

To look up the byte offset for entity `i`: find the bucket from the prefix
array, subtract the bucket's starting entity index, index into that bucket's
u32 array, and reconstruct the full offset as `bucket_index * 4G + low_bits`.

For the common case (block blob < 4 GB), there is one bucket and one u32
array — no overhead beyond the single-entry prefix. When a block's blob exceeds
4 GB, additional buckets are added transparently. The per-entity metadata cost
is 4 bytes regardless of blob size.

`BucketedOffsets` lives in the shared substrate (not in any family module). The
logs family's `BlockLogHeader` currently uses a plain u32 offset array, which
is a single-bucket `BucketedOffsets` — the migration is mechanical.

## Storage Artifacts

### Block trace blob

`block_trace_blob` blob table, key `<block_num>`: the raw RLP bytes from the
ingest envelope, stored byte-for-byte.

### Block trace header

`block_trace_header` table, key `<block_num>`: a compact offset table for
random access into the blob.

The header stores:

- an encoding version for the trace blob layout
- a `BucketedOffsets` structure for byte offsets into the blob (see above)
- `tx_starts: Vec<u32>` — one entry per transaction, giving the flat trace
  index of the first trace in that transaction

The byte range for trace `i` is `[offsets.get(i), offsets.get(i + 1))`, with
the final trace ending at `blob_len`. This keeps the header compact while still
allowing O(1) navigation to any individual call frame's RLP bytes.

Transactions with zero traces are represented by repeated entries in
`tx_starts`.

To recover `tx_idx` for flat trace `i`, find the first `tx_starts` entry `> i`
and subtract one from that position. This is O(log T) where T is the
transaction count in the block.

Empty blocks (zero traces) get an empty header and no blob entry, matching the
logs family convention.

### Trace block record

`block_record` table, key `<block_num>`:
`{ block_hash, parent_hash, logs: Option<PrimaryWindowRecord>, traces: Option<PrimaryWindowRecord> }`.

The traces family reads the `traces` window from this shared block-keyed
record. It is the authoritative per-block sequencing record for the trace ID
space.

### Directory

Same tiered structure as the logs directory, scoped to trace IDs:

| Tier | Table | Scope |
|------|-------|-------|
| Fragment | `trace_dir_by_block` | One block's contribution to one sub-bucket |
| Sub-bucket | `trace_dir_sub_bucket` | 10,000 trace-ID range |
| Bucket | `trace_dir_bucket` | 1,000,000 trace-ID range |

### Stream indexes

Same tiered bitmap structure as logs, scoped to trace IDs:

| Tier | Table |
|------|-------|
| By-block fragments | `trace_bitmap_by_block` |
| Compacted page meta | `trace_bitmap_page_meta` |
| Compacted page blob | `trace_bitmap_page_blob` |
| Open-page markers | `trace_open_bitmap_page` |

## Indexed Streams

The following bitmap streams are built at ingest time:

| Stream | Index kind | Indexed value | Notes |
|--------|-----------|---------------|-------|
| `from` | address | `from` field (20 bytes) | Every trace has a sender |
| `to` | address | `to` field (20 bytes) | Only for traces with a recipient (not creates) |
| `selector` | selector | first 4 bytes of `input` | Call-type traces only, when `input.len() >= 4` |
| `has_value` | flag | `1` | Traces where `value > 0` |

The `from`, `to`, and `selector` streams use the same shard-partitioned stream
ID scheme as logs: `<index_kind>/<hex_value>/<shard_hex>`.

The `selector` stream is only populated for call-type traces (Call,
DelegateCall, CallCode, StaticCall) with at least 4 bytes of input. Create and
SelfDestruct traces are not indexed in the selector stream — init code
leading bytes are not function selectors, and selfdestructs have no meaningful
input.

The `has_value` stream uses `has_value/1/<shard_hex>` as its stream ID — the
value component is the literal string `1`, fitting the existing
`<index_kind>/<hex_value>/<shard_hex>` scheme without special-casing. One
bitmap per shard tracks all trace IDs with non-zero value. It is sparse (most
calls do not transfer value) and cheap to maintain.

## isTopLevel

The external filter field is `isTopLevel`. Inside the query engine it maps to
an `is_top_level` predicate.

`isTopLevel` filtering uses `depth == 0` from the call frame. This is handled
as a post-filter at materialization time rather than a dedicated bitmap stream.
The bitmap intersection on `from`/`to`/`selector` already narrows the
candidate set; checking depth on the surviving candidates is negligible.

`isTopLevel` is intentionally not indexed in the base design. For many
workloads it is not selective enough to justify the extra write amplification
and bitmap surface area. If profiling later shows that `isTopLevel`-dominant
queries are common and expensive, a dedicated bitmap stream can be added.

## Zero-Copy Accessors

The trace family depends on `alloy_rlp` for RLP header decoding. No
higher-level Ethereum type libraries are used.

### CallFrameView

`CallFrameView<'a>` borrows a single call frame's RLP bytes and exposes
field accessors without allocation. It lazily parses the frame layout once,
then reuses the computed field boundaries for subsequent access.

```rust
struct CallFrameView<'a>(&'a [u8]);

impl<'a> CallFrameView<'a> {
    fn typ(&self) -> u8;
    fn flags(&self) -> u64;
    fn from_addr(&self) -> &'a [u8; 20];
    fn to_addr(&self) -> Option<&'a [u8; 20]>;
    fn value_bytes(&self) -> &'a [u8];
    fn gas(&self) -> u64;
    fn gas_used(&self) -> u64;
    fn input(&self) -> &'a [u8];
    fn output(&self) -> &'a [u8];
    fn status(&self) -> u8;
    fn depth(&self) -> u64;
    fn selector(&self) -> Option<&'a [u8; 4]>;
    fn has_value(&self) -> bool;
}
```

The lazy parse step decodes only enough RLP structure to locate each field's
byte range. Payload fields remain borrowed slices into the original frame
bytes; they are not copied or re-encoded.

This gives a simple split of responsibilities:

- block metadata resolves `trace_id -> blob byte range`
- `CallFrameView` resolves `frame bytes -> field byte ranges`
- materialization reads borrowed payload bytes directly

### Block-level iteration

`BlockTraceIter<'a>` walks the outer RLP list structure, yielding
`(tx_idx, CallFrameView<'a>)` pairs in flat order. This is the primitive used
by both ingest (to build indexes) and query materialization (to extract
matching frames).

`trace_idx` is the flat encounter order within the transaction.

## Query

### query_traces

Filter fields: `from`, `to`, `selector`, `is_top_level`, `has_value`.

At least one indexed filter (`from`, `to`, `selector`, or `has_value`) is
required. Unfiltered trace queries and queries with only `is_top_level` are not
supported by this backend — they would degenerate to a full scan of every trace
in the range, which is prohibitively expensive given trace volume per block.

Execution follows the same pipeline as logs:

1. Resolve the block range to a trace-ID range via the directory
2. For each active indexed filter field, load the corresponding bitmap stream
   pages
3. Intersect bitmaps to produce candidate trace IDs
4. For each candidate, load the block trace blob and header, navigate to the
   frame via the offset table, construct a `CallFrameView`, and check any
   post-filters (`is_top_level`)
5. Materialize requested fields from the view

### Native transfers

At the substrate level there is no separate `query_transfers` method. The RPC
layer maps `eth_queryTransfers` to `query_traces` with `has_value: true`
implicitly set and `selector` excluded from the allowed filter surface.

The materializer for transfers extracts a smaller field set (from, to, value,
position fields) from the same `CallFrameView`.

## Dependency

The only new dependency is `alloy-rlp`. It provides RLP header decoding
primitives with no transitive Ethereum domain types. The dependency stays
isolated to the trace family module; the rest of the crate does not depend on
it.

## Block Hash Index

The `block_hash_index` table (hash → block_num reverse lookup) is a shared
artifact, not owned by any single family. It is written once per block during
ingest, regardless of which families are active. Currently all families are
always ingested together, but the hash index should not depend on that
assumption.

The trace family does not maintain its own hash index. It uses the shared one
for any hash → block_num resolution.

## Artifact Write Order

Within a single block's trace ingest:

1. `block_trace_blob` — raw RLP bytes
2. `block_trace_header` — offset table
3. `block_record` — shared block sequencing metadata
4. directory fragments — `trace_dir_by_block` rows
5. stream fragments — `trace_bitmap_by_block` rows
6. compaction — directory sub-buckets, stream pages (when boundaries seal)

All trace artifacts are written before the shared `publication_state` head
advance, consistent with the existing publication ordering invariant.

## State From Head

`TracesFamily::load_state_from_head` derives `TraceSequencingState` from the
published `indexed_finalized_head` by reading the shared `block_record` for the
head block and computing `next_trace_id = traces.first_primary_id + traces.count`.

If no shared block records with a `traces` window exist (fresh instance),
`next_trace_id` starts at 0.
