# Bitmap-Indexed History Queries

## The problem

`eth_queryLogs` (a proposed successor to `eth_getLogs` with field selection, relation joins, and improved pagination) returns event logs matching address and topic filters within a block range. A naive implementation scans every log in the range even when the filter is highly selective. For large ranges or busy chains this is prohibitively expensive.

The same problem applies to the other proposed query methods (`eth_queryTransactions`, `eth_queryTraces`, `eth_queryTransfers`), each of which filters a different object type by different fields over a block range. This document uses logs as the running example because they are the first implementation target, but the approach generalizes.

## Monotonic log IDs

Finalized logs are assigned a monotonic global ID as they're ingested. This ID is the primary query and pagination identity instead of block number or transaction index.

Finalized history is append-only, so these IDs are stable once assigned. A log's ID never changes, and no gaps appear retroactively.

## Bitmap indexes

For each indexed value (an address, a topic at a given position), a roaring bitmap tracks which global log IDs contain that value.

For example, if address `0xA` appears in logs 3, 7, 8, and 15, the bitmap for `0xA` is `{3, 7, 8, 15}`.

A compound filter like "address = 0xA AND topic[0] = 0xB" becomes a bitmap intersection:

```
result = bitmap("address/0xA") & bitmap("topic0/0xB")
```

The result is the set of log IDs matching all clauses, in ascending order.

### Why roaring bitmaps

Standard bitsets waste space when the ID space is large but sparsely populated. Roaring bitmaps compress sparse regions as sorted integer arrays and dense regions as traditional bitsets, switching per 2^16-element chunk. They're compact for both sparse and dense streams, support fast intersection/union at the chunk level, and iterate set bits in order.

## Partitioning the ID space

A single global bitmap per indexed value would grow without bound. To keep bitmaps bounded, the ID space is partitioned into shards of 2^24 (≈16.8M) IDs. The shard number is encoded in the upper 40 bits of the log ID so it's derivable from any ID without a lookup. Within each shard, bitmaps are further divided into fixed-size pages of 4,096 local IDs.

Query execution walks shards in order, and within each shard loads only the pages that overlap the query's ID window. Fully written (sealed) pages don't change.

## Resolving log IDs to payloads

Bitmaps identify *which* log IDs matched, but the actual log bytes are stored per-block. A directory structure maps log ID ranges to block numbers:

```
log_id 120,000,000–120,000,002  →  block 5001
log_id 120,000,003–120,000,007  →  block 5003
...
```

Given a matched log ID, the directory lookup yields the block number and the log's position within that block, so a byte-range read can retrieve just that log's payload without fetching the entire block.

## Exact-match filtering

The bitmap intersection identifies specific log IDs, not coarse page-level candidates. But the bitmap index may not cover every dimension of a filter. A query like `address=0xA AND topic[0]=0xB AND topic[1]=0xC` might have indexed bitmaps for address and topic[0] but not topic[1]. After intersection narrows to specific IDs, each materialized log is still checked against the full filter to catch dimensions the index didn't cover.

## Pagination

Monotonic IDs make pagination straightforward:

1. A request specifies a block range; the engine maps it to a log ID window.
2. The engine iterates matching IDs up to `limit + 1`. The extra candidate determines whether more results exist.
3. The last returned log ID serves as the internal resume position.
4. The next page resumes strictly after that ID without re-scanning.

The RPC layer translates this internal cursor into the block-aligned `cursorBlock` pagination defined by the query spec.

## Immutability and caching

Finalized history is append-only. Log IDs, sealed bitmap pages, directory buckets for completed ID ranges, and block payloads are all immutable once written and can be cached indefinitely with no invalidation logic.

The only mutable state is the publication head, which tracks how far ingestion has progressed. Readers clip their queries against this head so they never observe partially-written data.

## Generalization beyond logs

Any object type that can be assigned a monotonic ID during finalized ingestion can use the same structure:

- **Transactions**: bitmap indexes on `from`, `to`, and `selector` fields
- **Traces**: bitmap indexes on `from`, `to`, `selector`, with a flag index for `isTopLevel`
- **Transfers**: bitmap indexes on `from`, `to`, with a flag index for `isTopLevel`

Each object type gets its own ID space, directory, and stream indexes. The bitmap intersection, sharding, materialization, and pagination machinery is shared. The current implementation targets logs as a proof of concept; extending to other types means adding new family adapters on top of the same substrate.

## Summary

| Concern | Mechanism |
|---------|-----------|
| Fast filtering | Roaring bitmap intersection per clause |
| ID → payload | Directory maps ID ranges to blocks; byte-range reads for individual objects |
| Pagination | Monotonic IDs as internal resume cursors, translated to block-aligned cursors at the RPC layer |
| Caching | Sealed artifacts are immutable, no invalidation needed |
| Scalability | Two-level partitioning (shards, then pages); only overlapping regions are loaded |
