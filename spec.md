# `eth_getLogs` Index Spec: Roaring Bitmaps with Global Log IDs

**Status:** Draft · **Scope:** High-level design

---

## Goal

Provide sub-millisecond `eth_getLogs` queries whose cost scales with the number of **matching logs**, not the number of blocks in the requested range. Avoid full-receipt deserialization entirely.

---

## Core Idea

Maintain roaring bitmap inverted indices keyed by address and topic values. Bitmaps are over a **global log ID** space (not block numbers), giving log-level precision on lookup. The one exception is **topic0** (event signatures), which is indexed at **block-level granularity** to avoid massive write amplification for ubiquitous signatures like `Transfer`.

---

## Storage Tables

### 1. `Logs` — Canonical log data

```
Key:   global_log_id  (uint64, monotonically increasing)
Value: Log { address, topics[], data, block_num, tx_idx, log_idx }
```

Logs are stored **flat**, outside the receipt trie. One sequential read per log; no RLP receipt decoding required.

### 2. `BlockLogRange` — Block ↔ log-ID mapping

```
Key:   block_num      (uint64 BE)
Value: { first_log_id: uint64, count: uint32 }
```

Converts a `[fromBlock, toBlock]` range into a `[first_log_id, last_log_id]` range in O(2) reads. Also supports reverse mapping (binary search on `first_log_id` to recover block number from a log ID).

### 3. `LogAddressIndex` — Address → log-level bitmap (chunked)

```
Key:   address (20 bytes) ++ chunk_upper_log_id (uint64 BE)
Value: serialized RoaringBitmap<global_log_id>
```

One entry per chunk. Chunks are bounded by entry count (~1950 entries), not by log-ID range. MDBX DupSort on `address` with sorted dup values keyed by `chunk_upper_log_id`.

### 4. `LogTopic0Index` — Event signature → block-level bitmap (chunked)

```
Key:   topic0_hash (32 bytes) ++ chunk_upper_block (uint64 BE)
Value: serialized RoaringBitmap<block_num>
```

**Block granularity, not log-ID.** There are ~10K unique event signatures total. The hot ones (`Transfer`, `Approval`, `Swap`) appear in nearly every block. Block-level bitmaps keep write amplification manageable while still providing useful coarse pre-filtering. Topic0 alone is never selective — selectivity comes from address + argument topics.

### 5. `LogTopic1Index`, `LogTopic2Index`, `LogTopic3Index` — Argument topics → log-level bitmap (chunked)

```
Key:   topic_hash (32 bytes) ++ chunk_upper_log_id (uint64 BE)
Value: serialized RoaringBitmap<global_log_id>
```

Same structure as `LogAddressIndex`. Argument topics (indexed event parameters) are the selective dimensions — a specific `from` or `to` address in a Transfer event, a specific pool in a Swap, etc.

---

## Chunk Strategy

All bitmap tables use **entry-count-bounded chunks** (not range-bounded):

- **Chunk limit:** ~1950 entries per chunk (tuned to fit one MDBX leaf page when serialized).
- **Chunk boundary key** is the upper bound (log-ID or block-num) of the entries in that chunk.
- Rare values (obscure NFT contract) may have a single chunk spanning the entire chain.
- Hot values (USDC) have many small chunks, each covering a narrow log-ID range.
- Chunks are **append-only** during normal operation. Only the latest chunk for each value is mutable.
- On **reorg**, truncate the latest chunk for affected values.

---

## Query Path

```
eth_getLogs(fromBlock: 18M, toBlock: 18.001M, address: USDC, topics: [Transfer, _, Bob])
```

### Step 1 — Convert block range to log-ID range

```
BlockLogRange[18_000_000].first_log_id  → 5_200_000
BlockLogRange[18_001_000].first_log_id + count → 5_502_000
log_id_range = [5_200_000, 5_502_000)
```

### Step 2 — Fetch bitmaps (chunked cursor seeks)

```
bm_addr   = LogAddressIndex.range_query(USDC, log_id_range)        // log-level
bm_topic0 = LogTopic0Index.range_query(Transfer, block_range)      // block-level
bm_topic2 = LogTopic2Index.range_query(Bob, log_id_range)          // log-level
```

Each range query is an MDBX cursor seek to the first overlapping chunk, then a forward scan through chunks until past the range. Typically 1–3 chunk reads per value.

### Step 3 — Intersect

Topic0 is at block granularity, so convert it to a log-ID mask first:

```
bm_topic0_logs = expand_block_bitmap(bm_topic0, BlockLogRange)
// For each set block, set all log-IDs in [first_log_id, first_log_id + count)
```

Then intersect everything in log-ID space:

```
candidates = bm_addr AND bm_topic0_logs AND bm_topic2
// → {5_340_201, 5_340_887, 5_401_332, 5_488_019}
```

### Step 4 — Point reads

```
results = candidates.map(id => Logs[id]).filter(exact_match)
```

4 log reads. Each log is ~200 bytes. Final exact-match filter catches any residual false positives from the topic0 block-level expansion (where a block matched Transfer but the specific log at that ID was a different event). In practice the address + argument topic intersection already eliminated nearly all false positives.

---

## Write Path

On each new block:

1. Assign `global_log_id` starting from `prev_block.first_log_id + prev_block.count`.
2. Write each log to `Logs[global_log_id]`.
3. Write `BlockLogRange[block_num] = { first_log_id, count }`.
4. For each log:
   - Append `global_log_id` to `LogAddressIndex[log.address]` latest chunk.
   - Append `block_num` to `LogTopic0Index[log.topics[0]]` latest chunk (dedup within block).
   - For `i` in `1..=3`: if `log.topics[i]` exists, append `global_log_id` to `LogTopic{i}Index[log.topics[i]]` latest chunk.
5. Flush any chunks that hit the entry-count limit.

### Reorg handling

1. Look up `BlockLogRange` for the invalidated blocks to get the affected log-ID range.
2. Truncate the latest chunk of every bitmap that has entries in the invalidated range. (Only the latest chunk can be affected since chunks are append-only.)
3. Delete `Logs` entries in the invalidated range.
4. Delete `BlockLogRange` entries for invalidated blocks.

---

## Space Estimate

| Component | Per-entry cost | Ethereum mainnet (~10B map values) | High-throughput chain (500B MV) |
|-----------|---------------|-------------------------------------|----------------------------------|
| `Logs` table | ~180 B/log | ~500 GB | ~25 TB |
| `LogAddressIndex` | ~2 B/entry | ~5.7 GB | ~285 GB |
| `LogTopic0Index` | ~2 B/entry (block-level) | ~0.04 GB | ~2 GB |
| `LogTopic{1,2,3}Index` | ~2 B/entry | ~5 GB | ~250 GB |
| `BlockLogRange` | 12 B/block | ~0.24 GB | ~0.24 GB |
| Chunk key overhead | varies | ~1-2 GB | ~10-50 GB |
| **Total index overhead** | | **~12-14 GB** | **~550-590 GB** |

Index overhead is ~2.5% of raw log storage. The dominant cost is always the logs themselves.

---

## Granularity Rationale: Why Topic0 Is Block-Level

Event signatures have extreme write amplification at log-level granularity:

- `Transfer` (topic0 = `0xddf2...`) appears in ~30% of all logs on Ethereum. At log-level, its bitmap would have ~3B entries, producing ~1.5M chunks. Every single block appends dozens of entries, causing constant chunk flushes.
- At block-level, `Transfer`'s bitmap has ~20M entries (one per block it appears in), with far fewer chunk flushes.
- Topic0 is **never selective on its own** — it's always combined with an address or argument topic that narrows the candidate set. The block-level expansion in Step 3 adds at most a few hundred false-positive log-IDs per candidate block, which the address/argument intersection immediately eliminates.

The cost of block-level topic0: on a high-throughput chain where a candidate block has 5000 logs, the `expand_block_bitmap` step produces a 5000-entry temporary bitmap for that block. But this only applies to the ~3-5 candidate blocks surviving the address intersection — the total expansion is bounded and negligible.

---

## Alternatives Considered

| Approach | Verdict |
|----------|---------|
| All bitmaps at block granularity (Erigon 2) | Requires full-receipt scan per candidate block. Unacceptable at >1000 logs/block. |
| All bitmaps at log-ID granularity | Works but ~50x write amplification for hot topic0 values. |
| Filtermaps (EIP-7745) | Superior theoretical properties (Merkle provable, no per-value state). Higher implementation complexity. Worth considering if light-client proofs are a priority. |
| Bloom filters (legacy) | O(blocks) query cost. Not viable for range queries. |
