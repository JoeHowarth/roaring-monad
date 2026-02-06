# `eth_getLogs` Finalized Index Spec v2: Backend-Agnostic Persistent Index

**Status:** Proposed v2  
**Scope:** This crate handles **finalized** log indexing and persistent query acceleration only.

---

## 1. Goals

1. Serve finalized `eth_getLogs` with cost that scales with matching logs, not scanned blocks.
2. Support exact JSON-RPC filter semantics over finalized data.
3. Avoid receipt-trie scans and receipt deserialization on the query path.
4. Support KV/object-store backends without cross-key transactions.
5. Keep restart/recovery fast without full log reindex.

---

## 2. Crate Boundary

### In Scope

- Persistent storage and querying for **finalized** blocks.
- Chunked roaring inverted indexes.
- Finalized block ingest.
- Metadata/state required for correctness and recovery.

### Out of Scope

- Tracking non-finalized tip blocks.
- Serving `latest`/tip window directly.
- Reorg-window buffering/rollback for unfinalized blocks.
- P2P/head-following/finality detection.

This crate assumes it is fed a finalized canonical block stream by an upstream component.

---

## 3. Assumptions

1. Upstream provides blocks in strictly finalized canonical order.
2. Backend provides durable `get`/`put` and CAS/versioned update for small metadata records.
3. Exactly one active writer exists (lease + fencing token).
4. Every metadata/canonical write/delete in this crate is fencing-guarded.

---

## 4. Core Decisions

1. Roaring inverted indexes are the primary access path.
2. Chunk blobs are immutable; manifests are mutable visibility pointers.
3. Global log IDs are `uint64` and indexed as sharded roaring32 (`hi32` shard + `lo32` value).
4. `topic0` is hybrid: always block-level, optional log-level for cold signatures.
5. This crate indexes and serves finalized range only (`<= meta/state.indexed_finalized_head`).

---

## 5. Data Model

### 5.1 Canonical Log Identity

```
global_log_id: uint64
shard_hi32 = global_log_id >> 32
local_lo32 = global_log_id & 0xffffffff
```

### 5.2 Canonical Tables

#### `Logs`

```
Key:   global_log_id (uint64 BE)
Value: Log { address, topics[0..3], data, block_num, tx_idx, log_idx, block_hash }
```

#### `BlockMeta`

```
Key:   block_num (uint64 BE)
Value: {
  block_hash: H256,
  parent_hash: H256,
  first_log_id: uint64,
  count: uint32
}
```

#### `BlockHashToNum`

```
Key:   block_hash (H256)
Value: block_num (uint64)
```

#### `Meta`

```
meta/state -> {
  indexed_finalized_head: uint64,
  next_log_id: uint64,
  writer_epoch: uint64
}
```

---

## 6. Index Streams and Objects

A stream is one indexed value in one shard:

```
stream_id = index_kind || value_hash || shard_hi32
```

Index kinds:

- `addr` (log-level)
- `topic1` (log-level)
- `topic2` (log-level)
- `topic3` (log-level)
- `topic0_block` (block-level; shard by block_hi32)
- `topic0_log` (log-level, enabled for cold signatures)

### 6.1 Immutable Sealed Chunks

```
chunks/{stream_id}/{chunk_seq} -> ChunkBlob
ChunkBlob = {
  encoding: roaring32,
  min_local: uint32,
  max_local: uint32,
  count: uint32,
  crc32: uint32,
  payload: bytes
}
```

Chunks are append-only and never modified in place.

### 6.2 Stream Manifest (Atomic Boundary)

```
manifests/{stream_id} -> {
  version: uint64,
  last_chunk_seq: uint64,
  chunk_refs: [ChunkRef],
  tail_ref: optional TailRef,
  approx_count: uint64
}
```

`ChunkRef` is required to contain:

```
ChunkRef = {
  chunk_seq: uint64,
  min_local: uint32,
  max_local: uint32,
  count: uint32
}
```

For large streams, split `chunk_refs` into manifest segments and CAS only a small header pointer.

### 6.3 Mutable Tail Checkpoint

```
tails/{stream_id} -> {
  entries: roaring32,
  count: uint32
}
```

Tail is mutable and persisted periodically; it is included in finalized reads.

---

## 7. Chunking Policy

### 7.1 Seal Conditions

Seal tail when any condition is true:

1. `entry_count >= target_entries` (default `1950`)
2. `serialized_bytes >= target_bytes` (backend tuned)
3. maintenance seal interval elapsed (default `10 minutes`)

### 7.2 Tail Flush Policy

Persist tails at least:

1. once per finalized block ingest
2. or every `5 seconds` (whichever comes first)

### 7.3 Why Chunk Size Stays Large

Small chunk sizes to force frequent seals cause severe metadata/read amplification, especially for block-level `topic0` streams with low append rate. Keep chunk size near page-efficient target and rely on tail persistence.

---

## 8. `topic0` Hybrid Strategy

### 8.1 Always-On Block-Level

Every `topic0` signature is indexed in `topic0_block` with per-block dedup.

### 8.2 Optional Log-Level for Cold Signatures

```
topic0_mode/{sig} -> {
  log_enabled: bool,
  enabled_from_block: uint64
}
```

Rolling-window bookkeeping state (for 50k-block policy) is persisted in:

```
topic0_stats/{sig} -> {
  window_len: uint32,             // default 50_000
  blocks_seen_in_window: uint32,  // count of blocks containing sig
  ring_cursor: uint32,            // position in rolling ring
  ring_bits: bytes                // one bit per block slot
}
```

This state can be rebuilt from `topic0_block` if missing, but normal startup uses persisted state.

Policy:

- Enable log-level when signature appears in `< 0.1%` of blocks over rolling `50k` blocks.
- Disable when it exceeds `1.0%` over rolling `50k` blocks.
- Hysteresis prevents flapping.

---

## 9. Query Semantics (Finalized Only)

### 9.1 Filter Rules

- `address`: single value or OR-list.
- `topics[i]`: `null` wildcard, single value, or OR-list.
- OR within clause, AND across clauses.
- `blockHash` cannot be combined with `fromBlock`/`toBlock`.
- Query API supports optional `max_results`; planner/executor may stop early once limit is met.

### 9.2 Planner

1. Read `meta/state` and `Hf = indexed_finalized_head`.
2. If `blockHash` present, execute Section 9.4.
3. Resolve `fromBlock`/`toBlock` tags to numeric block numbers.
4. Clip range to `[0, Hf]`; if empty, return `[]`.
5. Resolve clipped block range to log-id interval via `BlockMeta` endpoints.
6. Compute shard ranges from clipped log-id interval:
   - log-level shard range: `[from_log_id >> 32, to_log_id >> 32]`
   - block-level shard range for `topic0_block`: `[from_block >> 32, to_block >> 32]`
7. For each clause value, discover and read all overlapping shards in range, then union per-value bitmaps.
8. Estimate clause cardinality from overlapping `ChunkRef.count` sums plus tail counts in range.
9. Intersect clauses in ascending estimated cardinality.
10. Apply block-level predicates (`topic0_block`) as late block-membership filter on surviving candidates.
11. Fetch candidate `Logs`, run exact filter, return sorted `(block_num, tx_idx, log_idx)`.

Rationale for Step 10:

- Late `topic0_block` checking avoids materializing block-expanded log-id masks.
- Complexity is proportional to surviving candidate logs, not `matched_blocks * logs_per_block`.

Large OR-list guardrail:

- If OR-list cardinality for any clause exceeds `planner.max_or_terms`, planner may return `query too broad` or switch to block-driven scan policy (operator-configured).
- Default `planner.max_or_terms` is implementation-defined and must be surfaced in config/metrics.

### 9.3 Topic0-Only Queries

If query has only block-level `topic0` predicates and no selective log-level clauses, run block-driven scan over matched finalized blocks and exact-filter logs.

### 9.4 `blockHash` Execution

1. If `fromBlock` or `toBlock` is also present: return `-32602` (invalid params).
2. Lookup `BlockHashToNum[blockHash]`.
3. If missing: return `-32000` (`Block not found.`).
4. Read `BlockMeta[num]`, verify hash equality.
5. If mismatch: return `-32000` (`Block not found.`).
6. Execute normal filtering constrained to that single block.

---

## 10. Write Path (Finalized Blocks)

### 10.1 Invariants

1. Only finalized blocks are ingested.
2. `indexed_finalized_head` advances only after all data for that block is durable.
3. All writes are idempotent via deterministic keying.
4. Immutable chunk + CAS manifest update pattern is mandatory.
5. Every write/delete is fenced by active writer epoch.
6. Tail re-append after crash is safe because roaring inserts are set-idempotent.

### 10.2 Per-Block Finalized Ingest (`B`)

1. Verify active writer lease (epoch `E`).
2. Read `meta/state`; require `B == indexed_finalized_head + 1`.
3. Verify parent linkage with `BlockMeta[indexed_finalized_head]` (or genesis base).
4. Assign `first_log_id = next_log_id`.
5. Write `Logs` for block logs.
6. Write `BlockMeta[B]` and `BlockHashToNum[hash(B)]`.
7. Update stream tails; seal/write chunks as thresholds hit; CAS manifests.
8. Persist updated tails.
9. CAS `meta/state`:
   - `indexed_finalized_head = B`
   - `next_log_id = first_log_id + block_log_count(B)`
   - `writer_epoch = E`

If commit CAS fails, retry idempotently.

---

## 11. Finality Violation Handling

This crate assumes finalized input is stable. If upstream violates that assumption (e.g., a different block appears at `<= indexed_finalized_head`):

1. Enter `degraded` fail-closed mode immediately.
2. Stop ingest and emit critical alert.
3. Require operator runbook (`finality-violation-rebuild`) to rebuild from divergence point.

No automatic rollback of persisted finalized index is attempted in v2.

---

## 12. Recovery and GC

### 12.1 Startup Recovery

1. Read `meta/state`.
2. Load lightweight planner stats catalogs.
3. Load manifests/tails lazily on first stream touch; keep LRU cache.
4. Optional async warmup of top-K hot streams.

No full scan of `Logs` is required.

### 12.2 GC

Periodic GC removes:

- orphan chunk blobs not referenced by manifests
- stale manifest segments
- stale superseded tail checkpoints
- optional stale `block_hash_to_num/*` entries when history pruning is enabled

Hard guardrails:

- `orphan_chunk_bytes <= gc.max_orphan_chunk_bytes`
- `orphan_manifest_segments <= gc.max_orphan_manifest_segments`
- `stale_tail_keys <= gc.max_stale_tail_keys`

If caps are exceeded, throttle ingest or fail closed (operator policy).

---

## 13. Integration Into Full Workflow (Out of Scope)

This crate is one component in a larger log-serving pipeline.

Typical architecture:

1. **Chain/finality service** tracks canonical head and finalization depth.
2. **Tip-window service** maintains recent non-finalized logs in memory and serves tip-range queries.
3. **This crate** ingests only blocks declared finalized and serves finalized-range queries.
4. **RPC composition layer** splits `eth_getLogs` request:
   - finalized portion -> this crate
   - non-finalized tip portion -> tip-window service
   - merge + stable sort + return

The tip-window service and composition layer are intentionally out of scope for this crate.

---

## 14. Required Tests

1. JSON-RPC filter parity (`address` OR, topic OR/null, `blockHash` mode) on finalized range.
2. Crash injection through ingest steps with recovery correctness.
3. Idempotent reprocessing of same finalized block.
4. Lease-loss fencing prevents stale-writer mutations.
5. Lazy manifest/tail loading behavior under cold-start traffic.
6. GC guardrail behavior and backpressure/fail-closed actions.

---

## 15. Summary

This crate provides a strict, backend-agnostic finalized log index with chunked roaring bitmaps, deterministic write visibility, and exact finalized query semantics. Tip-window handling is a separate concern and must be composed outside this crate.
