# Trace Family Implementation Plan

> Sibling to [trace-family.md](trace-family.md) (current implementation).
> This document sequences the implementation into concrete steps.

## Phases

The work is ordered so that each phase produces a compilable, testable
increment. Later phases depend on earlier ones but are independently
commitable.

---

### Phase 1: Shared substrate changes

These changes are prerequisites that touch shared code outside the trace
family module.

#### 1a. BucketedOffsets

Add `BucketedOffsets` to the shared substrate (`src/core/` or a new
`src/core/offsets.rs`).

- struct with a prefix array (cumulative entity counts per bucket) and a
  `Vec<Vec<u32>>` of per-bucket low-order offsets
- `push(offset: u64)` — appends an offset, starting a new bucket when the u32
  range is crossed
- `get(i: usize) -> u64` — reconstructs the full offset for entity `i`
- `len() -> usize`
- `encode() -> Bytes` / `decode(bytes: &[u8]) -> Result<Self>`
- unit tests: single-bucket (all offsets < 4G), multi-bucket (offsets crossing
  4G boundary), empty, round-trip encode/decode

The logs family's `BlockLogHeader` can continue using its existing plain u32
offset array for now. Migration to `BucketedOffsets` is mechanical and can
happen in a follow-up.

#### 1b. TraceId types

Add `TraceId`, `TraceShard`, `TraceLocalId` to `src/core/ids.rs`, mirroring
the `LogId` family. Same 40-bit shard + 24-bit local split, same constants.

Alternatively, generalize the ID types into a single parameterized family if
the duplication is excessive — but only if it doesn't complicate the logs code.
Start with concrete types; refactor later if warranted.

#### 1c. FinalizedBlock envelope change

Change `FinalizedBlock.traces` from `Vec<Trace>` to `trace_rlp: Vec<u8>`.

Update all call sites that construct `FinalizedBlock` (tests, ingest engine,
test fixtures). The scaffold `Trace` struct is removed.

#### 1d. alloy-rlp dependency

Add `alloy-rlp` to the crate's `Cargo.toml`. Pin a specific version.

#### 1e. Block hash index extraction

Move `block_hash_index` writes out of the logs family into the shared ingest
coordinator (`Families::ingest_block` or `IngestEngine`). The hash index is
written once per block, not per family.

Update the logs ingest path to stop writing it. Update tests.

---

### Phase 2: Zero-copy RLP accessors

This phase is pure library code with no storage dependencies. It can be tested
entirely with synthetic RLP payloads.

#### 2a. CallFrameView

`src/traces/view.rs`

- `CallFrameView<'a>` — borrows a single RLP-encoded call frame
- lazy parse: on first access, walk the RLP list to compute all field byte
  ranges, cache them in a `FrameLayout` struct
- field accessors: `typ`, `flags`, `from_addr`, `to_addr`, `value_bytes`,
  `gas`, `gas_used`, `input`, `output`, `status`, `depth`, `selector`,
  `has_value`, `is_call_type`
- `is_call_type` returns true for Call/DelegateCall/CallCode/StaticCall (typ 0
  with flags 0 or 1, typ 1, typ 2)
- error handling: return `Result` from the parse step; individual accessors
  can assume the parse succeeded

Tests:
- construct valid RLP frames for each CallKind variant, verify all accessors
- test edge cases: empty input, empty output, zero value, max-size
  input/output, None `to` (create)
- test malformed RLP detection

#### 2b. BlockTraceIter

`src/traces/view.rs`

- `BlockTraceIter<'a>` — walks the outer `Vec<Vec<CallFrame>>` RLP structure
- yields `(tx_idx: u32, trace_idx: u32, CallFrameView<'a>)` in flat order
- also yields `(byte_offset, byte_length)` for each frame relative to the blob
  start, which ingest needs for the offset header

Tests:
- multi-tx block with varying trace counts per tx
- empty tx (zero traces in one transaction)
- single-tx block
- empty block (zero transactions)

---

### Phase 3: Trace storage types and table infrastructure

#### 3a. Trace keys and constants

`src/traces/table_specs.rs`

- table IDs: `TRACE_BLOCK_RECORD_TABLE`, `BLOCK_TRACE_HEADER_TABLE`,
  `TRACE_DIR_BUCKET_TABLE`, `TRACE_DIR_SUB_BUCKET_TABLE`,
  `TRACE_DIR_BY_BLOCK_TABLE`, `TRACE_BITMAP_BY_BLOCK_TABLE`,
  `TRACE_BITMAP_PAGE_META_TABLE`, `TRACE_OPEN_BITMAP_PAGE_TABLE`
- blob table ID: `BLOCK_TRACE_BLOB_TABLE`, `TRACE_BITMAP_PAGE_BLOB_TABLE`
- reuse the same bucket/sub-bucket/page span constants as logs (1M, 10K,
  4096). If these need to differ for traces, parameterize later.
- stream ID helpers for `from`/`to`/`selector`/`has_value` index kinds

#### 3b. Trace codec

`src/traces/codec.rs`

- `BlockTraceHeader` — encoding version, `BucketedOffsets`, `tx_starts:
  Vec<u32>`, encode/decode
- `TraceBlockRecord` — `block_hash`, `parent_hash`, `first_trace_id`, `count`,
  encode/decode using the same `fixed_codec` pattern as logs `BlockRecord`
- reuse `DirBucket`, `DirByBlock`, `StreamBitmapMeta` from the logs codec
  directly (they are structurally identical for traces, just scoped to trace
  IDs)

#### 3c. Trace table specs

`src/traces/table_specs.rs`

- typed table wrappers for each trace table, following the logs pattern
- key construction helpers

#### 3d. Tables registration

Add trace tables to `Tables<M, B>` struct in `src/tables.rs`. Add accessor
methods.

---

### Phase 4: Trace sequencing state

#### 4a. TraceSequencingState

`src/traces/types.rs` and `src/traces/mod.rs`

- `TraceSequencingState { next_trace_id: TraceId }`
- derive from `trace_block_record` at the published head, same as logs

#### 4b. Status integration

- update `FamilyStates` to hold `TraceSequencingState` instead of
  the old trace startup alias
- update `ServiceStatus` accordingly
- update `TracesFamily::load_state_from_head_record` to read `trace_block_record` and
  derive `next_trace_id`

---

### Phase 5: Trace ingest

This is the largest phase. Follow the logs ingest structure closely.

#### 5a. Trace artifact persistence

`src/traces/ingest.rs`

- `persist_trace_artifacts` — iterate `BlockTraceIter` over the raw RLP,
  collect byte offsets and tx boundaries, build `BlockTraceHeader`, write
  `block_trace_blob` and `block_trace_header`
- `persist_trace_block_record` — write `TraceBlockRecord`
- `persist_trace_dir_by_block` — write directory fragments, same logic as
  logs

#### 5b. Stream index collection and persistence

`src/traces/ingest.rs`

- `collect_trace_stream_appends` — iterate `BlockTraceIter`, for each frame
  extract `from`, `to` (if present), `selector` (if call-type and input >= 4
  bytes), `has_value` (if value > 0), group by stream ID and shard
- `persist_trace_stream_fragments` — write `trace_bitmap_by_block` rows,
  same machinery as logs
- open-page marker tracking for trace bitmap pages

#### 5c. Compaction

`src/traces/ingest.rs` plus shared helpers under `src/ingest/`

- directory compaction (sub-bucket, bucket) — same logic as logs, different
  table IDs
- stream page compaction — same logic as logs, different table IDs

#### 5d. TracesFamily::ingest_block

Wire up the family entry point:

1. walk `BlockTraceIter` once, collecting offsets, tx boundaries, and stream
   appends in a single pass
2. persist blob + header
3. persist block record
4. persist directory fragments
5. persist stream fragments
6. run compaction for sealed boundaries
7. advance `next_trace_id`
8. return trace count

#### 5e. Ingest tests

- single block with known traces, verify all artifacts written correctly
- multi-block batch crossing directory sub-bucket and page boundaries
- empty block (zero traces)
- block with transactions that have zero traces
- block with traces that have large input/output (verify BucketedOffsets
  multi-bucket path)

---

### Phase 6: Trace query

#### 6a. TraceFilter

`src/traces/filter.rs`

- `TraceFilter { from, to, selector, is_top_level, has_value }`
- validation: at least one indexed field required
- stream ID resolution for each active filter field

#### 6b. Query execution

`src/traces/query/engine.rs`

- `TracesQueryEngine` — same structure as `LogsQueryEngine`
- range resolution via trace directory
- bitmap loading and intersection
- candidate materialization via `CallFrameView`
- `is_top_level` post-filter

#### 6c. API surface

- add `query_traces` to `FinalizedHistoryService`
- `QueryTracesRequest` mirroring `QueryLogsRequest`
- no separate `query_transfers` method at this layer

#### 6d. Query tests

- filter by `from` only
- filter by `to` only
- filter by `selector`
- filter by `has_value` (transfer query)
- compound filters (from + to, from + has_value)
- `is_top_level` post-filter
- pagination across blocks
- verify unfiltered query is rejected

---

## Implementation notes

### Code reuse

The logs and trace families share substantial structural overlap in directory
management, stream indexing, compaction, and pagination. The initial
implementation should duplicate the logs patterns into the traces module with
trace-specific types and table IDs. Once both families are complete, common
logic can be extracted into shared utilities if the duplication is excessive.
Premature abstraction before the second family exists would be speculative.

### Testing strategy

Each phase includes its own tests. Integration tests that exercise the full
ingest-then-query path should use the existing in-memory store backends.

Synthetic RLP payloads for testing should be constructed using `alloy_rlp`
encoding, not hand-crafted bytes.

### What is not in scope

- Descending traversal
- Field selection / partial materialization at the RPC level
- Relation joins (traces → transactions, traces → blocks)
- Migration of the logs family to `BucketedOffsets`
- Refactoring shared logic between logs and traces into generic utilities
