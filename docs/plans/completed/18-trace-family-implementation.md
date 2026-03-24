# Trace Family Implementation

## Goal

Implement the trace family described in `docs/trace-family.md` as a real
finalized-history family with:

- authoritative raw per-block trace blobs
- compact per-block trace headers for random access
- trace-owned sequencing and block records
- family-scoped directory and bitmap artifacts over `TraceId`
- zero-copy trace materialization from stored RLP bytes
- query support for traces and transfer-style projections built on the same
  substrate

This plan is about execution order and concrete work packages. The target
architecture and intended semantics live in `docs/trace-family.md`.

## Problem

The crate has a traces family slot, but it is still scaffold-only.

Today:

- `src/traces.rs` exposes only placeholder types
- startup returns an empty `TraceStartupState`
- ingest rejects non-empty trace payloads
- there are no trace-owned tables, codecs, keys, or query paths
- the shared finalized block envelope still carries `Vec<Trace>` placeholders,
  not the target raw `trace_rlp` blob

That leaves three gaps:

1. the ingest coordinator has a trace family boundary but no real trace storage
2. the query layer has no trace-primary execution path
3. the current block envelope does not match the target authoritative trace
   representation

## Design Principles

### 1. Keep The Raw Trace Blob Authoritative

Do not normalize traces into a second canonical representation.

Persist the raw engine-emitted RLP bytes and build compact metadata and indexes
around that blob.

### 2. Reuse Shared Logs-Shaped Machinery Where It Actually Fits

The trace family should reuse the existing directory, bitmap, publication, and
ingest-order patterns where possible.

Do not introduce trace-only infrastructure when the logs family's existing
substrate is already the right abstraction.

### 3. Make Random Access Cheap Without Inflating Metadata

The block trace header should stay compact.

Prefer a `tx_starts` transaction-boundary array and offset metadata over
repeating per-trace tuples that duplicate derivable information.

### 4. Keep Materialization Zero-Copy

Trace materialization should borrow from stored bytes.

Parse only enough RLP structure to locate field ranges. Do not allocate or
re-encode payload fields during normal query execution.

### 5. Index Only The Predicates That Carry Their Weight

Start with:

- `from`
- `to`
- `selector`
- `has_value`

Keep `isTopLevel` as a post-filter unless real measurements show that a bitmap
index materially improves important workloads.

### 6. Tighten The Envelope Before Building Family Internals

The family should not be implemented around the temporary `Vec<Trace>` shape and
then migrated later.

Move the shared finalized block envelope to the target `trace_rlp` form early,
so ingest, tests, codecs, and query materialization all converge on one
authoritative input representation.

## Work Packages

### 1. Move The Shared Block Envelope To Raw Trace Bytes

Replace the placeholder trace payload in `FinalizedBlock` with the target raw
blob representation.

Concretely:

- add `trace_rlp: Vec<u8>` to the shared block envelope
- remove the scaffold `Vec<Trace>` trace payload from the ingest path
- delete the scaffold `Trace` struct once callers no longer depend on it
- update any helpers, fixtures, and tests that construct finalized blocks
- keep `txs` and `logs` behavior unchanged while the trace payload shape moves

Likely touch points:

- `src/block.rs` or `src/family.rs`, depending on the current finalized-block
  home
- ingest fixtures and startup/query tests that construct `FinalizedBlock`
- any existing tests that currently build `FinalizedBlock { traces: vec![] }`
  and must switch to `trace_rlp: Vec::new()`

This step is intentionally early. It establishes the correct source of truth
before any trace storage logic lands.

### 2. Add Shared Trace Substrate Pieces

Add the shared prerequisites that the trace family depends on but does not own.

Minimum pieces:

- add `alloy-rlp` to the crate dependency set
- add `TraceId`, `TraceShard`, and `TraceLocalId` alongside the existing shared
  primary-ID types
- move `block_hash_index` writes out of the logs family and into the shared
  ingest coordinator so the index is written once per block, not once per
  family

Likely touch points:

- `Cargo.toml`
- shared ID definitions such as `src/core/ids.rs` or the current equivalent
- `src/ingest/engine.rs` and/or `src/family.rs`
- logs ingest code that currently writes `block_hash_index`

Tests in this package should verify:

- `TraceId` encoding and arithmetic match the logs-family shape
- the shared block-hash index is still written exactly once per ingested block

### 3. Add Shared `BucketedOffsets`

Implement the shared offset utility described in `docs/trace-family.md` for
block-keyed blob random access.

This package should:

- define the shared structure and codec
- support the common single-bucket case efficiently
- support transparent growth past the u32 offset range
- expose simple point lookup by entity index

Likely implementation home:

- `src/core/offsets.rs` or another small shared substrate module

Tests in this package should cover:

- empty
- single-bucket offsets
- multi-bucket offsets crossing the u32 boundary
- encode/decode round-trips

Keep the first consumer small. The trace family should use it immediately. A
logs migration onto the shared utility can happen as a follow-up if that keeps
the trace rollout smaller.

### 4. Introduce Trace-Owned Types, Keys, Table Specs, And State

Create the trace family's persisted vocabulary under `src/traces/`.

Minimum targets:

- trace sequencing state
- trace block record type
- block trace header type with `BucketedOffsets` and `tx_starts`
- trace key suffix helpers
- trace table specs/constants
- trace stream-id helpers for `from`, `to`, `selector`, and `has_value`
- trace table registration and accessors in `src/tables.rs`

Likely modules:

- `src/traces/codec.rs`
- `src/traces/keys.rs`
- `src/traces/table_specs.rs`
- `src/traces/state.rs`
- `src/tables.rs`

The trace family should own its own names and codecs rather than extending
logs-local types.

### 5. Implement Zero-Copy Trace Views And Iteration

Add trace-owned read helpers over the raw RLP blob.

Minimum pieces:

- `BlockTraceIter<'a>` over the outer transaction/frame structure
- `CallFrameView<'a>` over a single frame's bytes
- lazy frame-layout parsing that caches field boundaries after first use
- helpers for indexed fields and materialized fields

This package consumes the `alloy-rlp` dependency added in Package 2 and uses its
header decoding primitives to locate field boundaries without copying payloads.

Likely module:

- `src/traces/view.rs`

This package should also define the exact flat encounter ordering rules used for:

- per-block trace enumeration
- `trace_idx` within a transaction
- global `TraceId` assignment across a block

Tests in this package should cover:

- valid frames for each supported trace kind
- malformed RLP detection
- empty input/output and missing `to`
- blocks with multiple transactions and varying trace counts
- transactions with zero traces
- exact `(tx_idx, trace_idx, byte range)` enumeration

### 6. Implement Trace Ingest Artifacts And Recovery State

Teach `TracesFamily::ingest_block` to write real trace artifacts.

Split the implementation into small family-local pieces.

Artifact persistence:

- decode the outer RLP structure enough to enumerate frames
- write `block_trace_blob`
- write `block_trace_header`
- write `trace_block_record`
- write trace directory fragments

Stream collection and persistence:

- assign flat trace order and `TraceId`s`
- collect appends for `from`, `to`, `selector`, and `has_value`
- write trace bitmap fragments

Compaction:

- seal/compact trace directory and bitmap tiers when boundaries close

Family entrypoint wiring:

- perform one logical pass over the block to collect offsets, transaction
  boundaries, and stream appends
- persist artifacts in publication-safe order
- advance `next_trace_id`
- return trace count

For empty trace blocks:

- write the empty header: encoding version, empty `BucketedOffsets`, and empty
  `tx_starts`
- write `trace_block_record` with `count = 0`
- follow the same publication ordering guarantees as other families

Likely modules:

- `src/traces/ingest/artifact.rs`
- `src/traces/ingest/stream.rs`
- `src/traces/ingest/compaction.rs`
- `src/traces/family.rs` or the current family entrypoint home

Tests in this package should cover:

- single-block ingest with known artifacts
- multi-block ingest crossing directory and page boundaries
- empty trace blocks
- blocks containing transactions with zero traces
- large blobs that exercise the multi-bucket offset path

Startup and recovery:

- replace the scaffold startup path with real trace sequencing recovery
- read the published finalized head and the trace block record at that head
- derive `next_trace_id = first_trace_id + count`
- initialize to zero when no trace records exist yet

Recovery behavior should match the crate's publication invariant: unpublished
trace artifacts must not affect the next startup state.

### 7. Add Query Planning, Service API, And Execution For Traces

Introduce a public service-level trace query surface and the family-local
execution path behind it.

Split the implementation into small family-local pieces.

Filter and planning:

- filter fields `from`, `to`, `selector`, `isTopLevel`, `has_value`
- resume and pagination keyed on `TraceId`
- block-range to trace-ID-range resolution through the trace directory
- block-hash to block-number resolution through the shared `block_hash_index`

Execution and materialization:

- indexed bitmap loading and intersection over trace streams
- post-filter evaluation for `isTopLevel`

Service surface:

- public service wiring for `query_traces`

Guardrails:

- require at least one indexed predicate
- reject unfiltered or `isTopLevel`-only trace scans

This package should stop at the service boundary. RPC transport wiring can land
later.

Likely modules:

- `src/traces/filter.rs`
- `src/traces/query/engine.rs`
- `src/api.rs`

Tests in this package should cover:

- `from`-only, `to`-only, `selector`, and `has_value` queries
- compound indexed filters
- `isTopLevel` post-filter behavior
- pagination and resume on `TraceId`
- block-hash query mode through the shared hash index
- rejection of unfiltered and `isTopLevel`-only queries

### 8. Add Transfer-Style Materialization On Top Of Traces

Implement the transfer projection as a trace query mode rather than a separate
artifact family.

This package should:

- map transfer-style requests to the trace query engine with `has_value = true`
- apply an additional success/status post-filter so reverted or failed
  value-carrying traces are excluded from transfer results
- exclude `selector` from the transfer filter surface
- materialize the smaller transfer field set from the same `CallFrameView`
- keep position semantics explicit: block number, transaction index, and flat
  `trace_idx`

Tests in this package should cover:

- transfer results from nested and top-level traces
- creates and missing-`to` traces where applicable
- reverted or otherwise failed value-carrying traces being excluded from the
  transfer projection

### 9. Expand End-To-End And Recovery Coverage

Add cross-phase coverage in the same spirit as the logs family.

Priority areas:

- startup state derivation
- empty and non-empty trace block ingest
- trace-ID sequencing across blocks
- block-header offset navigation
- zero-copy view correctness on representative frame shapes
- indexed filter correctness for `from`, `to`, `selector`, and `has_value`
- `isTopLevel` post-filter behavior
- resume and pagination correctness
- block-hash query-mode correctness through the shared hash index
- crash/retry behavior around publication boundaries
- transfer projection correctness from trace artifacts

Where possible, include small hand-authored RLP fixtures and larger generated
workloads.

### 10. Update Current-State Docs As Each Milestone Lands

As implementation packages land, keep `docs/overview.md` and any relevant topic
docs aligned with reality.

Do not wait until the end to reconcile documentation. The current-state docs
should stop describing traces as scaffold-only once that is no longer true.

## Suggested Execution Order

1. move the shared finalized block envelope to `trace_rlp`
2. add shared trace substrate pieces: `alloy-rlp`, shared `TraceId` types, and
   shared `block_hash_index` writes
3. add shared `BucketedOffsets`
4. add trace-owned types, keys, table specs, state, and table registration
5. implement zero-copy trace views and block iteration
6. implement trace ingest artifacts and recovery state round-trip
7. add the public service-level `query_traces` surface and trace query execution
8. add transfer-style projection on top of trace queries
9. expand end-to-end and crash/retry coverage
10. update current-state docs as each stage lands

## Implementation Notes

### Code Reuse

The logs and traces families share substantial structure in directory
management, stream indexing, compaction, and pagination.

The initial trace implementation should follow the logs pattern closely with
trace-owned types and table IDs. Extract shared helpers only after the trace
family is complete enough to show where duplication is truly excessive.

### Testing Strategy

Each package should land with its own focused tests. End-to-end ingest-then-
query coverage should continue using the existing in-memory backends.

Prefer synthetic RLP payloads built through `alloy-rlp` encoding helpers rather
than hand-crafted byte strings unless a specific malformed-byte case is being
tested.

## Verification

Each milestone should verify the crates it touches with the repo-standard
verification script.

Completion criteria for the full plan:

- non-empty trace payloads are accepted and persisted
- the published finalized head implies a recoverable `next_trace_id`
- every ingested block has a `trace_block_record`, including empty trace blocks
- trace queries execute through trace-owned indexes and materialization
- the public service exposes `query_traces`
- trace query pagination and resume are keyed on `TraceId`
- block-hash trace query mode reuses the shared `block_hash_index`
- transfer-style queries read from trace artifacts rather than a duplicate store
- unindexed full-scan trace queries are rejected
- trace materialization borrows from stored bytes rather than rebuilding frames
- crash/retry tests demonstrate publication safety for trace artifacts
- current-state docs describe the implemented trace family accurately

## Deferred Scope

This plan does not require:

- descending trace traversal
- a dedicated `isTopLevel` bitmap index
- blob-store-specific trace metadata attributes
- a second canonical trace representation beside the raw RLP blob
- speculative optimization work before basic correctness and query behavior land
