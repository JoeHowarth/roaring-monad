# Finalized Log Index Architecture Roadmap

This guide is for a new developer who wants to understand the core indexing and query path in `crates/finalized-log-index`.

## Scope

Focus first on the finalized-path happy case:

1. A finalized block arrives.
2. Its logs are assigned global log IDs and serialized into blob storage.
3. Secondary index streams are updated so queries can narrow candidates quickly.
4. Queries translate filter clauses into stream scans, candidate intersections, and exact log fetches.

Read GC, recovery, and distributed stores only after that model is clear.

## The System in One Picture

The crate is a two-store log index:

- Meta store:
  stores small structured records such as head state, block metadata, stream manifests, stream tails, block-hash lookup, and log locator pages.
- Blob store:
  stores large payloads such as packed logs and sealed bitmap chunks.

The main object is `FinalizedIndexService` in `crates/finalized-log-index/src/api.rs`. It is a thin orchestrator around:

- `IngestEngine`: builds and persists index state for new finalized blocks.
- `QueryEngine`: plans and executes queries over that state.

## Core Concepts to Learn First

### 0. Key terms

- `global_log_id`:
  the monotonically increasing 64-bit log sequence number assigned during ingest
- `shard`:
  the high bits of a block number or global log ID; used to partition stream storage
- `local value`:
  the low bits stored inside a shard-local bitmap for either log IDs or block numbers
- `stream_id`:
  the physical identifier for one secondary-index stream, built as:
  `"<index_kind>/<hex-encoded value>/<shard>"`
- `index_kind`:
  one of the logical index families such as `addr`, `topic0_log`, `topic1`, `topic2`, or `topic3`
- `manifest`:
  the per-`stream_id` metadata record listing sealed chunk blobs for that stream
- `tail`:
  the mutable per-`stream_id` bitmap of recently appended local values that have not yet been sealed into a chunk

### 1. Global log IDs and sharding

Logs are given a monotonically increasing global `log_id`. Many index structures store only the low 24 bits as a local value and derive the shard from the high 40 bits. The helper `stream_id(...)` in `crates/finalized-log-index/src/domain/keys.rs` encodes this.

Why it matters:

- ingest writes local bitmap entries into shard-specific streams
- query reconstructs global candidates by combining shard and local value

### 2. Two kinds of query acceleration

The query path uses log-level streams:

- `addr`
- `topic0_log`
- `topic1`
- `topic2`
- `topic3`

`topic0` is handled the same way as the other topic filters: it is an exact log-level stream, not a separate block-level special case.

### 3. Tail vs sealed chunks

Each stream has:

- a mutable `tail` bitmap in meta storage
- a `manifest` that points to immutable sealed chunks in blob storage

Ingest appends new local IDs into the tail. Once the tail is big enough, old enough, or serialized size is large enough, it is sealed into a chunk blob and referenced from the manifest.

### 4. Log retrieval is separate from filtering

Filtering produces candidate log IDs from bitmap streams. Actual logs are loaded later through:

1. `log_locator_page`
2. blob key + byte span
3. packed log blob

That separation is the center of the design:

- streams answer “which log IDs might match?”
- locator pages answer “where is log ID N stored?”
- packed blobs answer “what is the full log?”

## Suggested Study Roadmap

### Stage 1. Learn the public surface

Read `crates/finalized-log-index/src/api.rs` and `crates/finalized-log-index/src/config.rs`.

Goals:

- understand the public trait
- see where ingest vs query are delegated
- notice runtime degradation/throttling behavior
- identify tuning knobs that affect seal policy, planner breadth, and ingest consistency

Questions to answer:

- What API methods define the service?
- What failures merely return errors, and what failures degrade the service?
- Which config values affect correctness vs performance?

### Stage 2. Learn the domain model and keyspace

Read `crates/finalized-log-index/src/domain/types.rs`, `crates/finalized-log-index/src/domain/filter.rs`, and `crates/finalized-log-index/src/domain/keys.rs`.

Goals:

- understand `Block`, `Log`, `BlockMeta`, `MetaState`, and `LogFilter`
- see how clauses represent `Any`, `One`, and `Or`
- map logical structures to physical keys

Questions to answer:

- Which records live in meta vs blob storage?
- Which structures are block-scoped, log-scoped, and stream-scoped?
- How does `block_hash` mode differ from range mode?

### Stage 3. Understand physical encodings

Read `crates/finalized-log-index/src/codec/log.rs`, `crates/finalized-log-index/src/codec/manifest.rs`, and `crates/finalized-log-index/src/codec/chunk.rs`.

Goals:

- see exactly what each persisted record contains
- connect manifests/tails/chunks/locator pages to the ingest and query flows

Key observation:

The architecture is easier to follow once you know these record types:

- `meta/state`
- `block_meta/*`
- `block_hash_to_num/*`
- `log_locator_pages/*`
- `manifests/*`
- `tails/*`
- `chunks/*`
- `log_packs/*`

### Stage 4. Walk ingest end to end

Read `crates/finalized-log-index/src/ingest/engine.rs`.

Track this sequence in order:

1. load `MetaState`
2. validate next block number and parent hash
3. encode all block logs into one packed blob
4. write locator pages for each log span
5. persist `BlockMeta` and block-hash lookup
6. derive per-stream appends
7. append into stream tails and maybe seal chunks
8. persist the new `MetaState`

This file contains most of the real architecture.

Focus especially on these regions:

- `ingest_finalized_block`
- `collect_stream_appends`
- `apply_stream_appends`
- `should_seal`
- `store_state` / `store_manifest`

Questions to answer:

- What must be written before the finalized head can advance?
- Why does stream data use local IDs while block metadata uses global ranges?
- How does `StrictCas` differ from `SingleWriterFast`?
- Why is `topic0` now just another log-level stream instead of a special hybrid path?

### Stage 5. Walk query end to end

Read `crates/finalized-log-index/src/query/engine.rs`, then `planner.rs`, then `executor.rs`.

Mental flow:

1. normalize the filter range
2. resolve block-hash mode if requested
3. compute the log-ID window from block metadata
4. estimate clause selectivity
5. either build an ordered clause plan or fall back to block scan
6. use manifest metadata to find the relevant chunk slice for each shard-local range
7. load candidate stream entries within that shard-local range
8. intersect candidate sets
9. fetch full logs by locator
10. run exact predicate checks

Questions to answer:

- How is “too broad” decided?
- Why does the planner estimate overlap from manifests and tails instead of reading every chunk?
- How do binary search and full-shard shortcuts reduce per-query manifest work?
- Why does the executor still do `exact_match` after using indexes?
- Why does `topic0` now fit the same mental model as `topic1`, `topic2`, and `topic3`?

### Stage 6. Learn the storage abstraction

Read `crates/finalized-log-index/src/store/traits.rs`, then `store/meta.rs`, `store/blob.rs`, and optionally `store/fs.rs`.

Goals:

- understand the minimal contracts assumed by ingest/query
- understand fencing and versioned CAS behavior
- see that the in-memory stores are the easiest executable model of the backend

### Stage 7. Use tests to validate the mental model

Read:

- `crates/finalized-log-index/tests/finalized_index.rs`
- `crates/finalized-log-index/tests/differential_and_gc.rs`

These are the quickest way to verify you now understand the design:

- happy-path ingest and query
- block-hash mode
- broad query policy
- differential correctness against a naive scan
- how GC and startup recovery attach to the same persisted structures

## Mental Model Summary

If you only remember five facts, remember these:

1. `MetaState` is the serialized authority for finalized head and next global log ID.
2. Full logs live in packed blobs; queries usually reason about bitmap streams first.
3. Each secondary index stream is split into mutable tail plus immutable chunk history.
4. Query planning is mainly about choosing the cheapest clause order or falling back to block scan.
5. Topic0 now behaves like the other topic clauses: it is an exact log-level stream that participates in candidate intersection.

## What To Read After the Core

After the above, read these only as follow-ons:

- `crates/finalized-log-index/src/recovery/startup.rs`
  to see how persisted state is interpreted at startup
- `crates/finalized-log-index/src/gc/worker.rs`
  to see how orphan chunks, stale tails, and guardrails are managed
- distributed store backends in `store/scylla.rs` and `store/minio.rs`
  if you need production backend behavior
