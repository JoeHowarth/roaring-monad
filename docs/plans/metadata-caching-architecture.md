# Metadata Caching Architecture

## Summary

This document defines the cache layer that sits around the finalized-history storage boundary.

The query API and executor remain cache-transparent. They call typed family readers and materializers, which may use caches internally but do not change query semantics or expose cache-specific behavior.

The initial implementation should focus on immutable metadata and immutable decoded index fragments. Mutable tip metadata should be designed now, but implemented later under stricter refresh rules.

## Goals

- keep the query API and executor unaware of cache mechanics
- support several metadata backends and object-store backends through one cache shape
- support multiple artifact families, not only logs
- eagerly populate newly sealed immutable metadata because queries are expected to be tip-biased
- partition cache policy by object class so small high-value metadata is not evicted by large derived objects
- prevent duplicate backend reads and duplicate decode work on concurrent misses
- define mutable caching as a follow-on design without making the immutable path depend on it

## Non-Goals

- changing query semantics, pagination semantics, or traversal order
- making cache entries part of correctness or commit semantics
- designing a single prefix-routed generic cache that infers behavior from storage paths alone
- implementing mutable manifest/tail caching in the first pass

## Design Principles

### 1. Cache below the query layer

The cache belongs at the typed storage and materialization boundary.

The executor should not know:

- what is cached
- which cache table an object belongs to
- whether a miss was filled eagerly or read-through

The executor should only observe lower latency and fewer backend reads.

### 2. Generic engine, typed family facades

The reusable part of the design is the cache engine, not a fully generic path-router.

Families should get their own typed cache facades, for example:

- `LogsCache`
- `TransactionsCache`
- `TracesCache`

Each facade owns:

- semantic key types
- store reads
- decode logic
- cache table selection
- eager-population behavior after successful writes

This keeps family semantics explicit while reusing one engine for:

- capacity accounting
- eviction
- metrics
- in-flight miss coordination

The engine may own the miss-coordination flow itself, but families still define the typed fetchers that know how to read, decode, and weight each object kind.

### 3. Tables are logical policy boundaries

Each cache table has:

- a logical identity, which determines policy and metrics
- a capacity budget
- caller-provided entry weights

The engine should treat cached values as opaque entries.

Each insert supplies:

- the key
- the value
- the entry weight in bytes

That lets the same engine store:

- serialized byte buffers
- decoded metadata objects
- decoded index fragments

Table identity answers:

- which objects compete for capacity
- which metrics bucket they belong to
- which admission and eager-population rules apply

The engine does not need separate first-class storage modes for bytes versus objects as long as weight accounting is explicit at insertion time.

## Cache Model

### Generic Engine

The cache engine should expose:

- named tables with per-table config
- `get`
- `insert`
- fill APIs that perform internal miss coordination
- eviction and capacity tracking
- per-table metrics

The public family-facing API does not need to expose generic async loader closures.

Instead, each typed family facade should call into cache-owned fill logic using a typed fetcher:

1. check cache
2. if present, return it
3. otherwise ask the cache to fill `(table_id, key)` using the supplied fetcher
4. the elected loader invokes `fetcher.fetch()`, gets the decoded value plus weight, inserts it, and completes the in-flight state
5. waiters resume and read the inserted value

This keeps deduplication and fill ownership in one place while avoiding closure-heavy public APIs.

### In-Flight Miss Coordination

Concurrent miss coordination should be built into the first implementation and used consistently across tables.

The cache does not need a public `get_or_load(...)` closure API to support this. The essential mechanism is:

- one shared in-flight map keyed by `(table_id, cache_key)`
- first miss becomes the loader
- later misses wait on the same in-flight entry
- the loader runs a typed fetcher for that key
- successful load inserts into cache and wakes waiters
- failed load wakes waiters without inserting

This complexity is worth centralizing once rather than reintroducing duplicate fetch and decode behavior at every use site.

The intended fetcher shape is:

- cache-owned miss coordination
- family-owned typed fetchers
- fetchers return the fully decoded value plus its weight

The fetcher should not be a raw `(store, key)` tuple implementation by default. Named typed fetchers or family-local helper structs are preferred because they give a clearer home for:

- decode logic
- weight computation
- backend-specific reads
- object-kind-specific error handling

## Immutable-First Scope

The first implementation should cache immutable objects only.

That includes:

- finalized log directory buckets
- finalized block log headers
- sealed manifest snapshots where applicable
- sealed stream chunks in decoded form where the decode cost is proven material

For immutable entries:

- ingest may eagerly populate after a successful write
- reads may populate on miss
- entries are never invalidated for correctness
- entries leave the cache only through eviction

This gives the system the simplest possible correctness model:

- cache is an optimization only
- immutable entries do not need refresh rules

## Mutable Future Scope

Mutable tip metadata should be described now but implemented later.

The expected mutable objects are:

- manifests
- tails
- any family-specific planning metadata that can change at the ingest tip

The intended model for mutable data is:

- separate tables from immutable metadata
- explicit refresh or write-through behavior after successful ingest writes
- version-aware or epoch-aware reads where needed
- no shared policy pool with immutable finalized metadata

The immutable-first implementation should not block this later extension, but it should not try to solve mutable correctness prematurely.

## Initial Tables

The first draft should assume separate logical tables even if they share the same implementation.

Recommended initial tables for logs:

- `log_directory_buckets`
  - value: decoded `LogDirectoryBucket`
  - notes: small, immutable, very high reuse
- `block_log_headers`
  - value: decoded `BlockLogHeader`
  - notes: small, immutable, very high reuse
- `decoded_stream_chunks`
  - value: decoded roaring chunk
  - notes: immutable but materially larger; independent capacity

Future tables:

- `stream_manifests`
- `stream_tails`
- family-specific equivalents for transactions, native transfers, and traces

Even when two objects use the same insertion model, they should not share a table unless they have meaningfully similar value density and eviction behavior.

## Relationship Between Persisted Metadata and Derived Objects

Persisted metadata records and decoded index fragments may both be cached as opaque weighted entries, but they are not the same class of data.

Persisted metadata examples:

- `LogDirectoryBucket`
- `BlockLogHeader`

Derived object examples:

- decoded roaring chunk bitmaps

The difference is not serialized versus deserialized representation. The difference is:

- persisted metadata is a direct decoded view of a stored record
- decoded chunk bitmaps are derived in-memory execution artifacts produced from stored chunk bytes

That distinction matters for:

- sizing
- admission
- eager population
- observability

## Backend Boundary

The cache engine should remain backend-agnostic.

It should not:

- construct storage paths
- know backend-specific codecs
- own metadata-store or object-store handles directly as part of its long-lived state

Those concerns belong to the family facade and storage adapters.

This is especially important because the project must support several metadata backends:

- memory
- filesystem
- Scylla
- DynamoDB

and several object-store backends:

- memory
- filesystem
- S3

The cache should operate on semantic keys and weighted typed values rather than storage paths.

## Family Integration

Each family should expose typed load helpers over the shared engine.

For logs, examples include:

- `load_log_directory_bucket(bucket_start)`
- `load_block_log_header(block_num)`
- `load_decoded_chunk(stream_id, chunk_seq)`

Those helpers should:

- provide the typed fetchers used by cache-owned fill logic
- read and decode from the appropriate backend on miss
- compute object weights
- optionally eagerly populate after successful ingest writes

This is intentionally less abstract than a shared fully typed substrate layer. The repeated engine behavior is centralized, but family integration remains explicit.

## Eager Population

For immutable sealed metadata, ingest should eagerly populate cache entries after the backend write has succeeded.

That should apply to:

- finalized directory buckets
- finalized block headers
- other immutable finalized family metadata

It should not make cache state part of the success path for ingest. If cache population fails, the write is still committed and future reads may fill the cache on demand.

## Observability

Metrics should be reported per logical table.

Initial metrics:

- hit count
- miss count
- fill count
- in-flight wait count
- bytes stored
- evictions
- eager-population count
- backend load latency

For derived decoded chunks, also record:

- decode count
- decode bytes
- decode avoidance count

## Implementation Order

### Phase 1

- add the generic engine with weighted opaque entries and named tables
- add in-flight miss coordination keyed by `(table_id, cache_key)`
- add logs-family facade methods for immutable metadata
- eagerly populate immutable log directory buckets and block headers on ingest
- populate immutable reads on cache miss

### Phase 2

- add decoded chunk caching if profiles show material decode cost
- add additional family facades

### Phase 3

- implement mutable manifest and tail caching with explicit refresh rules

## Open Questions

- whether decoded chunk caching should be part of the first implementation or only defined as a reserved table
- how conservative object weight hints need to be for roaring structures
- whether some families will need their own immutable finalized metadata tables beyond the shared patterns described here

## Relationship To Other Plans

This document complements:

- `docs/plans/finalized-history-query-shard-streaming-query-architecture.md`

The shard-streaming plan defines executor shape and pagination behavior.

This document defines the reusable cache layer that should sit beneath those query paths and serve multiple families and backend implementations.
