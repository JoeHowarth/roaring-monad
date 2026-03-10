# Finalized History Query Metadata Caching Architecture

## Summary

This document is a stub for the metadata-caching architecture that should sit alongside the shard-streaming query design.

It is intentionally not detailed yet.

Its purpose is to make the design boundary explicit:

- shard-streaming query execution is one problem
- cross-request metadata caching is a separate problem

The executor plan depends on good metadata reuse, but this document will eventually define that reuse as its own architecture.

## Why This Needs Its Own Document

Shard-streaming execution answers:

- what unit of query work the system processes
- when a query can stop
- how pagination interacts with traversal order

Metadata caching answers:

- what metadata the system keeps beyond one request
- where caches live
- how cache entries are keyed
- how entries are refreshed or invalidated
- how memory is bounded and observed

Those concerns interact, but they should not be conflated in one plan.

## Scope

The eventual caching architecture should cover reusable metadata such as:

- stream manifests
- stream tails
- decoded chunk bitmaps
- block log headers
- log directory buckets
- any lightweight per-stream planning metadata that becomes useful later

## Key Questions To Cover

### 1. Cache ownership

The design should decide where each cache belongs, for example:

- inside the query engine
- at the service layer
- inside store adapters
- in a shared process-wide cache layer

### 2. Cache keying

The design should define stable keys, for example:

- `stream_id`
- `(stream_id, chunk_seq)`
- `block_num`
- `bucket_start`

### 3. Coherence and refresh

The design should explain how each object behaves under writes:

- immutable chunk blobs are easy to cache
- manifests and tails need refresh rules
- finalized block headers and directory buckets are easier because they are append-only after finalization

### 4. Eviction and admission

The design should cover:

- byte-based capacity limits
- whether admission is frequency-aware
- whether hot large objects should be treated differently from hot small objects

### 5. Interaction with ingest

The design should say whether ingest:

- populates caches eagerly
- invalidates or refreshes entries on write
- relies on version checks or read-through refresh

### 6. Interaction with shard-streaming queries

The design should explain what reuse the executor expects across:

- adjacent shards in one query
- repeated pages of the same query
- unrelated queries hitting the same hot streams

### 7. Observability

The design should define metrics such as:

- hit rate
- miss rate
- bytes cached
- decode avoidance
- refresh rate
- stale-read avoidance behavior

## Non-Goals For The Stub

This stub does not yet choose:

- a concrete cache implementation
- exact invalidation rules
- exact eviction policy
- exact capacity defaults

## Relationship To Other Plans

This document is intended to complement:

- `docs/plans/finalized-history-query-shard-streaming-query-architecture.md`

The streaming-query plan should stay focused on executor shape and pagination semantics.

This caching document should eventually define the reusable metadata layer that allows that executor to run efficiently across requests.
