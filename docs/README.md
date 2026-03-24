# Docs Guide

This directory has three kinds of documentation:

- conceptual design docs for first-pass orientation
- current-state architecture docs for how the crate works today
- plans and historical docs for rollout context and prior decisions

If you are new to the codebase, start with the docs in the order below rather
than reading files alphabetically.

## Recommended Reading Order

### Pass 1: Why this backend exists

1. [reference/queryX](reference/queryX) — the external product/interface this
   backend is powering
2. [queryx-support.md](queryx-support.md) — how `finalized-history-query`
   maps onto that product boundary and what lives here vs. in the RPC layer
3. [design/bitmap-indexed-history-queries.md](design/bitmap-indexed-history-queries.md)
   — the core query/indexing idea
4. [design/immutable-artifact-model.md](design/immutable-artifact-model.md) —
   the storage and publication model that makes the read path work

This pass answers:

- what product this crate is supporting
- why the storage/query substrate looks the way it does
- which responsibilities are in this crate versus the transport layer

### Pass 2: Current architecture

5. [overview.md](overview.md) — top-level service boundary, crate layout, main
   types, and suggested source-file reading path
6. [storage-model.md](storage-model.md) — authoritative artifact layout and
   lookup flow
7. [query-execution.md](query-execution.md) — query normalization, bitmap
   intersection, materialization, and pagination
8. [ingest-pipeline.md](ingest-pipeline.md) — ingest ordering, per-family
   writes, compaction, and recovery hooks
9. [write-authority.md](write-authority.md) — lease, ownership, and publication
   rules

This pass answers:

- what the major runtime layers are
- how data gets written and made visible
- how queries execute against finalized immutable artifacts

### Pass 3: Operational and implementation details

10. [caching.md](caching.md) — typed artifact-table caches and runtime cache
    ownership
11. [backend-stores.md](backend-stores.md) — store traits and concrete backend
    implementations
12. [config.md](config.md) — the tunable configuration surface

Read these once the basic ingest/query model is already clear.

### Pass 4: Family-specific deep dives

13. [trace-family.md](trace-family.md) — trace-family storage, indexing, and
    query behavior

Logs are the reference family across the main architecture docs, so there is
not a separate top-level logs family doc today.

## Fastest Useful Onboarding Path

If you want the shortest path before opening code, read only these:

1. [reference/queryX](reference/queryX)
2. [queryx-support.md](queryx-support.md)
3. [design/bitmap-indexed-history-queries.md](design/bitmap-indexed-history-queries.md)
4. [design/immutable-artifact-model.md](design/immutable-artifact-model.md)
5. [overview.md](overview.md)
6. [storage-model.md](storage-model.md)
7. [query-execution.md](query-execution.md)
8. [ingest-pipeline.md](ingest-pipeline.md)

That is enough context to understand most of
`crates/finalized-history-query/src/`.

## How To Use The Rest

- `docs/plans/` — active and completed implementation plans; useful when
  working on a roadmap item or understanding why a refactor is staged the way
  it is
- `docs/historical/` — older architecture notes and superseded planning
  material; not part of the normal onboarding path
- `docs/issues/` — focused writeups for specific known problems
- `docs/reference/` — external or product-shaping reference material

## Source Walk After Docs

After the reading path above, a good first code walk is:

1. `crates/finalized-history-query/src/lib.rs`
2. `crates/finalized-history-query/src/api.rs`
3. `crates/finalized-history-query/src/family.rs`
4. `crates/finalized-history-query/src/runtime.rs`
5. `crates/finalized-history-query/src/tables.rs`
6. `crates/finalized-history-query/src/query/`
7. `crates/finalized-history-query/src/ingest/`
8. `crates/finalized-history-query/src/logs/`
9. `crates/finalized-history-query/src/traces/`
10. `crates/finalized-history-query/tests/`

`overview.md` also contains a more detailed file-by-file reading order inside
the main crate.
