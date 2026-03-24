# Upstream Review Stack

## Summary

This plan describes how to break `finalized-history-query` into a small stack
of reviewable upstreaming commits while keeping churn low.

The guiding constraint is to upstream the crate largely as it exists today.
Each commit must compile on its own, but intermediate commits do not need to
provide end-to-end ingest/query behavior.

The chosen strategy is a horizontal split by architectural layer rather than a
vertical split by product slice. That matches the current crate layout more
closely and avoids temporary review-only reshaping.

## Target Stack

Aim for five commits.

### 1. Crate boundary and shared core vocabulary

Land the crate shell, public API surface, and shared types:

- workspace wiring
- `crates/finalized-history-query/Cargo.toml`
- `src/lib.rs`
- `src/api.rs`
- `src/config.rs`
- `src/error.rs`
- `src/family.rs`
- `src/core/*`

This commit should establish the crate boundary, request/result types, block
envelope, ID types, range types, and pagination vocabulary.

### 2. Storage substrate and runtime

Land the backend and table substrate:

- `src/store/*`
- `src/kernel/*`
- `src/runtime.rs`
- `src/tables.rs`
- `src/streams.rs`

This commit should establish the storage contracts, typed artifact tables,
cache-backed reads, and shared runtime wiring.

### 3. Shared query, publication, and status

Land the read-path substrate that is shared across families:

- `src/query/*`
- `src/core/directory.rs`
- `src/core/directory_resolver.rs`
- `src/store/publication.rs`
- `src/status.rs`

This commit should make the generic read path reviewable before ingest and
family-specific logic arrive.

### 4. Shared ingest, authority, and recovery

Land the write-path substrate:

- `src/ingest/*`

This commit should establish write authority, lease/session handling, recovery,
compaction helpers, and the shared ingest coordinator.

### 5. Family adapters, tests, and support crates

Land the concrete families and supporting surfaces:

- `src/logs/*`
- `src/traces/*`
- `src/txs.rs`
- `crates/finalized-history-query/tests/*`
- `crates/log-workload-gen/*`
- `crates/benchmarking/*`

This commit should keep family-specific logic together on top of an already
reviewed substrate.

## Commit-Shaping Rules

- Keep docs with the layer they explain.
- Prefer moving files in coherent groups over splitting files mechanically.
- Avoid temporary API reshaping just to manufacture nicer intermediate commits.
- If a file spans layers, place it with the layer that owns most of its
  concepts.
- Intermediate commits must compile, but they may contain stubs or partial
  wiring where a later commit finishes the feature.

## Why This Split

This stack minimizes churn because it follows the crate's current architecture:

1. public/core vocabulary
2. storage/runtime substrate
3. shared read path
4. shared write path
5. family adapters

That should produce reviewable commits without introducing much code motion
whose only purpose is to support the review stack.
