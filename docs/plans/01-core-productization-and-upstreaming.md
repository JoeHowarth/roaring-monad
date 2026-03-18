# Core Productization And Upstreaming

## Summary

The crate's architecture is coherent but several files are too large to
review as a unit. This plan splits them into subsystem-oriented modules
and themed test suites so the crate is reviewable before the later
production hardening work.

Oversized files:

- `src/logs/query.rs` — ~1,182 lines
- `src/logs/ingest.rs` — ~892 lines
- `src/logs/materialize.rs` — ~705 lines
- `src/api/service.rs` — ~562 lines
- `src/ingest/authority/lease.rs` — ~650 lines
- `tests/finalized_index.rs` — ~1,527 lines
- `tests/crash_injection_matrix.rs` — ~513 lines

## Work Packages

### 1. Normalize active docs

Remove or supersede docs that conflict with the current
`publication_state` and `WriteAuthority` model. Ensure the topic docs
agree on publication/visibility, lease/fencing, ingest ordering, query
execution, and backend expectations.

End state: one coherent doc set, no contradictory narratives.

### 2. Split large implementation surfaces

Decompose by subsystem responsibility, not arbitrary file size:

- `logs/query/*` — engine entry, clause/spec prep, stream bitmap
  loading, shard execution
- `logs/ingest/*` — immutable artifact writes, directory compaction,
  stream fragment/page compaction, helpers and key parsing
- `logs/materialize/*` — directory resolution, block header/payload
  reads, block-ref hydration, request-local caches
- `api/service/*` — constructors/roles, startup/recovery, query path,
  ingest path
- `ingest/authority/lease/*` (if needed) — acquisition, renewal/publish,
  tests by behavior family

The exact tree can change but must map to the architecture docs.

### 3. Split large test surfaces

Replace broad integration files with themed suites:

- publication and authority
- startup and recovery
- query semantics and pagination
- ingest and compaction boundaries
- cache behavior
- failure injection and retry
- single-writer and reader-only roles

Extract shared test helpers where repetition obscures intent.

### 4. Define initial upstream surface

Decide what lands in the first review stack vs follow-up. Already
decided: full crate surface lands over time including distributed
backends; `benchmarking` and `log-workload-gen` land early where they
help justify performance-sensitive slices; no transitional compatibility
layers unless strictly needed.

Deliverable: explicit first-wave scope and follow-up list.

## Exit Criteria

- active docs tell one consistent story
- large files split into clearly owned subsystem modules
- themed test suites replace monolithic integration files
- upstream scope and follow-up scope both explicit
- unfinished production work isolated from the core review path

## Risks

- **Over-refactoring:** if this becomes an architecture rewrite it
  delays the merge path instead of enabling it.
- **Under-refactoring:** if big files stay mostly intact, the PR stack
  is mechanically split but mentally monolithic.
- **Scope confusion:** if upstream surface isn't defined clearly,
  production-only work leaks into every review.
