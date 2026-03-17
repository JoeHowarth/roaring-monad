# Core Productization And Upstreaming

## Summary

This plan defines the work required to make
`crates/finalized-history-query` straightforward to review and stage
into an existing codebase before finishing the later production-only
hardening work.

The crate already has a coherent architecture:

- a transport-free method boundary
- a shared finalized-history substrate
- a logs-family adapter

That architecture is documented in [../overview.md](../overview.md),
[../storage-model.md](../storage-model.md),
[../query-execution.md](../query-execution.md),
[../ingest-pipeline.md](../ingest-pipeline.md),
[../write-authority.md](../write-authority.md), and
[../backend-stores.md](../backend-stores.md).

The main remaining upstreaming problem is not architecture discovery. It
is review surface management.

Several implementation and test files are still too large to land as a
single unit:

- `src/logs/query.rs` — about 1,182 lines
- `src/logs/ingest.rs` — about 892 lines
- `src/logs/materialize.rs` — about 705 lines
- `src/api/service.rs` — about 562 lines
- `src/ingest/authority/lease.rs` — about 650 lines
- `tests/finalized_index.rs` — about 1,527 lines
- `tests/crash_injection_matrix.rs` — about 513 lines

This plan is about turning that monolithic review surface into a staged
upstreaming path while keeping the current semantics intact.

## Goal

Make `crates/finalized-history-query` easy to review, easy to stage into
an existing codebase, and explicit about which behaviors are complete
versus still production-hardening work.

## Why This Matters

The current crate already has the right high-level layering, but several
large files and broad integration tests still force reviewers to absorb
too much at once.

That creates three avoidable costs:

1. architectural review becomes tangled with mechanical refactoring
2. unfinished production-only concerns distract from the core merge path
3. reviewers cannot easily isolate regressions to one subsystem

This workstream exists to reduce those costs before asking upstream
reviewers to evaluate distributed backends, GC, operations, or
performance evidence.

## Review-Stack Constraints

The upstream target is a monorepo landing problem, not a deep
adaptation-to-existing-abstractions problem.

The constraints for this workstream are:

- the full crate surface is intended to land over the review stack, not
  a reduced "core only" subset
- `finalized-history-query` is the main focus, but supporting crates such
  as `benchmarking` and `log-workload-gen` should land as early as makes
  sense
- review-motivated internal restructures should happen in this repo
  first, before creating the monorepo review commits
- the review commits should diverge from this repo's eventual end state
  as little as possible
- purely mechanical preparatory PRs are acceptable when they materially
  reduce reviewer load
- early review PRs may land infrastructure that is not yet fully wired
  into the final service path, so long as each PR compiles and has
  meaningful local tests

## Current State

The crate already has the architectural seams needed for upstreaming.

The documented layering is:

### Method boundary

- public query and write interfaces
- service constructors and transport-free API types

### Shared finalized-history substrate

- cache and runtime state
- range and finalized-head resolution
- write authority and fencing
- stream/page support

### Logs family adapter

- filter semantics
- query execution
- materialization
- artifact publication and compaction

The upstreaming problem is that these seams exist conceptually and in
the docs, but not yet strongly enough in the implementation layout and
test layout.

## Scope

- split large source files into smaller subsystem-oriented modules
- split large integration tests into focused themed suites
- reduce stale or conflicting active docs and issue notes
- make the staged merge path into an existing codebase explicit
- isolate optional or unfinished production-only pieces so they do not
  block upstream review of the core architecture
- decide what should land in the initial upstream stack versus later
  follow-up stacks

## Out Of Scope

- finishing distributed backend semantics
- implementing GC or maintenance behavior
- adding full operational telemetry or runbooks
- proving production SLOs
- redesigning the architecture unless a review-surface refactor exposes a
  real design flaw

## Guiding Principles

### Preserve semantics while shrinking review surface

This workstream is primarily about productization, not behavior change.
When in doubt, prefer mechanical reshaping that makes existing behavior
easier to review.

### Split by subsystem responsibility, not by arbitrary file size

The target state should match the architecture docs:

- service boundary code together
- write-authority code together
- query planning/execution code together
- materialization code together
- ingest/publication code together

### Keep the upstream merge path transport-free

The RPC or host-service integration should remain outside this crate's
core review path. The crate should continue to present a transport-free
boundary.

### Isolate production-only follow-up work

Known unfinished production topics should be documented and isolated
rather than intermixed with core architectural PRs.

## Work Packages

## 1. Normalize The Active Docs Story

### Intent

Make the active docs tell one clear, current-state story so reviewers do
not have to reconcile current code with superseded narratives.

### Tasks

- identify plan docs and issue notes that are still active versus merely
  historical context
- remove, rewrite, or clearly supersede docs that conflict with the
  current `publication_state` and `WriteAuthority` model
- ensure the topic docs and active plan docs agree on:
  - publication and visibility
  - lease and fencing semantics
  - ingest ordering
  - query execution shape
  - backend capability expectations

### Deliverables

- one coherent active-doc set
- stale docs either removed from the active path or clearly marked
- no contradictory "current behavior" narratives in active docs

## 2. Split Large Implementation Surfaces

### Intent

Turn the oversized modules into smaller reviewable units that line up
with documented subsystem boundaries.

### Targeted Surfaces

- `src/logs/query.rs`
- `src/logs/ingest.rs`
- `src/logs/materialize.rs`
- `src/api/service.rs`
- `src/ingest/authority/lease.rs`

### Desired End State

Likely decomposition shape:

- `logs/query/*`
  - public engine entry
  - clause/spec preparation
  - stream bitmap loading
  - shard execution
  - benchmark-only helpers if they remain public
- `logs/ingest/*`
  - immutable artifact writes
  - directory compaction
  - stream fragment/page compaction
  - shared helpers and key parsing
- `logs/materialize/*`
  - directory resolution
  - block header and payload reads
  - block-ref hydration
  - request-local caches and helpers
- `api/service/*`
  - constructors and roles
  - startup/recovery path
  - query path
  - ingest path
  - maintenance and GC path
- `ingest/authority/lease/*` if needed
  - acquisition helpers
  - renewal/publish helpers
  - tests organized by behavior family

The exact file tree can change, but the end result should map cleanly to
the architecture docs.

### Deliverables

- smaller modules with clear ownership and less context switching per
  file
- preserved behavior and tests
- easier diff boundaries for later upstream PRs

## 3. Split The Large Test Surfaces

### Intent

Move from "one giant integration file proves everything" to a themed
test layout where each suite proves a small set of invariants.

### Targeted Surfaces

- `tests/finalized_index.rs`
- `tests/crash_injection_matrix.rs`

### Desired End State

Likely integration test groupings:

- publication and authority
- startup and recovery
- query semantics and pagination
- ingest and compaction boundaries
- cache behavior
- failure injection and retry
- single-writer and reader-only roles

The point is not to maximize file count. The point is to let reviewers
connect each suite to one subsystem or invariant family.

### Deliverables

- smaller themed integration suites
- shared test helpers extracted where repetition currently obscures the
  intent of individual tests
- more direct mapping from docs and invariants to test files

## 4. Define The Initial Upstream Surface

### Intent

Decide what is part of the first upstream stack and what remains a
follow-up.

### Landed Decisions

- the review stack should land the full crate surface over time,
  including distributed backends
- `log-workload-gen` and `benchmarking` should land early when they help
  justify performance-sensitive review slices
- the stack is primarily about reviewability, not about adapting to an
  unknown upstream abstraction layer
- no transitional compatibility layer should be introduced unless it is
  strictly necessary to keep an intermediate PR correct and buildable

### Deliverables

- explicit scope statement for the first upstream merge
- explicit follow-up list for intentionally deferred work
- no ambiguity about what reviewers are expected to sign off on in the
  first wave

## 5. Produce The Upstream PR Stack

### Intent

Translate the cleaned-up implementation and docs into a staged merge
plan for a larger codebase.

### Expected PR Groups

The exact stack may change, but it should roughly track these slices:

1. public types and method boundary
2. store and publication abstractions
3. distributed-capable store backends and local test doubles
4. storage keys, codecs, and state loading
5. materialization substrate
6. indexed query execution
7. write authority
8. ingest publication and compaction
9. service startup and orchestration
10. benchmarking/workload support where it strengthens nearby review
    slices

### Deliverables

- a named upstream PR sequence
- clear dependency ordering between PRs
- review notes that tell reviewers what to focus on in each slice
- a stack shape where each PR is approximately one commit and stays close
  to this repo's intended end state

## Milestones

## Milestone A: Docs And Scope Cleanup

Complete when:

- active docs no longer conflict
- the first upstream scope is explicit
- the PR-stack target shape is agreed

## Milestone B: Implementation Surface Split

Complete when:

- oversized source files are decomposed
- the new module layout matches subsystem ownership

## Milestone C: Test Surface Split

Complete when:

- themed suites replace the current broad integration surfaces
- helpers are extracted where they clarify, rather than hide, intent

## Milestone D: Ready For Upstream Execution

Complete when:

- docs, code layout, and test layout all support the staged upstream PR
  sequence
- unfinished production-only items are clearly isolated

## Exit Criteria

- active docs tell one consistent story about publication, ownership,
  query execution, storage layout, and service behavior
- the crate can be reviewed as a sequence of smaller PRs instead of one
  monolithic drop
- large test and implementation surfaces are broken into smaller,
  clearly owned units
- unfinished production work is documented as such and isolated from the
  core upstreaming path
- the initial upstream scope and follow-up scope are both explicit

## Dependencies

- no hard technical dependency on later workstreams
- should happen before all other workstreams, because it reduces review
  cost everywhere else
- should inform the final shape of
  `distributed-backend-completion`,
  `recovery-gc-and-maintenance`, and
  `correctness-verification-matrix`

## Risks

### Over-refactoring before upstreaming

If this workstream becomes an architecture rewrite, it will delay the
real merge path instead of enabling it.

### Under-refactoring before upstreaming

If the big files and tests remain mostly intact, the PR stack will still
be mechanically split but mentally monolithic.

### Scope confusion

If the first upstream surface is not defined clearly, production-only
follow-up work will continue to leak into every review.

## Review Checklist

When this plan is implemented, reviewers should be able to answer "yes"
to all of the following:

- can I explain the crate from the docs without reconciling stale
  narratives
- can I review one subsystem without holding the entire crate in my head
- can I tell which tests prove which invariant family
- can I see what lands in the first upstream wave versus later waves

## Dependencies On Other Plans

- [distributed-backend-completion.md](distributed-backend-completion.md)
  assumes this plan has already reduced the review surface around backend
  adapters
- [recovery-gc-and-maintenance.md](recovery-gc-and-maintenance.md)
  benefits from the service and ingest surfaces being split first
- [correctness-verification-matrix.md](correctness-verification-matrix.md)
  depends on clearer test-family boundaries established here

## Follow-Up Questions

- which pieces should be considered core upstream surface versus optional
  follow-up modules
- whether any internal subsystem should move into a separate crate before
  upstreaming
- whether the upstream target codebase already provides abstractions that
  should replace some local store or service plumbing
