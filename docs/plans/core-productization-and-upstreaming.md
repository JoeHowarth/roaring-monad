# Core Productization And Upstreaming

## Goal

Make `crates/finalized-history-query` easy to review, easy to stage into
an existing codebase, and explicit about which behaviors are complete
versus still production-hardening work.

## Why This Matters

The current crate already has the right high-level layering, but several
large files and broad integration tests still force reviewers to absorb
too much at once:

- `src/logs/query.rs`
- `src/logs/ingest.rs`
- `src/logs/materialize.rs`
- `src/api/service.rs`
- `tests/finalized_index.rs`

There is also doc drift around older publication/lease narratives that
will make upstream review harder than necessary if left unresolved.

This workstream is about making the crate mergeable before asking
reviewers to evaluate the remaining production hardening work.

## Scope

- split large source files into smaller subsystem-oriented modules
- split large integration tests into focused themed suites
- reduce stale or conflicting active docs and issue notes
- make the staged merge path into an existing codebase explicit
- isolate optional or unfinished production-only pieces so they do not
  block upstream review of the core architecture

## Out Of Scope

- finishing distributed backend semantics
- implementing GC or maintenance behavior
- adding full operational telemetry or runbooks
- proving production SLOs

## Exit Criteria

- active docs tell one consistent story about publication, ownership,
  query execution, storage layout, and service behavior
- the crate can be reviewed as a sequence of smaller PRs instead of one
  monolithic drop
- large test and implementation surfaces are broken into smaller,
  clearly owned units
- unfinished production work is documented as such and isolated from the
  core upstreaming path

## Dependencies

- no hard technical dependency on later workstreams
- should happen before all other workstreams, because it reduces review
  cost everywhere else

## Follow-Up Questions

- which pieces should be considered core upstream surface versus optional
  follow-up modules
- whether any internal subsystem should move into a separate crate before
  upstreaming
- how much of the benchmarking and distributed-store support should land
  in the initial upstream stack
