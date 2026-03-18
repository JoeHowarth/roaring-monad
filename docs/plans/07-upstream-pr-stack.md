# Upstream PR Stack

## Goal

Translate the finished crate into a staged review stack for monorepo
landing.

This plan runs after all other workstreams are complete. The crate
should be in its intended end state before PR cutting begins.

## Principles

- each review PR should be approximately one commit
- the reviewed stack should preserve this repo's end state as closely as
  possible
- temporary compatibility layers or transitional abstractions should be
  avoided unless strictly necessary to keep an intermediate PR compiling
  and reviewable
- design-review docs are reviewed out-of-band before PR cutting
- current-state docs should be updated in the same implementation PR
  whenever that PR changes architecture, terminology, storage layout, or
  user-visible behavior
- purely mechanical preparatory PRs are acceptable when they materially
  reduce reviewer load
- early review PRs may land infrastructure that is not yet fully wired
  into the final service path, so long as each PR compiles and has
  meaningful local tests

## Expected PR Groups

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

## Deliverables

- a named upstream PR sequence
- clear dependency ordering between PRs
- review notes that tell reviewers what to focus on in each slice

## Dependencies

- depends on all earlier workstreams being complete
- the crate must be in its intended end state before this work begins
