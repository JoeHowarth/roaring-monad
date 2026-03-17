# Benchmark Methodology And Release Evidence

## Purpose

This document defines the concrete methodology for benchmark evidence and
release-readiness measurements.

It is the implementation companion to
`docs/plans/performance-capacity-and-deployment.md`.

## Goal

Define:

- which benchmark classes exist
- what evidence should accompany review PRs
- how startup and recovery cost should be measured
- what soak evidence is needed
- how results should be recorded and compared

## Benchmark Classes

### 1. Review-PR evidence runs

Small, targeted runs attached to a review PR when that PR introduces or
wires a performance-sensitive feature.

Examples:

- cache-on versus cache-off query comparisons
- materialization improvements
- startup/recovery cost changes

### 2. Baseline performance runs

Repeatable benchmark profiles used to establish a known-good operating
baseline for the crate.

Examples:

- selective indexed query
- broad multi-clause query
- ingest-only path
- mixed ingest/query path

### 3. Startup and recovery runs

Measure:

- startup plan cost
- startup cleanup cost
- marker repair cost
- large-state recovery path cost

### 4. Soak runs

Longer-running evidence for:

- stable latency
- bounded memory growth
- bounded cleanup debt
- absence of obvious degradation drift

## Required Evidence By Readiness Bar

### Upstream-ready

Required:

- manual benchmark evidence on relevant review PRs for performance-
  sensitive slices
- enough targeted runs to show that major features such as caching or
  recovery wiring do not obviously regress the path they affect

Not required:

- full soak
- complete production SLO report

### Production-ready

Required:

- explicit SLO targets
- representative baseline benchmark runs
- startup and recovery cost measurements
- soak evidence with defined pass criteria

## Review-PR Evidence Expectations

When a review PR introduces a performance-sensitive change, it should
include a short evidence block with:

- workload description
- command(s) used
- environment notes
- before metric(s)
- after metric(s)
- interpretation

This should be attached manually if it cannot run in normal CI.

Examples of PRs that should include such evidence:

- cache wiring or cache budget changes
- materialization changes
- query execution changes
- startup or recovery path changes

## Startup And Recovery Measurement

These measurements should be treated as first-class.

Minimum dimensions:

- empty state
- moderate published head
- large published head with cleanup/repair work present

Record:

- elapsed time
- relevant counts scanned or repaired
- whether work was startup-only or deferred

## Soak Expectations

Production-facing soak should define:

- duration
- workload mix
- pass/fail thresholds
- artifacts collected

The plan does not require a fixed duration yet, but it does require an
explicitly stated duration and pass criteria before claiming production
readiness.

## Result Recording

Performance evidence should be captured in two places.

### PR-local summary

Use a concise markdown summary in the review PR for reviewer context.

That summary should compare the result against the most recent accepted
baseline for the same benchmark class and call out any material
regression explicitly.

### Repo evidence log

Use `OPTIMIZATION_LOG.md` for actual profiling and benchmark evidence in
this repo when work here is performance-driven.

Record:

- timestamp
- change summary
- hypothesis
- exact commands
- before/after metrics
- interpretation

## Methodology Rules

- compare like-for-like workloads
- keep environment notes explicit
- avoid mixing behavior changes and performance claims without separate
  measurements
- treat noisy or one-off numbers as exploratory, not release evidence
- prefer a small number of repeatable benchmark classes over many ad hoc
  runs
- keep representative workload definitions in benchmark configs or
  tooling inputs rather than hard-coding them into this methodology doc

## Open Questions

### 1. How much benchmark evidence should every performance-sensitive PR include?

Options:

- A. a single targeted before/after run
- B. a small repeated run set with simple variance awareness
- C. only evidence on large milestone PRs

Recommendation:

- B for materially performance-sensitive PRs
- A is acceptable for small changes if the signal is very clear

Reason:

- single-shot numbers are too easy to overread
- full benchmark suites on every PR are too heavy

### 2. Should soak be required before the first monorepo landing?

Options:

- A. yes
- B. no, only before production readiness

Recommendation:

- B

Reason:

- the immediate problem is reviewable upstream landing, not production
  sign-off

### 3. Should startup and recovery evidence be part of every relevant PR?

Options:

- A. yes, whenever startup/recovery code changes
- B. only for milestone PRs

Recommendation:

- A for changes that directly touch startup, recovery, maintenance, or
  GC behavior

Reason:

- those paths are easy to regress quietly and are not well represented by
  ordinary query/ingest benchmarks
