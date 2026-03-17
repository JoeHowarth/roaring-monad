# Correctness Verification Matrix

## Goal

Build an explicit verification matrix that proves the crate's
correctness properties across unit, integration, crash-injection, and
distributed failure scenarios.

## Why This Matters

The current test base is strong for single-process behavior, but
production confidence requires more than happy-path coverage and a small
number of distributed races.

What matters here is not just adding more tests, but making the intended
proof surface explicit:

- what invariants are non-negotiable
- which scenarios exercise them
- which suites are required before upstreaming versus before production

## Scope

- define the invariant list the crate must uphold
- map each invariant to one or more test categories
- expand distributed tests for takeover, partial publish, retry,
  compaction, cleanup, and outage scenarios
- expand crash-injection coverage where state can be left partially
  written
- identify which suites should run in normal CI versus controlled
  pre-prod pipelines

## Out Of Scope

- backend implementation work itself
- operational telemetry design
- performance benchmarking except where needed to make tests realistic

## Exit Criteria

- the crate has an explicit verification matrix rather than an ad hoc set
  of tests
- each core invariant is backed by at least one test path
- distributed multi-writer and failure scenarios are covered, not only
  single-process flows
- the difference between must-pass upstream suites and must-pass
  production suites is documented

## Dependencies

- depends on `distributed-backend-completion` for full distributed-path
  coverage
- should advance together with `recovery-gc-and-maintenance`
- informs `observability-and-operations` by identifying which failure
  modes must be observable

## Follow-Up Questions

- which scenarios are cheap enough for regular CI and which belong in a
  slower gated pipeline
- whether some correctness cases should be expressed as property tests or
  model-based tests rather than example-based tests
