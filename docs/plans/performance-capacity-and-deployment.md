# Performance, Capacity, And Deployment

## Goal

Define and prove the performance and deployment bar required for running
the crate as a real service on representative infrastructure.

## Why This Matters

The repository already contains benchmarking and workload-generation
tools, but the crate still lacks the production framing that turns raw
benchmarks into release evidence:

- explicit SLOs
- representative workload definitions
- soak criteria
- capacity growth expectations
- deployment and rollback guidance

Without that framing, performance work can produce numbers without
proving readiness.

## Scope

- define ingest, query, startup, and recovery SLO targets
- define representative workloads and benchmark profiles
- define soak-test duration, pass conditions, and regression thresholds
- build a storage and cleanup capacity model
- document deployment prerequisites such as backend topology,
  configuration, secrets, TLS, and rollback expectations
- document deployment-mode guidance for lease-backed versus
  single-writer operation
- decide whether optional cache and storage optimizations remain
  necessary after baseline tuning evidence is available

## Out Of Scope

- implementing backend semantics
- building the core metrics surface
- basic code-shape productization work

## Exit Criteria

- the crate has explicit SLOs with benchmark and soak evidence attached
- performance numbers are tied to representative workloads instead of
  ad hoc local runs
- storage growth and cleanup lag have an explicit operating model
- deployment mode selection and replacement expectations are documented
- deployment requirements and rollback expectations are documented

## Dependencies

- depends on all earlier workstreams, especially
  `observability-and-operations`
- should be the last production-readiness workstream to close

## Follow-Up Questions

- which hardware and backend topologies count as representative for the
  first production target
- which benchmark classes belong in CI versus scheduled performance
  pipelines
- how much deployment configuration should live in this repo versus the
  eventual host/service repo
- whether optional optimizations such as miss deduplication, ingest-time
  cache population, compression, alternate cache backing allocation, or
  payload-only stored log encoding are justified by measured evidence
