# Performance, Capacity, And Deployment

## Summary

The repo has benchmarking and workload-generation tools but lacks the
production framing that turns raw numbers into release evidence: SLOs,
representative workloads, soak criteria, startup cost expectations, and
deployment guidance.

## Benchmark Classes

### Review-PR evidence

Small targeted runs attached to performance-sensitive PRs. Examples:
cache-on vs cache-off, materialization changes, startup/recovery cost
changes.

Each evidence block should include: workload description, commands,
environment notes, before/after metrics, interpretation. Compare against
the most recent accepted baseline and flag regressions.

A small repeated run set is preferred over single-shot numbers (too easy
to overread). Full benchmark suites on every PR are too heavy.

### Baseline performance

Repeatable profiles for a known-good operating baseline: selective
indexed query, broad multi-clause query, ingest-only, mixed
ingest/query.

Workload definitions live in benchmark configs/tooling, not in this doc.

### Startup and recovery

First-class measurements across: empty state, moderate published head,
large published head with repair work. Record elapsed time, counts
scanned/repaired, and whether work was startup-only or deferred.

Include evidence on any PR that touches startup or recovery code.

### Soak

Longer-running evidence for stable latency, bounded memory growth, and
absence of degradation drift. Must define duration, workload mix,
pass/fail thresholds, and collected artifacts before claiming production
readiness.

Not required for upstream landing — only for production readiness.

## Readiness Bars

### Upstream-ready

- manual benchmark evidence on performance-sensitive review PRs
- enough targeted runs to show major features don't obviously regress

### Production-ready

- explicit SLO targets
- representative baseline runs
- startup and recovery cost measurements
- soak evidence with defined pass criteria

## Other Scope

- deployment prerequisites: backend topology, configuration, secrets,
  TLS, rollback expectations
- deployment-mode guidance: lease-backed vs single-writer
- schema/key-layout coordination for distributed backend changes
- decide whether optional optimizations (miss dedup, ingest-time cache
  population, compression, alternate cache backing, payload-only stored
  log encoding) are justified by measured evidence

## Result Recording

- **PR-local:** concise markdown summary in the review PR
- **Repo:** `OPTIMIZATION_LOG.md` with timestamp, hypothesis, commands,
  before/after metrics, interpretation

## Methodology Rules

- compare like-for-like workloads
- keep environment notes explicit
- don't mix behavior changes and performance claims without separate
  measurements
- treat noisy or one-off numbers as exploratory, not release evidence
- prefer a small number of repeatable benchmark classes over many ad hoc
  runs

## Exit Criteria

- SLOs exist with benchmark and soak evidence
- performance numbers tied to representative workloads
- startup and recovery cost documented and measured
- deployment mode, schema coordination, and rollback expectations
  documented
