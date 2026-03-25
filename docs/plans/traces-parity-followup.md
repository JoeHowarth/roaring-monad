# Traces Parity Follow-Up

This is a small companion note to the tx/log parity plan.

It captures the trace-side findings that came up while checking whether the same
concerns also exist in `crates/finalized-history-query/src/traces/`.

## Actionable

### 1. Remove the zero-block-hash fallback in trace materialization

Problem:

- Trace materialization currently substitutes `[0; 32]` when the block record is
  missing.
- That can fabricate a plausible-looking `TraceRef` with the wrong block hash
  instead of failing loudly.

Work:

- Change trace materialization to require the real block record, matching the
  stricter tx-side behavior.
- Add a test that proves missing block metadata is surfaced as an error rather
  than silently producing a zero hash.

Exit criteria:

- Trace loads fail if the block hash cannot be recovered from authoritative
  metadata.

### 2. Make owned-trace and hydrated-trace filter semantics agree

Problem:

- `TraceFilter::matches_trace` derives a selector from any input with at least 4
  bytes.
- Zero-copy trace matching only exposes selectors for call-like trace types.
- That means owned trace matching and query-time trace matching can disagree.

Work:

- Pick one selector policy and use it in both paths.
- Preferred direction: make owned-trace matching follow the same call-type gate
  as `CallFrameView`/`TraceRef`.
- Add paired tests that exercise the same non-call frame through both paths.

Exit criteria:

- A filter either matches or rejects the same trace consistently, regardless of
  whether it is evaluated against an owned `Trace` or a hydrated `TraceRef`.

## Not Priority Work

### 1. Table naming

- Trace table naming is already family-prefixed and does not have the log-side
  generic-name drift.

### 2. Materializer style parity

- Traces already matches the tx-side materializer style and does not carry the
  log-side `load_by_id` override difference.

### 3. Test coverage parity

- Traces already has local tests in ingest, filter, materialize, codec, view,
  and the trace iterator, so it does not have the same obvious coverage gap as
  txs.

### 4. Header-ref cleanup

- I did not find a trace analogue to the stale `BlockLogHeaderRef` issue.
