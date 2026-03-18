# Distributed Backend Completion

## Summary

The distributed backend pair (Scylla + MinIO) is not yet a complete
implementation of the crate's storage contracts. This plan closes that
gap.

The concrete blocker: `MinioBlobStore::put_blob_if_absent(...)` returns
`Unsupported`. The Scylla metadata path works but lacks strong
contention evidence.

## Work Packages

### 1. Implement MinIO Conditional Create

Implement `put_blob_if_absent(...)` using `If-None-Match: "*"`:

1. `PutObject` with `If-None-Match: "*"` — success → `Created`
2. Backend rejects (object exists) — `GetObject` to read existing bytes
3. Bytes match → `AlreadyExists`
4. Bytes differ → conflict escalated above the blob layer (the
   artifact-vs-summary distinction already lives in the immutable-write
   helper, not in the blob adapter)
5. Reject-then-missing → `Error::Backend` (do not normalize)

The implementation must not silently degrade into unconditional
overwrite or check-then-put. Unconditional overwrites from outside this
system are not part of the supported deployment model.

Replay check uses a full-object read for byte comparison. Acceptable
for the base design if replay is rare. If blob size or replay frequency
makes this expensive, ETag/content-hash comparison can follow.

**Capability validation:** `If-None-Match: "*"` is version-dependent in
MinIO. Validation must establish the MinIO version floor the project
depends on — not just "works on one local docker."

**Tests:**

- first create → `Created`
- same-bytes replay → `AlreadyExists`
- different-bytes replay → conflict at immutable-write helper layer
- two writers race same key/same bytes → one winner, no overwrite
- two writers race same key/different bytes → one winner, conflict
- integration test against the project's MinIO deployment

**Rollout order:**

1. adapter-level conditional create
2. focused adapter tests
3. integration validation against MinIO
4. switch write-path assumption from "unsupported" to "required"
5. expand distributed ingest tests

### 2. Prove Scylla CAS Correctness

Move from "Scylla appears to work" to "Scylla behavior is proven under
the crate's publication and takeover protocol."

- Review publication-state operations against the fencing model
- Test conflict handling: create-if-absent, versioned updates,
  publication CAS, delete-if-version
- Test contention and takeover on real Scylla
- Confirm fence refresh and stale-epoch rejection under retries
- Document which guarantees rely on LWT

### 3. Review Retry And Error Policy

Ensure retries do not hide failures or weaken fail-closed behavior.

- Review retryable error classification for both backends
- Verify bounded retry on write and read paths
- Verify non-retryable correctness failures surface directly
- Decide whether policy should differ between CAS paths, ordinary
  reads, and background enumeration

### 4. Expand Distributed Tests

Make the distributed suite prove the backend contracts rather than
smoke-check them.

Required scenarios:

- ingest → query happy path with immutable writes
- publication conflict and retry
- takeover after lease expiry
- stale writer rejection after fence advance
- object-store outage before/after write
- metadata outage during publish
- restart and recovery after partial distributed writes

Separate always-on tests from opt-in chaos. Define the manual
distributed validation gate for review PRs (normal CI does not run
these).

## Exit Criteria

- `MinioBlobStore::put_blob_if_absent(...)` works with validated MinIO
  version floor
- Scylla CAS, fencing, and takeover behavior proven by distributed
  tests
- no unsupported backend operations on the core write path
- retry policy documented, correctness failures not masked
- distributed tests cover happy-path and adversarial scenarios

## Risks

- **MinIO version compatibility:** `If-None-Match: "*"` may not work on
  older deployments. Must establish a version floor, not just one
  passing environment.
- **Docker-local overfitting:** distributed tests passing on
  docker-compose is not the same as a backend contract proof.
- **Retry masking:** if retries blur contention vs transient failure vs
  hard violations, the state machine makes worse decisions.
