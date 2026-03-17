# Distributed Backend Completion

## Goal

Finish and harden the distributed backend pair so the crate has a real
production-capable remote metadata and blob-store story.

## Why This Matters

The current local and filesystem-backed behavior is much stronger than
the distributed path.

The most concrete known gap is in the blob-store adapter:

- ingest writes immutable artifacts through `put_blob_if_absent(...)`
- `MinioBlobStore` currently reports that operation as unsupported

That means the distributed object-store path is not yet a complete
implementation of the crate's immutable artifact contract.

The Scylla path also needs its correctness evidence tightened beyond
basic CAS race tests.

## Scope

- implement the missing immutable blob-create semantics for the MinIO
  backend
- verify that the Scylla publication and metadata CAS behavior matches
  the crate's fencing and ownership model under contention
- review backend-specific retry, timeout, and error classification
  policy
- make backend limitations and guarantees explicit in docs and tests

## Out Of Scope

- GC and maintenance semantics above the backend contract layer
- dashboards, alerts, and runbooks
- long-duration performance characterization

## Exit Criteria

- the distributed blob-store path fully supports the immutable write
  operations used by ingest and compaction
- the distributed metadata path has an explicit correctness story for
  CAS, fencing, and takeover behavior
- happy-path and adversarial tests cover the backend behaviors the crate
  relies on
- there are no known unsupported backend operations on the core write
  path

## Dependencies

- depends on `core-productization-and-upstreaming` for a stable code and
  docs surface
- unblocks `correctness-verification-matrix`,
  `recovery-gc-and-maintenance`, and later production work

## Follow-Up Questions

- whether object-store "create if absent" should be implemented with a
  strict backend-native conditional path, a higher-level manifest
  protocol, or another immutable-write scheme
- whether backend capabilities should be feature-gated or negotiated more
  explicitly at construction time
