# Distributed Blob Conditional Create

## Purpose

This document defines the concrete implementation design for
`BlobStore::put_blob_if_absent(...)` on the distributed blob path.

It is the implementation-level companion to
`docs/plans/distributed-backend-completion.md`.

## Goal

Implement immutable blob creation for `MinioBlobStore` using native
conditional object creation semantics via `If-None-Match: "*"`.

The design must preserve the crate's current contract:

- first write succeeds
- exact replay is treated as harmless
- conflicting second write is rejected
- concurrent writers cannot silently overwrite one another

## Chosen Design

The chosen path is:

1. issue `PutObject` with `If-None-Match: "*"`
2. interpret success as `CreateOutcome::Created`
3. if the backend reports that the object already exists, read the
   existing object
4. if the existing bytes match the requested bytes, return
   `CreateOutcome::AlreadyExists`
5. if the existing bytes differ, return an application-level conflict

This keeps the existing `BlobStore` trait contract and avoids a larger
storage-model shift to content-addressed or staged blob keys.

## Concrete Semantics

### Success path

- request: `PutObject` with key `K`, body `B`, header
  `If-None-Match: "*"`
- result: object created
- return: `CreateOutcome::Created`

### Benign replay path

- request: same key `K`, same body `B`
- backend rejects create because object exists
- implementation performs `GetObject(K)`
- if returned bytes equal `B`
- return: `CreateOutcome::AlreadyExists`

### Conflict path

- request: same key `K`, different body `B2`
- backend rejects create because object exists
- implementation performs `GetObject(K)`
- if returned bytes differ from requested bytes
- return: backend-independent immutable-write conflict at the call site

### Missing-after-exists path

- backend rejects create as if object exists
- follow-up read returns missing
- treat as backend inconsistency and return `Error::Backend`

This should not be normalized into `AlreadyExists`.

## Request And Error Mapping

The implementation should classify outcomes into four buckets.

### Created

- object create succeeds

Return:

- `Ok(CreateOutcome::Created)`

### Already exists, equal bytes

- conditional create fails because object exists
- follow-up read returns identical bytes

Return:

- `Ok(CreateOutcome::AlreadyExists)`

### Already exists, different bytes

- conditional create fails because object exists
- follow-up read returns different bytes

Return:

- `Ok(CreateOutcome::AlreadyExists)` from the blob layer is not enough
- the higher immutable-write helper must convert this into
  `Error::ArtifactConflict` or `Error::SummaryConflict` depending on the
  caller

### Backend or capability failure

- unsupported conditional header behavior
- missing-after-exists inconsistency
- transport/read/write failure

Return:

- `Err(Error::Backend(...))` or `Err(Error::Unsupported(...))` if the
  capability truly is absent

The implementation should avoid silently degrading into unconditional
overwrite or best-effort check-then-put semantics.

## Capability Validation

This design depends on the MinIO deployment honoring `If-None-Match:
"*"` semantics on object create.

Validation must establish:

- a passing MinIO version/capability floor for the project's deployment
  target
- that concurrent conditional creates yield a single winner
- that existing-object detection happens before any overwrite
- that the observed failure shape is stable enough to map cleanly in code

The project should treat this as a backend capability requirement, not
as a local Docker accident.

## Test Strategy

The minimum test set should include:

### Local adapter tests

- first create returns `Created`
- second same-bytes create returns `AlreadyExists`
- second different-bytes create produces conflict at the immutable-write
  helper layer

### Concurrency tests

- two writers race on the same key and same bytes
- two writers race on the same key and different bytes

Expected:

- exactly one actual create
- no silent overwrite

### Capability validation tests

- explicit integration test against the MinIO deployment used by the
  project
- record the validated version/capability floor in docs or release
  evidence

## Rollout Notes

Implementation should happen in this order:

1. add adapter-level conditional create implementation
2. add focused adapter tests
3. add integration validation against MinIO
4. switch distributed write-path assumptions from "unsupported" to
   "required"
5. expand higher-level distributed ingest tests

## Non-Goals

- introducing content-addressed blob keys
- introducing staged blob commit protocols
- changing semantic object keys in the current storage model
- weakening immutable-write guarantees for convenience

## Open Questions

### 1. Where should conflicting existing bytes be turned into a domain conflict?

Options:

- A. inside `MinioBlobStore::put_blob_if_absent(...)`
- B. in the existing immutable-write helper above the blob layer

Recommendation:

- B

Reason:

- the blob layer should stay generic and report create-versus-existing
  behavior
- the artifact-versus-summary distinction already exists above the blob
  layer

### 2. Should capability mismatch return `Unsupported` or `Backend`?

Options:

- A. `Unsupported` if the behavior is definitively absent
- B. `Backend` for any failed validation or ambiguous behavior

Recommendation:

- use `Unsupported` only for explicit construction-time or validation-time
  capability rejection
- use `Backend` for runtime failures or ambiguous responses

### 3. Should the project record an explicit MinIO version floor now?

Options:

- A. record only the tested version used in integration
- B. record a minimum validated version/capability floor
- C. defer version language entirely and rely on tests

Recommendation:

- B

Reason:

- the design is now depending on a backend capability, not just generic
  S3 compatibility
- reviewers and operators need to know what was actually validated
