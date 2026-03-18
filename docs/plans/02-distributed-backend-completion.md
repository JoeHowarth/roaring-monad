# Distributed Backend Completion

## Summary

This plan defines the work required to make the distributed backend pair
for `crates/finalized-history-query` a complete and trustworthy remote
implementation of the crate's storage contracts.

The crate already has a clear backend abstraction model:

- `MetaStore`
- `BlobStore`
- `PublicationStore`
- `FenceStore`

Those contracts are documented in [../backend-stores.md](../backend-stores.md).
The local and filesystem implementations already satisfy the core write
path well enough for development and single-process validation.

The distributed path is not at that bar yet.

The most concrete current blocker is in the object-store adapter:

- ingest publishes immutable artifacts through
  `BlobStore::put_blob_if_absent(...)`
- `MinioBlobStore` still returns `Unsupported` for that method

That means the advertised distributed object-store pair is not yet a
complete implementation of the crate's immutable artifact model.

The metadata path is in better shape:

- `ScyllaMetaStore` already uses LWT-backed `IF NOT EXISTS` and
  `IF version = ?` conditionals
- fence state exists
- retry policy and telemetry exist

But it still needs stronger end-to-end evidence under the ownership,
publication, and retry semantics this crate relies on.

## Goal

Finish and harden the distributed backend pair so the crate has a real
production-capable remote metadata and blob-store story.

## Why This Matters

The crate's correctness model depends on the backend pair providing all
of the following in practice, not just in trait signatures:

- immutable artifact writes
- conditional metadata publication
- epoch fencing
- reliable list and range-read behavior
- predictable retry and failure semantics

If the distributed backends cannot provide those semantics cleanly, the
rest of the crate's publication, recovery, and verification work will be
either misleading or blocked.

This workstream is the bridge between "the design works on in-memory and
filesystem doubles" and "the design has a real remote execution path."

Concrete implementation notes:

- `docs/plans/distributed-blob-conditional-create.md`

## Current State

### Storage contracts

The crate expects:

- conditional metadata writes from `MetaStore`
- conditional publication-state updates from `PublicationStore`
- epoch fencing from `FenceStore`
- immutable blob creation and range reads from `BlobStore`

### Scylla path

`ScyllaMetaStore` already provides:

- key partitioning by group and bucket
- `IF NOT EXISTS` support for `PutCond::IfAbsent`
- `IF version = ?` support for `PutCond::IfVersion`
- fence state with cached reads
- retry and telemetry infrastructure

### MinIO path

`MinioBlobStore` already provides:

- blob put/get/delete
- list-prefix
- retry policy

But it does not yet provide:

- immutable create-if-absent semantics

### Current distributed tests

The distributed tests currently cover:

- two basic Scylla CAS race cases
- one Scylla+MinIO happy-path roundtrip
- one opt-in chaos test

That is useful, but it is not yet enough evidence for production-grade
distributed correctness.

## Scope

- implement the missing immutable blob-create semantics for the MinIO
  backend
- verify that the Scylla publication and metadata CAS behavior matches
  the crate's fencing and ownership model under contention
- review backend-specific retry, timeout, and error classification
  policy
- tighten distributed tests so they actually exercise the intended
  backend path and failure mode
- make backend limitations and guarantees explicit in docs and tests

## Out Of Scope

- GC and maintenance semantics above the backend contract layer
- dashboards, alerts, and runbooks
- long-duration performance characterization
- deployment topology and production security policy in full

## Backend Requirements

The distributed backend pair must satisfy the following end-state
requirements.

### Metadata requirements

- conditional create and versioned update behavior must be authoritative
- publication-state CAS must behave predictably under takeover and
  conflict
- fencing must reject stale epochs on the paths that mutate metadata
- prefix listing must be correct enough for recovery, compaction, and
  cleanup flows

### Blob requirements

- immutable artifact creation must be supported for the write path
- blob reads must distinguish missing versus failed objects
- range reads must be correct and economical enough for point
  materialization
- listing semantics must support recovery and cleanup enumeration

### Failure-model requirements

- retries must not silently weaken correctness guarantees
- backend errors must be classifiable into retryable versus terminal
  classes
- takeover and retry must not leave the system dependent on unspecified
  backend behavior

## Design Constraints

### Immutable artifact writes are not optional

The storage model assumes that once published, data-path artifacts are
immutable and can be safely cached forever until eviction.

That means the backend pair needs a real answer for immutable artifact
creation. A plain unconditional overwrite is not equivalent.

### Metadata and blob semantics do not need to be symmetric

It is acceptable for Scylla and MinIO to use different backend-native
techniques internally so long as they preserve the same crate-level
contract.

### Correctness comes before convenience

If a backend-native feature is awkward to expose but required for the
crate's safety model, the plan should prefer correctness over a simpler
but weaker API.

### Native conditional object create is the chosen direction

The current design choice for the distributed blob path is:

- use native conditional object creation semantics via
  `If-None-Match: "*"` on object write

This is the required direction for the current plan.

That means the work here is:

- implement that path in `MinioBlobStore`
- validate that the MinIO deployment used for this project honors the
  semantics correctly

Fallback protocols such as content-addressed indirection, staged blob
commit, or best-effort "check then put" are out of scope for the current
implementation plan unless the native path proves unusable in practice.

### Backend capability validation must include version compatibility

Native conditional object creation is not just an API-shape choice. It
is also a deployment-capability requirement.

This plan therefore assumes:

- the MinIO version used in integration and production environments must
  actually honor the required `If-None-Match: "*"` semantics

Validation must not stop at "works on one local environment." It must
establish the effective capability floor the project depends on.

## Work Packages

## 1. Close The MinIO Immutable-Write Gap

### Intent

Provide a real implementation of immutable create-if-absent behavior for
the blob path.

### Tasks

- implement native conditional object creation behind
  `put_blob_if_absent(...)` using `If-None-Match: "*"`
- validate that the MinIO deployment used in integration testing honors
  the expected semantics
- define how identical replay writes versus conflicting writes are
  detected and reported
- verify the behavior for:
  - first write
  - exact replay
  - conflicting second write
  - concurrent writers

### Deliverables

- working `MinioBlobStore::put_blob_if_absent(...)`
- tests proving the intended immutable-write semantics
- docs that describe the chosen behavior explicitly
- an explicit statement of the validated MinIO capability/version floor

## 2. Tighten The Scylla Correctness Story

### Intent

Move from "Scylla appears to implement the required CAS paths" to
"Scylla behavior is proven sufficient for the crate's publication and
takeover protocol."

### Tasks

- review publication-state operations against the lease and fencing model
- verify conflict handling on:
  - create-if-absent
  - versioned updates
  - publication-state CAS
  - delete-if-version where used
- test contention and takeover scenarios using real Scylla
- confirm fence refresh timing and stale-epoch rejection behave as
  expected under retries

### Deliverables

- stronger distributed correctness tests for Scylla-backed authority and
  publication flows
- explicit doc language about which guarantees rely on LWT and which do
  not

## 3. Review Retry And Error Policy

### Intent

Ensure the backend retry layers do not hide failures, misclassify
conflicts, or weaken fail-closed behavior.

### Tasks

- review retryable error classification for both Scylla and MinIO
- verify bounded retry behavior on write and read paths
- verify that non-retryable correctness failures surface directly
- decide whether jitter, backoff tuning, or per-operation policy should
  differ between:
  - publication/cas paths
  - ordinary reads
  - background enumeration

### Deliverables

- documented retry policy
- tests for retry-budget exhaustion and terminal failure propagation
- no ambiguity about which errors should trip degraded/throttled state

## 4. Repair And Expand The Distributed Test Matrix

### Intent

Make the distributed test suite a real source of confidence rather than
just a smoke-check layer.

### Tasks

- ensure each test actually reaches the intended backend path
- add missing distributed scenarios for:
  - ingest then query happy path with immutable writes
  - publication conflict and retry
  - takeover after lease expiry
  - stale writer rejection after fence advance
  - object-store outage before and after write attempt
  - metadata outage during publish
  - restart and recovery after partial distributed writes
- separate always-on distributed tests from opt-in chaos or destructive
  tests
- define the manual distributed validation gate that runs for every
  review PR, since normal PR CI does not run these suites

### Notes

The current chaos test should be reviewed carefully to ensure the query
path actually reaches backend access rather than failing early on request
validation.

### Deliverables

- a distributed suite whose scenarios map cleanly to the backend
  guarantees the crate depends on
- clear distinction between smoke, correctness, and chaos layers
- an explicit per-PR manual distributed validation expectation

## 5. Clarify Backend Capability Boundaries

### Intent

Make backend guarantees and limitations explicit enough that host
codebases do not assume more than the adapters actually provide.

### Tasks

- document required versus optional backend features
- decide whether unsupported backends should be rejected eagerly at
  construction time
- document any backend-specific caveats that remain after this plan
  lands

### Deliverables

- cleaner backend docs
- no hidden capability gaps on the core distributed write path

## Milestones

## Milestone A: Blob Contract Closed

Complete when:

- MinIO supports immutable create-if-absent semantics for the write path
- distributed ingest no longer depends on an unsupported operation

## Milestone B: Metadata Semantics Proven

Complete when:

- Scylla CAS, fencing, and publication behavior are validated against the
  crate's authority model
- contention cases are covered by real distributed tests

## Milestone C: Distributed Test Matrix Credible

Complete when:

- happy-path, adversarial, and outage scenarios are all represented
- smoke and chaos layers are separated cleanly

## Milestone D: Ready For Production-Facing Follow-Up

Complete when:

- the distributed backend pair is a full implementation of the crate's
  storage contracts
- later workstreams can treat the remote path as real, not provisional

## Exit Criteria

- the distributed blob-store path fully supports the immutable write
  operations used by ingest and compaction
- the distributed metadata path has an explicit correctness story for
  CAS, fencing, and takeover behavior
- happy-path and adversarial tests cover the backend behaviors the crate
  relies on
- there are no known unsupported backend operations on the core write
  path
- backend retry and conflict semantics are documented well enough for
  later correctness and operations work

## Dependencies

- depends on [(01-core-productization-and-upstreaming.md)]((01-core-productization-and-upstreaming.md))
  for a stable code and docs surface
- unblocks
  [(04-correctness-verification-matrix.md)]((04-correctness-verification-matrix.md))
  and later production work

## Risks

### Choosing a weak object-store protocol

If the MinIO immutable-write answer is convenient but not actually
strong enough, later correctness work will rest on a false assumption.

### Validating only one MinIO version

If native conditional object create is validated only against one local
or CI MinIO version, the project can still fail later in another
deployment that lacks the same behavior. The plan needs an explicit
capability floor, not just one successful test environment.

### Over-fitting to local Docker success

A distributed test that only passes on the developer docker-compose path
is not the same as a backend contract proof.

### Retry layers masking correctness failures

If retries blur the difference between contention, transient backend
trouble, and hard semantic violations, the service's higher-level state
machine will make worse decisions.

## Review Checklist

When this plan is implemented, reviewers should be able to answer "yes"
to all of the following:

- does the distributed path implement every store operation the core
  write path depends on
- do Scylla CAS and fence behaviors match the crate's ownership and
  publication model
- do distributed tests exercise the intended backend path rather than an
  earlier validation failure
- are backend limitations explicit rather than implicit

## Dependencies On Other Plans

- [(04-correctness-verification-matrix.md)]((04-correctness-verification-matrix.md))
  builds on the scenarios and guarantees clarified here
- [(05-observability-and-operations.md)]((05-observability-and-operations.md))
  depends on stable backend error and retry semantics

## Follow-Up Questions

- whether object-store "create if absent" should be implemented with a
  strict backend-native conditional path, a higher-level manifest
  protocol, or another immutable-write scheme if MinIO validation proves
  the native conditional path unusable in practice
- whether backend capabilities should be feature-gated or negotiated more
  explicitly at construction time
- what MinIO version floor or capability statement should be documented
  as a deployment requirement for native conditional object create
- whether some distributed tests should move into a dedicated docker or
  pre-prod pipeline rather than ordinary CI
