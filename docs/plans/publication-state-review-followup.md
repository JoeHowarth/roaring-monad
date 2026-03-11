# Publication State Review Follow-Up

This document records the shortcomings in the current `publication_state` cutover work and
describes how to address them in a principled way.

It is a review companion to:

- `publication-state-architecture.md`
- `publication-state-implementation-plan.md`

The goal here is not to restate the architecture. The goal is to explain where the implementation
fell short of the intended protocol and how to correct that without layering on more ad hoc fixes.

## Summary

The current implementation moved the crate onto `publication_state`, but it did not fully land the
protocol described in the architecture and implementation-plan documents.

The most important gaps are:

- sealed summaries created before publish are not fully covered by recovery
- stream-page sealing discovery is implemented with a global scan instead of bounded shard/page scans
- the filesystem `PublicationStore` is not a true compare-and-set
- startup recovery is not wired into service lifecycle as a first-class step
- the direct cutover is incomplete because legacy control-plane types/codecs still exist
- batched `H -> T` publication for backfill is still missing

Those are not cosmetic issues. They are protocol issues.

## What Needs To Change

## 1. Treat sealed summaries as publication-critical recoverable state

### Problem

The implementation currently cleans only Class A unpublished artifacts:

- `block_meta/*`
- `block_hash_to_num/*`
- `block_log_headers/*`
- `block_logs/*`
- `log_dir_frag/*`
- `stream_frag_meta/*`
- `stream_frag_blob/*`

But it does not clean or validate pre-publish Class B summaries:

- `log_dir_sub/*`
- `log_dir/*`
- `stream_page_meta/*`
- `stream_page_blob/*`

This leaves a correctness hole:

1. writer creates Class A artifacts
2. writer creates sealed summaries
3. writer crashes before `publication_state` CAS
4. takeover cleans only Class A
5. stale sealed summaries remain and may later conflict with recomputed source state

That violates the model in the plan, which explicitly treated these summaries as publication-
critical artifacts for the corresponding head advance.

### Correct fix

Recovery needs to understand and clean Class B summaries as part of the unpublished suffix.

For a published head `H`:

1. discover unpublished blocks `H+1 ..= U` by walking `block_meta/<block_num>`
2. derive the sealed directory ranges introduced by that unpublished suffix
3. derive the sealed stream pages introduced by that unpublished suffix
4. delete those Class B summaries before deleting Class A anchors
5. then delete Class A artifacts in the documented order

### Required implementation shape

Add recovery helpers that compute:

- `summaries_from_unpublished_suffix(H, suffix) -> { dir_subs, dir_buckets, stream_pages }`

Then extend cleanup ordering to:

1. `stream_page_blob/*`
2. `stream_page_meta/*`
3. `log_dir/*`
4. `log_dir_sub/*`
5. Class A artifact cleanup
6. `block_meta/*` last

### Reuse rule

During publish, if a sealed summary already exists:

- load it
- recompute expected bytes from authoritative source fragments
- reuse only if bytes match exactly
- otherwise return `SummaryConflict`

That gives the implementation an actual cleanup-first story instead of leaving speculative sealed
state behind indefinitely.

## 2. Replace global open-page scans with bounded shard/page scans

### Problem

The current code discovers pages to seal by scanning all `open_stream_page/*` markers.

That works functionally, but it ignores the intended marker design:

- shard first
- page second
- prefix-scan by shard and by `(shard, page)`

The whole reason for the key layout was to bound work to pages that may newly seal for a proposed
`H -> T` publish. A global scan makes ingest cost proportional to total durable marker inventory,
which is not the intended protocol.

### Correct fix

Use frontier arithmetic to determine only the shard/page regions that can newly seal between `F_H`
and `F_T`.

Then:

1. enumerate candidate sealed shard/page ranges
2. prefix-scan markers only for those shard/page prefixes
3. union durable `OpenPages_H` with `OpenedDuring(H -> T)`
4. seal only those pages

### Required helper surface

Add helpers like:

- `list_open_stream_pages_for_shard(meta_store, shard, cursor, limit)`
- `list_open_stream_pages_for_shard_page(meta_store, shard, page_start, cursor, limit)`
- `delete_open_stream_page(...)`
- `mark_open_stream_page_if_absent(...)`

Then remove the full-keyspace `list_all_open_stream_pages` path from normal ingest.

### Why this matters

This is not just a performance optimization. The bounded scan is part of the protocol design
because it defines what state the writer is expected to reason about for a given head advance.

## 3. Implement a real filesystem CAS

### Problem

The filesystem `PublicationStore` currently does:

1. read current file
2. compare contents in process memory
3. overwrite file

That is not compare-and-set.

Two writers can both read the same prior state and both proceed to write a new state. The current
implementation therefore violates the central invariant of the new control plane.

### Correct fix

The filesystem implementation needs a true serialized compare-and-replace protocol.

### Required implementation shape

Use a process-local mutex keyed to the publication-state file path.

Within the critical section:

1. read current publication-state bytes
2. compare against `expected`
3. if mismatch, return `Failed { current }`
4. write `next` to a temp file
5. atomically rename temp file into place
6. update version file under the same lock

`create_if_absent` should similarly:

1. take the same lock
2. use `create_new`
3. return current state on conflict

### Tests that should exist

- concurrent `compare_and_set` calls: exactly one applies
- `create_if_absent` races: exactly one creates
- restart after partial temp-file write does not corrupt the state file

## 4. Make startup recovery a real lifecycle step

### Problem

There is now a `startup_plan`, but it is not authoritative:

- it is not wired into service construction
- recovery actually happens lazily during ingest
- startup cleanup uses `FenceToken(0)`, which is not compatible with fenced stores

That means startup recovery is currently more of a helper than a protocol boundary.

### Correct fix

Choose one model and make it explicit:

1. startup acquires publication ownership and performs recovery, or
2. startup is read-only and ingest does all acquisition and recovery lazily

The implementation plan recommended startup-time cleanup-first recovery. That is the safer shape
and should be adopted directly.

### Recommended shape

Add an explicit async startup step on the service:

- `FinalizedHistoryService::startup(...)`

It should:

1. acquire publication ownership for the local writer
2. load `publication_state`
3. clean unpublished suffix above published head
4. repair stale sealed markers
5. derive `next_log_id`
6. return the recovery plan / warm-state summary

Then ingest can assume:

- the process already owns publication
- startup recovery has already been run

### Fence handling

Do not use `FenceToken(0)` for recovery mutations. Startup recovery must run under an ownership or
recovery token that is valid for the target backend.

## 5. Complete the direct cutover

### Problem

The plan explicitly said:

- add `publication_state`
- remove `writer_lease` from ingest and reads
- remove `indexed_head` from ingest and reads
- do not land a dual-read or dual-write steady state

But legacy types/codecs remain in the crate:

- `MetaState`
- `IndexedHead`
- `WriterLease`
- old encode/decode helpers
- old key constants

Even if they are no longer used in the main path, they leave the codebase in an ambiguous state and
make it easier for future changes to accidentally depend on the superseded model.

### Correct fix

Delete the superseded control-plane model entirely.

That includes:

- domain types
- codecs
- dead key constants
- tests written around the old rows
- docs that still present the old state as current

### End-state rule

After cleanup, the only correctness-critical control-plane record should be:

- `publication_state`

Anything else should be either:

- auxiliary inventory
- local cache
- legacy stream code unrelated to publication correctness

## 6. Add real batched `H -> T` publication

### Problem

The architecture and implementation plan both called out:

- single-block publish
- required batched backfill publish under the same protocol

The current service still operates only on one block at a time.

That means a major part of the intended end state has not been implemented.

### Correct fix

Build the ingest engine around contiguous suffix publication, where single-block ingest is just the
degenerate batch case.

### Required batch flow

For proposed `H -> T`:

1. load current `PublicationState`
2. assert owner/epoch match the writer token
3. clean unpublished suffix if present
4. validate blocks `H+1 ..= T` as a contiguous finalized suffix
5. derive `first_log_id` from `H`
6. persist all Class A artifacts for the batch
7. mark open pages for pages still open at `F_T`
8. compute newly sealed directory summaries from `F_H` and `F_T`
9. compute newly sealed stream pages from `OpenPages_H union OpenedDuring(H -> T)`
10. create-or-validate all newly sealed summaries
11. delete corresponding markers only after successful create/validate
12. CAS `publication_state` directly from `H` to `T`

### Why this matters

Backfill publication is not a separate optimization. It is part of the protocol contract described
by the plan documents.

## 7. Tighten the tests around protocol invariants

### Problem

The current tests prove the implementation compiles and exercises some happy-path behavior, but they
do not fully cover the protocol described in the plan documents.

### Correct fix

Expand tests to cover the actual invariants:

#### Recovery / crash tests

- crash after Class A artifacts but before publish CAS
- crash after Class B summary creation but before publish CAS
- crash after publish CAS but before client acknowledgment
- takeover after partial unpublished suffix, followed by cleanup-first recovery

#### Immutability tests

- duplicate create with identical bytes succeeds for Class A artifacts
- duplicate create with mismatched bytes fails with `ArtifactConflict`
- duplicate create with identical bytes succeeds for Class B summaries
- duplicate create with mismatched bytes fails with `SummaryConflict`

#### Marker inventory tests

- markers are created only for pages still open at `F_T`
- `OpenPages_H union OpenedDuring(H -> T)` determines newly sealed pages
- markers are deleted only after successful summary create/validate
- stale marker recreation causes repair work only and never a query-visible gap

#### Backend tests

- filesystem publication CAS race tests
- publication bootstrap / takeover tests
- startup recovery tests under fenced stores

#### Batch tests

- `H -> T` publish for multiple blocks
- reuse of pre-existing identical summaries
- failure on mismatched pre-existing summaries

## Recommended Remediation Order

The safest way to fix the current state is:

1. make Class B summaries recoverable and validated
2. fix filesystem CAS
3. replace global marker scans with bounded shard/page scans
4. wire startup recovery into service lifecycle
5. add batched `H -> T` publication
6. remove legacy control-plane types/codecs
7. expand tests to lock the protocol in place

This order is deliberate:

- steps 1 and 2 fix correctness gaps
- step 3 fixes the intended protocol shape
- step 4 makes recovery explicit and operationally sane
- step 5 lands the missing feature from the architecture
- step 6 removes ambiguity
- step 7 prevents regression

## Implementation Philosophy

The right way to finish this work is not to keep patching isolated symptoms.

The implementation should be reorganized around these principles:

- one authoritative control-plane record
- immutable authoritative artifacts
- immutable sealed summaries
- cleanup-first recovery of anything above published head
- durable marker inventory as bounded discovery state, not as ownership state
- publication visibility changed only by a single CAS on `publication_state`

If a change does not make those invariants clearer, it is probably moving in the wrong direction.
