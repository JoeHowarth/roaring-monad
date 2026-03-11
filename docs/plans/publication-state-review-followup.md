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

- publication acquisition is not actually exclusive
- stale-writer fencing still uses owner identity instead of publication epoch
- `list_prefix` cursor semantics are underspecified and can livelock multi-page scans
- sealed summaries created before publish are not fully covered by recovery
- stream-page sealing discovery is implemented with a global scan instead of bounded shard/page scans
- the filesystem `PublicationStore` is not a true compare-and-set
- startup recovery is not wired into service lifecycle as a first-class step
- the direct cutover is incomplete because legacy control-plane types/codecs still exist
- batched `H -> T` publication for backfill is still missing

Those are not cosmetic issues. They are protocol issues.

## What Needs To Change

## 1. Make publication acquisition actually exclusive

### Problem

The current acquisition path still conflates observation with ownership.

There are two specific failure modes:

1. bootstrap does `create_if_absent(initial)` and returns the winning row even when this caller lost
   the race
2. acquire returns immediately when `current.owner_id == owner_id`, without proving that this
   process established or refreshed ownership

That means a caller can treat an old or foreign `publication_state` row as its lease without
performing a successful compare-and-set.

This violates the central ownership invariant from the architecture:

- publication ownership changes only by CAS on `publication_state`

### Correct fix

`acquire_publication` must return a lease only if this caller:

- created the row, or
- won a CAS that moved the row to a fresh epoch owned by this caller

It must never return a lease solely because the loaded row happens to contain the same `owner_id`.

### Required implementation shape

For owner `W`:

1. load current `publication_state`
2. if absent, try `create_if_absent({ owner_id: W, epoch: 1, head: 0 })`
3. if bootstrap create loses, continue acquisition from the returned current row
4. attempt `compare_and_set(current, { owner_id: W, epoch: current.epoch + 1, head })`
5. retry on CAS failure with the returned current row
6. return only after this caller wins create-or-CAS

### Same-owner restart rule

With the current schema, reusing the same numeric `writer_id` on restart must still force an epoch
bump.

That is the only safe interpretation absent an additional process/session nonce. It ensures:

- stale processes holding the old epoch lose fencing
- a restarted process does not silently inherit an unverifiable lease

If a future design wants same-process reattach semantics, it needs an explicit session identity in
`publication_state`. The current model does not have enough information for that.

## 2. Make publication epoch the only fence token

### Problem

The implementation still threads `owner_id` / `writer_id` into mutable-store operations as the
`FenceToken`.

But fenced stores compare that token against the minimum valid publication epoch, and publication
CAS itself is already driven by epoch.

That means stale-writer suppression is not keyed to the same logical clock as ownership. Once
`owner_id` and `epoch` diverge, fenced deletes and writes can fail or, worse, be authorized on the
wrong basis.

### Correct fix

All fenced mutable operations must use:

- `FenceToken(publication_epoch)`

and nothing else.

`owner_id` should remain an ownership label inside `publication_state`. It should not be used to
authorize mutable writes.

### Required implementation shape

Add a single helper on the acquired lease, for example:

- `PublicationLease::fence_token() -> FenceToken`

Then thread that token through:

- startup cleanup
- startup marker repair
- ingest artifact writes
- open-page marker puts/deletes
- summary compaction writes

### Code-shape rule

Any helper that only needs fencing authority should take:

- `fence: FenceToken`

not `writer_id` and not `owner_id`.

If a helper truly needs both ownership identity and write authorization, those should be passed as
separate values so the distinction is explicit in the call site.

## 3. Define `list_prefix` cursor semantics as opaque forward progress

### Problem

The `list_prefix` contract does not currently specify whether `next_cursor` is:

- inclusive of the last returned key
- exclusive of the last returned key
- an opaque backend continuation token

Some backends currently treat it as inclusive. Callers then pass that cursor back unchanged, which
can repeat the final key of the previous page forever once a prefix spans multiple pages.

This is not merely a pagination bug. The new bounded-scan recovery and marker-repair loops rely on
`list_prefix` making forward progress.

### Correct fix

Define `Page.next_cursor` as:

- an opaque token that resumes strictly after the keys in the current page

Callers should never inspect or transform it. They should only pass it back unchanged.

### Required implementation shape

1. document the cursor contract in the store traits
2. update in-memory and filesystem stores to return an exclusive continuation token
3. update Scylla to follow the same rule
4. keep MinIO using its backend continuation token, which already behaves like an opaque forward
   cursor
5. keep scan loops simple: `cursor = page.next_cursor`

### Tests that should exist

- meta-store multi-page prefix scan returns every key exactly once
- blob-store multi-page prefix scan returns every key exactly once
- filesystem multi-page prefix scan returns every key exactly once
- bounded recovery / marker scan tests that cross the pagination limit

## 4. Treat sealed summaries as publication-critical recoverable state

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

## 5. Replace global open-page scans with bounded shard/page scans

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

## 6. Implement a real filesystem CAS

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

## 7. Make startup recovery a real lifecycle step

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

## 8. Complete the direct cutover

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

## 9. Add real batched `H -> T` publication

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

## 10. Tighten the tests around protocol invariants

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
- same-owner reacquire must bump epoch
- fenced ingest / recovery use publication epoch, not owner identity
- startup recovery tests under fenced stores
- multi-page store listing tests with exact-once pagination

#### Batch tests

- `H -> T` publish for multiple blocks
- reuse of pre-existing identical summaries
- failure on mismatched pre-existing summaries

## Recommended Remediation Order

The safest way to fix the current state is:

1. make acquisition exclusive and epoch-driven
2. switch all fenced mutations to publication epoch
3. define and fix `list_prefix` forward-progress semantics
4. make Class B summaries recoverable and validated
5. fix filesystem CAS
6. replace global marker scans with bounded shard/page scans
7. wire startup recovery into service lifecycle
8. add batched `H -> T` publication
9. remove legacy control-plane types/codecs
10. expand tests to lock the protocol in place

This order is deliberate:

- steps 1 and 2 fix the ownership/fencing core
- step 3 fixes forward progress for recovery and bounded scans
- steps 4 and 5 fix publication-critical storage correctness
- step 6 fixes the intended protocol shape
- step 7 makes recovery explicit and operationally sane
- step 8 lands the missing feature from the architecture
- step 9 removes ambiguity
- step 10 prevents regression

## Implementation Philosophy

The right way to finish this work is not to keep patching isolated symptoms.

The implementation should be reorganized around these principles:

- one authoritative control-plane record
- ownership established only by successful create-or-CAS
- fencing authorized only by publication epoch
- immutable authoritative artifacts
- immutable sealed summaries
- cleanup-first recovery of anything above published head
- durable marker inventory as bounded discovery state, not as ownership state
- prefix scans defined by an exact-once forward-progress contract
- publication visibility changed only by a single CAS on `publication_state`

If a change does not make those invariants clearer, it is probably moving in the wrong direction.
