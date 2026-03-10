# Boundary Sealing Architecture

## Summary

This document defines how log-directory buckets and shard-local stream state become sealed and therefore safe to treat as immutable.

The intended end state is:

- keep the current public query and ingest APIs
- keep shard-local stream manifests and tails during active ingest
- synchronously finalize completed shards during ingest when `next_log_id` crosses a shard boundary
- treat completed log-directory buckets as sealed once `next_log_id` moves past the bucket end
- give readers a strong guarantee on supported strongly consistent backends: once shard `S + 1` has begun, shard `S` manifests and tails are final everywhere
- make sealed-state predicates explicit so the cache layer can promote mutable objects to immutable caching safely

This document is about storage lifecycle and ingest behavior. Cache policy is a separate concern and should build on the sealing rules defined here.

## Goals

- define when a `log_dir` bucket is sealed
- define when a shard-local manifest is sealed
- make shard-boundary finalization part of ingest, not eventual background maintenance
- avoid broad global scans when finding open tails for a completed shard
- preserve current query semantics and ascending `log_id` execution
- keep the current stream layout of manifest, tail, and chunk objects
- produce clear reader-visible immutability guarantees for sealed data

## Non-Goals

- changing `query_logs` request or pagination semantics
- redesigning stream chunk encoding
- redesigning the current log family index layout
- solving cache policy in this document
- generalizing the first implementation beyond logs

## Problem Statement

The current stream lifecycle has two different notions of "finished":

- a shard can be logically complete because ingest has moved into a later shard
- a stream tail for that completed shard may still remain in `tails/<stream_id>` until threshold-based sealing or later maintenance runs

That gap matters because the current manifest object is only truly immutable after the final tail for that shard has been drained into chunk and manifest state.

The current code also lacks a narrow way to discover the incomplete tails for one completed shard. Periodic maintenance lists all tail keys and replays `apply_appends(..., [])` globally.

As a result:

- old-shard manifests are not strongly immutable at shard rollover
- non-writer readers cannot safely treat completed-shard manifests as immutable
- the cache plan cannot cleanly promote manifests from mutable to immutable state

## Core Model

### 1. Bucket sealing

A `log_dir` bucket with aligned start `bucket_start` and size `LOG_DIRECTORY_BUCKET_SIZE` is sealed when:

```text
next_log_id > bucket_start + LOG_DIRECTORY_BUCKET_SIZE - 1
```

Equivalently, the bucket is sealed once ingest has advanced into a later bucket.

No extra write is required to seal a bucket. The bucket becomes immutable because future blocks cannot add or rewrite any boundary that belongs inside that aligned `log_id` range.

### 2. Shard sealing

A shard-local stream for shard `S` is logically complete when:

```text
S < log_shard(next_log_id)
```

However, logical completion is not enough. A shard-local manifest becomes sealed only after:

- no future appends can target shard `S`
- every remaining tail for shard `S` has been flushed into chunk/manifest state
- the mutable tail state for shard `S` has been removed

After those conditions hold, the manifest for shard `S` is immutable.

### 3. Reader guarantee

The system should provide this guarantee for supported strongly consistent backend profiles:

- once `meta/state.next_log_id` points into shard `S + 1` or later, every manifest for shard `S` is final and every tail for shard `S` has been drained before that state is committed

This guarantee is the reason shard-boundary finalization must be synchronous with ingest.

At the generic trait level, `MetaStore` and `BlobStore` do not themselves provide cross-key or cross-store snapshot semantics.

As a result, the strong reader guarantee in this document is not implied by the generic traits alone. It depends on a publication protocol plus backend consistency strong enough that a reader observing the new `meta/state` can also observe the writes that were required before that state was published.

## Required Metadata

The first implementation should add a shard-local open-tail index so ingest can finalize only the completed shard instead of scanning every tail globally.

Recommended shape:

- `open_tails/<shard>/<stream_id> -> marker`

Properties:

- the marker is a conservative membership hint for streams that may currently have a non-empty tail for that shard
- listing `open_tails/<completed_shard>/` yields a superset of the streams that may still need finalization for that shard
- marker churn happens only on tail empty/non-empty transitions, not on every append

This avoids a single giant per-shard membership object while also avoiding a full `tails/` scan at rollover.

The required safety invariant is:

- the index must never under-report a non-empty tail for a shard that might still need finalization before `meta/state` advances past that shard

Over-reporting is acceptable. A stale marker may cause an unnecessary verification read during finalization, but it must not allow a live tail to be skipped.

## Ingest Flow

### Normal appends inside the current open shard

The existing threshold/time-based behavior remains valid for the active shard:

- append local IDs into the tail
- seal to a chunk when normal thresholds fire
- update manifest and tail state

The new requirement is that tail writes also maintain `open_tails/<shard>/<stream_id>` membership:

- tail transitions from empty to non-empty: insert marker
- tail remains non-empty: no membership change
- tail transitions from non-empty to empty: delete marker

Because the metadata API does not provide a multi-key transaction across tail state and marker state, the implementation must rely on write ordering rather than exact atomic membership.

The required ordering is:

- when tail state becomes non-empty, write the marker before publishing the non-empty tail state
- when tail state becomes empty or is deleted, remove the tail state before deleting the marker

That ordering makes stale markers possible, but it prevents under-reporting of live tails.

### Boundary-triggered finalization

After a block’s log stream appends have been applied, ingest computes:

- `previous_next_log_id`
- `next_log_id`
- `previous_open_shard = log_shard(previous_next_log_id)`
- `new_open_shard = log_shard(next_log_id)`

If `new_open_shard > previous_open_shard`, ingest must synchronously finalize every newly completed shard in:

- `[previous_open_shard, new_open_shard)`

For each completed shard:

1. list `open_tails/<completed_shard>/`
2. for each listed stream, force-flush the remaining tail even if normal thresholds would not seal it
3. append the flushed tail into a final chunk and manifest update for that shard
4. remove the `tails/<stream_id>` object or overwrite it to a known-empty state, with removal preferred
5. remove the corresponding `open_tails/<completed_shard>/<stream_id>` marker
6. assert that the completed shard no longer has open-tail markers before advancing shared state

Only after all newly completed shards are finalized may ingest commit the new `meta/state`.

### Blocks that span multiple shards

A single large block may cross more than one shard boundary.

The finalization rule stays the same:

- apply the block’s stream appends
- finalize every shard that became completed by the resulting `next_log_id`
- then commit `meta/state`

This guarantees that every newly closed shard is finalized before the new head becomes visible.

This does not require splitting one mutable tail object across multiple shards. Stream IDs already include the shard, so:

- `manifests/<stream_id>` is already shard-qualified
- `tails/<stream_id>` is already shard-qualified
- a block that contributes entries to shard `S` and shard `S + 1` writes different stream keys for those shards

Finalizing shard `S` therefore does not delete or overwrite the live tail for shard `S + 1`.

## Stream Writer Changes

The current `StreamWriter::apply_appends` path should be split conceptually into two modes:

- normal append mode for the active shard
- forced-finalize mode for completed shards

Forced-finalize mode differs from normal sealing in one important way:

- it must flush a non-empty tail even when entry-count, byte-size, and time thresholds would not normally seal it

This should reuse the same chunk encoding and manifest update rules already used for normal sealing so the persisted format stays uniform.

If a completed-shard marker is stale and the corresponding tail is already missing or empty, forced-finalize mode should treat that as a no-op and clear the stale marker.

## Tail Object Lifecycle

After this change, a tail object represents only mutable in-progress state for the current open shard.

That implies:

- closed shards should not retain non-empty tail objects
- deleting empty tails is preferable to storing permanent empty-tail records
- query code should continue to interpret missing tail as empty

This keeps the meaning of `tails/<stream_id>` narrow and avoids leaving mutable-looking residue behind sealed manifests.

## Log-Directory Bucket Lifecycle

`log_dir` buckets do not need explicit finalization writes.

The only required change is to make their sealing predicate explicit:

- the current bucket containing `next_log_id` is mutable
- every earlier bucket is sealed

That gives the cache layer a clean promotion rule without adding extra ingest work.

## Query and Reader Implications

The query path does not need semantic changes.

However, shared state readers and future cache code need access to sealing watermarks. The simplest current-state basis is:

- `next_log_id` from `meta/state`

From that value readers can derive:

- current open shard
- current open `log_dir` bucket
- whether a manifest or bucket is sealed

The current `FinalizedHeadState` projection does not expose `next_log_id`, so the first implementation should add either:

- a wider shared finalized-state projection, or
- a new helper dedicated to sealing/caching watermarks

For unsupported or weakly consistent backend profiles, `next_log_id` still defines writer publication order, but it is not enough by itself to claim that every non-writer reader will immediately observe the sealed artifacts. In those environments, seal status should be treated as a writer-side property unless the backend contract is strengthened.

## Publication Contract

The strong reader guarantee in this document requires more than "write dependent objects first, then write `meta/state` last."

It also requires using backend consistency modes that make the finalization writes and the publishing `meta/state` write visible to readers with the intended semantics.

The required publication ordering is:

1. write all chunk blobs for newly finalized shards
2. write all manifest, tail, and marker changes for those finalized shards
3. only after those writes succeed, write `meta/state` with the new `next_log_id`

Readers may treat `meta/state.next_log_id` as a seal watermark only when the deployment uses one of the supported backend profiles below.

### Supported backend profiles

#### Scylla metadata + S3 or MinIO blobs

For Scylla-backed metadata stores, sealing publication should use `ConsistencyLevel::ALL` for:

- final manifest writes
- final tail deletes or empty-tail writes
- `open_tails` marker writes and deletes
- the final `meta/state` write that publishes the new `next_log_id`

This is intentionally stronger than the normal hot-path write profile and should apply only to the sealing writes that publish closed-shard immutability.

For blob visibility:

- all final chunk blob writes must complete before the `meta/state` publish write
- the blob backend must provide strong read-after-write and list-after-write consistency

This profile is intended for:

- Amazon S3
- single-site MinIO deployments configured in a consistency-preserving mode

It is not intended to rely on asynchronous cross-site replication.

#### DynamoDB metadata + S3 or MinIO blobs

For DynamoDB-backed metadata stores, sealing publication should use:

- transactional metadata publication where practical for the manifest/tail/marker set
- strongly consistent reads (`ConsistentRead=true`) for any reader path that derives seal status from metadata

As with Scylla:

- chunk blob writes must complete before the publishing `meta/state` write
- the blob backend must provide strong read-after-write and list-after-write consistency

This profile is also intended for:

- Amazon S3
- single-site MinIO deployments configured in a consistency-preserving mode

### Backend notes

Scylla:

- `CL=ALL` reduces availability because all replicas must acknowledge the write
- this stronger consistency mode should therefore be reserved for boundary-sealing publication, not every ingest write

DynamoDB:

- strongly consistent reads are available only on base tables and LSIs, not GSIs
- if metadata publication depends on multiple items changing together, transactional writes are preferred

Amazon S3:

- successful object writes, overwrites, deletes, and list operations are strongly consistent
- this makes S3 a suitable publication target for the chunk side of sealed-shard publication

MinIO:

- single-site MinIO deployments document strict read-after-write and list-after-write consistency
- asynchronous replication across sites should not be used as the basis for this reader guarantee

## Failure and Atomicity Expectations

The strong reader guarantee depends on ordering.

Required ordering for a boundary-crossing ingest step:

1. persist block artifacts and stream appends
2. finalize every newly completed shard
3. commit `meta/state` with the new `next_log_id`

If finalization fails:

- the ingest step fails
- `meta/state` must not advance past the boundary

This preserves the invariant that published state never claims a shard is closed while still leaving mutable tails behind for that shard.

## Maintenance Role After This Change

Periodic maintenance still has a role, but it should no longer be responsible for closed-shard correctness.

After this change, maintenance is for:

- opportunistic sealing inside the current open shard
- cleanup and repair checks
- operational guardrails

It should not be the mechanism that finally seals an already completed shard.

## Implementation Order

### Phase 1

- add shard-local open-tail membership markers
- teach normal tail writes to maintain marker membership
- add forced-finalize path for one completed shard
- enforce boundary-triggered synchronous finalization before `meta/state` commit

### Phase 2

- expose sealing watermarks through shared state helpers
- use the new predicates in the cache layer for mutable-to-immutable promotion

### Phase 3

- evaluate whether empty-tail deletion should replace all empty-tail writes, not only boundary finalization
- extend the same lifecycle rules to other artifact families if they adopt shard-local mutable state

## Relationship To Other Plans

This document is a prerequisite for:

- `docs/plans/metadata-caching-architecture.md`

The cache plan should treat shard-local manifests and `log_dir` buckets as promotable to immutable caching based on the sealing rules defined here.
