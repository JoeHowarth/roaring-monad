# Immutable Frontier Index Architecture

## Summary

This document proposes a replacement for the current mutable manifest, mutable tail, and mutable open-bucket model in `crates/finalized-history-query`.

The intended end state is:

- keep block-keyed log payload storage
- keep `log_id` as the primary ordering and pagination identity
- stop using one mutable `meta/state` record for both publication and writer-local sequencing state
- stop persisting mutable stream manifests and mutable stream tails
- stop persisting mutable open `log_dir` buckets
- write only immutable index fragments while a shard or bucket is still open
- compact those immutable fragments into larger immutable summaries when deterministic boundaries are crossed
- answer tip queries from a bounded immutable frontier plus direct recent block reads where needed

The key simplification is:

- sealed data is authoritative
- open-frontier data is also authoritative, but it is represented as immutable micro-structures rather than read-modify-write mutable summaries
- large grouped summaries are optimizations built from immutable source fragments, not correctness-critical state
- lease ownership is a separate concern from indexed-head publication
- `next_log_id` is writer-local derived state, not a per-block published metadata field

## Goals

- remove persisted mutable index state from the logs family
- make writer handoff safe without trusting inherited mutable summaries
- make every persisted index artifact immutable after write
- separate strict-CAS lease ownership from normal once-per-block indexed publication
- preserve the current public query API and pagination semantics
- preserve block-keyed log payload storage
- keep tip/frontier queries efficient enough without requiring mutable metadata
- define every metadata and blob structure required by the new model
- leave room for aggressive eager fetch and caching of recent immutable frontier artifacts

## Non-Goals

- changing `query_logs` request or response shapes
- changing `log_id` semantics
- changing block-keyed log payload layout
- designing non-log families in the first pass
- solving cache hierarchy in this document
- preserving compatibility with the current mutable manifest/tail layout

## Problem Statement

The current design still relies on persisted mutable aggregate state:

- `manifests/<stream_id>`
- `tails/<stream_id>`
- the current open `log_dir/<bucket_start>`

That state creates several coupled problems:

- writer handoff needs fresh, mutually consistent reads of mutable state
- mutable cache safety depends on writer tenure and backend visibility semantics
- read-modify-write metadata complicates correctness reasoning
- sealing logic becomes responsible for turning mutable state into immutable state at exactly the right time
- one mutable `meta/state` record mixes three different concerns:
  - indexed publication watermark
  - writer-local sequencing state
  - lease/fencing identity

The underlying data does not require that complexity.

Finalized blocks arrive in one monotonic order. Their derived log IDs are monotonic. That means the crate can instead represent both sealed history and the current frontier with append-only immutable artifacts, then build larger immutable summaries when deterministic boundaries become complete.

## Design Principles

### 1. No persisted mutable index state

The logs family should not persist any correctness-critical read-modify-write index object.

Specifically, the new design should remove:

- mutable stream manifests
- mutable stream tails
- mutable open `log_dir` buckets

Process-local in-memory batching may still exist as an optimization, but persisted state should remain valid if that batching layer is absent.

### 1a. Separate lease ownership from publication state

The design should distinguish:

- lease ownership: who is allowed to publish writes
- indexed publication watermark: how far readers may query
- writer-local derived state: what the active writer keeps in memory while ingesting

These should not be collapsed into one per-block mutable record.

The intended split is:

- dedicated lease record with strict CAS / versioned acquire and renew
- small published indexed-head watermark written once per block
- local `next_log_id` derived from authoritative `BlockMeta`, not persisted every block

### 2. Fixed boundaries, not time-driven sealing

Index publication should be driven by deterministic aligned boundaries in `log_id` space or shard-local ID space.

The new design should not depend on:

- wall-clock seal intervals
- mutable `last_seal_unix_sec`
- background maintenance as a correctness mechanism

### 3. Immutable frontier first, grouped summaries second

Small immutable frontier fragments are the base truth for the not-yet-grouped region.

Larger grouped summaries are derived from those frontier fragments after a boundary is complete.

That means:

- the frontier remains queryable even before compaction
- compaction can be retried or delayed without correctness loss
- grouped summaries can be treated as pure acceleration artifacts

### 4. Bound the ungrouped reader frontier

Readers should never need to search arbitrarily far through raw recent state.

The architecture should bound ungrouped work by fixed window sizes:

- one current open stream page per stream per shard
- one current open log-directory sub-bucket per 1M-log bucket

If the frontier is still too expensive in some cases, readers may fall back to direct recent block metadata and payload reads for that bounded region.

This bounded-frontier claim applies to the not-yet-sealed region.

If compaction lags behind for already sealed pages or sub-buckets, readers may still need fragment-based fallback outside the live frontier. That does not change correctness, but it does change latency and object-count behavior. Production use therefore still needs a compaction SLO even though correctness does not depend on immediate compaction.

## Persisted Model

### Shared authoritative records that stay unchanged

These objects remain authoritative and continue to exist:

Metadata store:

- `indexed_head -> IndexedHead { indexed_finalized_head }`
- `block_meta/<block_num> -> BlockMeta { block_hash, parent_hash, first_log_id, count }`
- `block_hash_to_num/<block_hash> -> block_num`
- `block_log_headers/<block_num> -> BlockLogHeader`

Lease / fencing store:

- `writer_lease -> WriterLease { owner_id, epoch, expiry or heartbeat metadata }`

Blob store:

- `block_logs/<block_num> -> concatenated encoded log bytes`

These remain the authoritative per-block finalized history records plus the minimal publication watermark needed by readers.

`indexed_head` and `writer_lease` serve different purposes:

- `indexed_head` tells readers how far indexing has been published
- `writer_lease` tells writers who may publish

Only the lease record needs strict CAS / conditional update on acquire and renew.

### Writer-local derived state

The active writer may keep these values locally:

- `next_log_id`
- current open frontier batching state, if any

`next_log_id` is derived from `BlockMeta`, not treated as a correctness-critical published record.

For block `N`:

- if `N == 0`, then `next_log_id = 0`
- otherwise load `block_meta/<indexed_head>` and compute:
  - `next_log_id = first_log_id + count`

Empty blocks still work under this rule because `BlockMeta.count` is authoritative even when `block_log_headers/<block_num>` is absent.

### New log-directory structures

The new directory layout is two-level and fully immutable.

#### Level 0: directory fragments

One immutable fragment is written per finalized block for every covered sub-bucket:

Metadata store:

- `log_dir_frag/<sub_bucket_start>/<block_num> -> LogDirFragment`

Recommended payload:

- `block_num`
- `first_log_id`
- `end_log_id_exclusive`

Properties:

- immutable after write
- small
- enough to resolve block boundaries within one sub-bucket

#### Level 1: sealed directory sub-buckets

When a log-directory sub-bucket boundary is crossed, the completed sub-bucket may be compacted into:

Metadata store:

- `log_dir_sub/<sub_bucket_start> -> LogDirectorySubBucket`

Recommended payload:

- `start_block`
- `first_log_ids`

This is the same lookup shape as the current `LogDirectoryBucket`, but on a much smaller aligned range.

#### Level 2: sealed 1M-log buckets

When the enclosing `LOG_DIRECTORY_BUCKET_SIZE` range seals, the crate may optionally write:

Metadata store:

- `log_dir/<bucket_start> -> LogDirectoryBucket`

This remains an immutable grouped summary built from sealed sub-buckets. It is an acceleration artifact, not the only authoritative source for that range.

### New stream index structures

The stream index layout is also two-level, with an optional third summary layer.

#### Level 0: stream frontier fragments

For every stream touched by a finalized block, ingest writes one immutable fragment per fixed page that the block contributes to:

Metadata store:

- `stream_frag_meta/<stream_id>/<page_start_local>/<block_num> -> StreamFragmentMeta`

Blob store:

- `stream_frag_blob/<stream_id>/<page_start_local>/<block_num> -> roaring bitmap fragment`

Recommended metadata payload:

- `count`
- `min_local`
- `max_local`

Recommended blob payload:

- roaring bitmap containing only the local IDs contributed by that one block within that one page

Properties:

- immutable after write
- directly queryable
- no manifest or tail rewrite required

#### Level 1: sealed stream pages

Partition each shard-local stream into fixed aligned local-ID pages.

When the shard-local frontier crosses a page boundary, the completed page may be compacted into:

Metadata store:

- `stream_page_meta/<stream_id>/<page_start_local> -> StreamPageMeta`

Blob store:

- `stream_page_blob/<stream_id>/<page_start_local> -> roaring bitmap page`

Recommended metadata payload:

- `count`
- `min_local`
- `max_local`

The page blob is the OR of all immutable frontier fragments for that page.

#### Level 2: optional sealed shard summary

Because shard-local ID space is fixed-width, a shard contains only a bounded number of fixed pages.

The crate may optionally write an immutable per-stream shard summary when the shard seals:

Metadata store:

- `stream_shard_summary/<stream_id> -> StreamShardSummary`

Recommended payload:

- one entry per fixed page, or a sparse list of non-empty pages
- per-page counts

This summary is only for planning and fast page existence checks. It is not required for correctness.

### Explicit removals from the current model

The new design removes these persisted objects from the logs family:

- `manifests/<stream_id>`
- `tails/<stream_id>`

The new design also removes the need for a mutable not-yet-sealed `log_dir/<bucket_start>`.

## Boundary Model

### Stream page boundary

Choose a fixed aligned local-ID page size:

- `STREAM_PAGE_LOCAL_ID_SPAN`

Examples:

- `4,096`
- `16,384`
- `65,536`

Properties:

- once the shard-local frontier moves beyond a page end, no future finalized block can add entries to that page
- the page is therefore sealed and safe to compact

This means each stream has at most one current open page in the current open shard.

### Directory sub-bucket boundary

Choose a fixed aligned log-ID sub-bucket size inside each 1M-log bucket:

- `LOG_DIRECTORY_SUB_BUCKET_SIZE`

Examples:

- `4,096`
- `16,384`
- `65,536`

Properties:

- once `next_log_id` moves beyond a sub-bucket end, no future finalized block can rewrite that sub-bucket
- the sub-bucket is therefore sealed and safe to compact

This means each 1M-log bucket has at most one current open sub-bucket.

## Ingest Flow

### Per-block artifacts

These writes stay the same:

- write `block_logs/<block_num>`
- write `block_log_headers/<block_num>`
- write `block_meta/<block_num>`
- write `block_hash_to_num/<block_hash>`

### Directory fragment publication

For block `N` with `first_log_id = X` and `count = C`:

1. compute every covered directory sub-bucket
2. write one immutable `log_dir_frag/<sub_bucket_start>/<block_num>` record for each covered sub-bucket
3. if any sub-bucket became completed because `next_log_id` crossed its end, compact that sub-bucket into `log_dir_sub/<sub_bucket_start>`
4. if the enclosing 1M bucket became completed, optionally compact its sealed sub-buckets into `log_dir/<bucket_start>`

No existing directory object is rewritten.

### Stream fragment publication

For each touched stream:

1. split the block’s local IDs by fixed page boundary
2. write one immutable `stream_frag_meta` + `stream_frag_blob` pair per touched page
3. if any page became completed because the shard-local frontier crossed its end, compact that page into `stream_page_meta` + `stream_page_blob`
4. if the shard became completed, optionally write `stream_shard_summary/<stream_id>`

No manifest or tail rewrite occurs.

### Lease and stale-writer protection

The active writer must hold a dedicated lease before publishing indexed artifacts or advancing `indexed_head`.

Required properties:

- lease acquire and renew use strict CAS / versioned conditional writes
- ordinary ingest writes do not pay per-write CAS cost
- ordinary ingest writes are still fenced by the active lease epoch so a stale writer cannot continue publishing after lease loss

The intended write classes are:

- lease writes:
  - infrequent
  - strict CAS / conditional
- ordinary artifact writes:
  - frequent
  - unconditional or lightweight writes, but fenced by current epoch
- indexed-head publish writes:
  - once per block
  - written last
  - fenced by current epoch

This keeps split-brain prevention strong without forcing every per-block or per-fragment write through heavyweight CAS.

One practical Scylla implementation is:

- one dedicated lease row acquired and renewed with LWT / CAS
- one fence epoch stored separately from the hot-path metadata rows
- ordinary writes validate their fence token against that epoch with a cached periodic refresh rather than a per-write LWT

That model is intentionally amortized-cheap rather than fully linearizable on every hot-path write. The correctness tradeoff is a bounded stale-writer detection window in exchange for avoiding per-write CAS cost.

### Publication ordering and failure semantics

Because the new frontier fragments are authoritative for the visible tip, `indexed_head` must not advance past a block until every authoritative artifact for that block has already been written successfully.

Required ordering for block `N`:

1. write `block_logs/<block_num>`
2. write `block_log_headers/<block_num>`
3. write `block_meta/<block_num>`
4. write `block_hash_to_num/<block_hash>`
5. write every authoritative `log_dir_frag/*` record for the block
6. write every authoritative `stream_frag_meta/*` and `stream_frag_blob/*` pair for the block
7. optionally write any derived compaction artifacts that were eagerly produced in the same step
8. only then write `indexed_head` with the new `indexed_finalized_head`

If any required authoritative write fails:

- the ingest step fails
- `indexed_head` must not advance

This is the publication rule that makes frontier fragments safe as the authoritative source for the latest visible finalized block.

For backend profiles with stronger visibility guarantees, the writer should also use a publication protocol strong enough that readers observing the new `indexed_head` can observe the authoritative frontier artifacts written before it. The exact backend-specific contract should follow the same pattern described in the sealing plan: publish dependent artifacts first, then publish the indexed head last.

### Compaction ordering

Compaction artifacts are derived from immutable source fragments.

That means compaction may be:

- eager in the ingest step
- deferred to a maintenance worker
- retried idempotently

Correctness does not depend on compaction finishing immediately, as long as the source fragments are already published and retained.

## Query Model

### Block-window to log-window resolution

This path stays unchanged:

- resolve finalized block range
- load `BlockMeta` for the endpoints
- derive the inclusive `log_id` window

The current `block_meta/<block_num>` records remain the authoritative source for this.

Readers only need `indexed_head` to know the highest block number that has been published. They do not need a persisted published `next_log_id`.

### Candidate stream loading

For a shard-local stream query:

1. determine the fixed page range overlapped by the requested local-ID range
2. for every sealed page in that range:
   - prefer `stream_page_meta` and `stream_page_blob` if present
3. for the current open page:
   - list `stream_frag_meta/<stream_id>/<page_start_local>/`
   - fetch only overlapping `stream_frag_blob` objects
   - OR those fragments together

If compaction is delayed for a sealed page, readers may fall back to the immutable `stream_frag_*` records for that page.

### `log_id -> block_num` materialization

For `log_id` resolution:

1. if the enclosing sealed 1M bucket `log_dir/<bucket_start>` exists, use it
2. otherwise, if the relevant sealed sub-bucket `log_dir_sub/<sub_bucket_start>` exists, use it
3. otherwise, list `log_dir_frag/<sub_bucket_start>/` for the current open sub-bucket
4. if that is still undesirable for the active tip frontier, fall back to bounded `block_meta` search for blocks in that open sub-bucket

After `block_num` is known, materialization continues exactly as it does now:

- load `block_log_headers/<block_num>`
- issue `read_range(block_logs/<block_num>, start, end)`
- decode one log

### Tip-frontier behavior

The frontier between the latest compacted boundary and the finalized tip is intentionally bounded.

For that bounded frontier, readers may:

- eagerly prefetch recent `BlockMeta`
- eagerly prefetch recent `BlockLogHeader`
- eagerly prefetch recent `block_logs/<block_num>` ranges or full blobs when locality is good
- eagerly prefetch recent `stream_frag_*` blobs for the active page

The important invariant is:

- direct recent-block reads are acceptable only because the ungrouped frontier is bounded by design

Compaction lag for already sealed regions is a separate operational concern. When lag exists, readers may need fragment-based fallback outside the live frontier. The architecture permits that for correctness, but production operation should keep the lag small enough that the common case still hits compacted summaries.

## Why This Removes Writer-Handoff Complexity

This design does not require the next writer to inherit any persisted mutable summary.

The next writer only needs:

- authoritative per-block finalized state
- the published `indexed_head` hint
- immutable frontier fragments already published by earlier writers

If the next writer wants process-local batching, it can rebuild that in-memory state from immutable inputs. No persisted mutable manifest, tail, or open bucket needs to be trusted or merged.

On takeover:

1. acquire the writer lease via strict CAS / versioned write
2. read the published `indexed_head` hint
3. load `block_meta/<indexed_head>` if the head is non-zero
4. derive local `next_log_id = first_log_id + count`
5. if the head hint may be stale, probe forward through `block_meta` continuity until the latest contiguous indexed block is found
6. continue ingest from that derived position

The authoritative source for deriving `next_log_id` is `BlockMeta`, not `block_log_headers`, because empty blocks may have no header object.

## Source Retention and GC

In this document, immutable frontier fragments remain the authoritative source of truth even after grouped summaries are written.

That means:

- `stream_page_*`, `log_dir_sub/*`, optional `log_dir/*`, and optional `stream_shard_summary/*` do not replace source fragments for correctness
- readers must always be allowed to fall back to the source fragments
- source-fragment deletion is out of scope for this plan

Deleting source fragments safely would require a separate publication and verification design that answers:

- how completeness of a grouped summary is proven
- how that proof is published
- how readers distinguish "summary is present" from "summary is complete and source replacement is now safe"
- how crashes during compaction or publication are recovered

Until that separate design exists, grouped summaries are acceleration-only and source fragments stay retained.

## Blob and Metadata Inventory

### Metadata store

Shared:

- `indexed_head`
- `block_meta/<block_num>`
- `block_hash_to_num/<block_hash>`
- `block_log_headers/<block_num>`

Lease / fencing:

- `writer_lease`

Directory:

- `log_dir_frag/<sub_bucket_start>/<block_num>`
- `log_dir_sub/<sub_bucket_start>`
- optional `log_dir/<bucket_start>`

Streams:

- `stream_frag_meta/<stream_id>/<page_start_local>/<block_num>`
- `stream_page_meta/<stream_id>/<page_start_local>`
- optional `stream_shard_summary/<stream_id>`

### Blob store

Payload:

- `block_logs/<block_num>`

Streams:

- `stream_frag_blob/<stream_id>/<page_start_local>/<block_num>`
- `stream_page_blob/<stream_id>/<page_start_local>`

## Observability

The implementation should measure:

- fragment writes
- page compactions
- sub-bucket compactions
- indexed-head publish latency
- lease acquire / renew latency and conflict count
- fallback reads that hit raw fragments instead of compacted summaries
- fallback reads that hit direct recent block metadata or payloads
- frontier width in blocks, log IDs, sub-buckets, and pages

These metrics are needed to validate that the frontier remains small enough and that compaction is keeping up.

## Tradeoffs

Benefits:

- no persisted mutable stream or directory summaries
- simpler writer handoff semantics
- immutable artifacts are naturally cacheable
- compaction is retryable and not correctness-critical
- strict CAS is paid for lease ownership, not for every hot-path metadata write

Costs:

- more small objects in the open frontier
- more reliance on deterministic fixed window sizing
- tip queries may need bounded direct block reads before higher-level summaries exist
- compaction lag still needs operational control even though correctness does not depend on immediate compaction
- source-fragment GC is intentionally deferred to a later design
- writers still need a lease/fence mechanism, and stale-writer rejection depends on correct fencing behavior

## Open Parameters

The architecture depends on tuning choices, not open correctness questions:

- `STREAM_PAGE_LOCAL_ID_SPAN`
- `LOG_DIRECTORY_SUB_BUCKET_SIZE`
- whether frontier stream fragments are one object per block or batched per small ingest bundle
- whether compacted page blobs should inline directly into metadata for very small pages
- whether sealed 1M `log_dir` buckets are required immediately or can remain optional acceleration
- whether shard summaries are sparse or dense

## Implementation Order

### Phase 1

- add immutable stream frontier fragments
- add immutable directory fragments
- teach queries to read fragments directly for the bounded frontier
- keep existing sealed-state readers for old data during migration if needed

### Phase 2

- add sealed stream page compaction
- add sealed directory sub-bucket compaction
- switch readers to prefer compacted summaries when present

### Phase 3

- add optional sealed 1M-log bucket compaction
- add optional per-stream shard summaries
- define compaction SLOs and observability targets for keeping sealed-region fallback rare

### Deferred follow-on

- design source-fragment GC only after grouped-summary publication and completeness proofs are specified

## Relationship To Other Plans

This document is intended to replace the mutable-state assumptions in:

- `docs/plans/boundary-sealing-architecture.md`

It also changes the cache design inputs described in:

- `docs/plans/metadata-caching-architecture.md`

Under this model, cross-request caching should focus on immutable fragments and immutable grouped summaries rather than on mutable manifests or tails.
