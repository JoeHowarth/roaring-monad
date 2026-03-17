# Cleanup Taxonomy And Execution

## Summary

This document defines the concrete cleanup taxonomy and execution
boundaries for `crates/finalized-history-query`.

The goal is to make recovery and cleanup behavior implementation-ready by
answering four questions precisely:

1. what artifact classes exist
2. which phase owns each class
3. when deletion is safe
4. which cleanup is automatic versus operator-driven

This document is a concrete follow-on to
`docs/plans/recovery-gc-and-maintenance.md`.

## Goal

Define an implementation-ready cleanup model for startup recovery,
periodic maintenance, and GC that is consistent with the current storage
model and publication semantics.

## Design Constraints

- `publication_state.indexed_finalized_head` is the only reader-visible
  publication barrier
- authoritative artifacts for blocks `<= indexed_finalized_head` must
  never be deleted by automatic cleanup
- compaction summaries are acceleration artifacts, not the only source of
  truth for frontier data
- `open_stream_page/*` is inventory state, not reader-visible data
- all metadata deletions must remain fence-aware
- cleanup must be idempotent when retried

## Cleanup Phases

## 1. Startup Recovery

Purpose:

- restore a clean write frontier before new ingest begins

Trigger:

- writer acquires or re-authorizes ownership during startup

Properties:

- correctness-critical
- synchronous with writer startup
- must be safe under replay

Current code already does:

- delete unpublished blocks above the published head
- delete unpublished directory and stream-page summaries for the same
  unpublished suffix
- repair stale sealed open-page markers

## 2. Periodic Maintenance

Purpose:

- perform bounded hygiene work during steady-state operation that should
  not require full startup recovery

Trigger:

- explicit service maintenance call while a writer lease is valid

Properties:

- not required to establish basic correctness at startup
- bounded work budget per invocation
- may be retried or skipped without corrupting visible state

Current code:

- placeholder only

## 3. Garbage Collection

Purpose:

- audit and clean long-lived debris or inventories that should not grow
  without bound

Trigger:

- explicit GC run while a writer lease is valid

Properties:

- background debt-management and policy enforcement
- may report-only for some classes before automatic deletion is adopted
- owns guardrail measurement

Current code:

- placeholder stats plus `prune_block_hash_index_below(...)`

## Artifact Classes

The cleanup model should classify artifacts into three groups.

## A. Authoritative Data-Path Artifacts

These are required to serve published data correctly.

Examples:

- `block_logs/<block_num>`
- `block_log_headers/<block_num>`
- `block_meta/<block_num>`
- `block_hash_to_num/<block_hash>`
- `log_dir_frag/<sub_bucket_start>/<block_num>`
- `stream_frag_meta/<stream_id>/<page_start>/<block_num>`
- `stream_frag_blob/<stream_id>/<page_start>/<block_num>`

Deletion rule:

- may be deleted automatically only when they are above the published
  head and therefore unpublished
- must never be deleted automatically once they may contribute to visible
  published data

Owner:

- startup recovery for unpublished suffix cleanup

## B. Acceleration Artifacts

These improve read performance but are not the only source of truth for
the frontier zone.

Examples:

- `log_dir_sub/<sub_bucket_start>`
- optional `log_dir/<bucket_start>`
- `stream_page_meta/<stream_id>/<page_start>`
- `stream_page_blob/<stream_id>/<page_start>`

Deletion rule:

- may be deleted automatically if they summarize only unpublished work
- may be recomputed from retained source fragments if missing or stale
- should not be deleted automatically once they summarize visible ranges,
  unless the system has an explicit rebuild strategy and can tolerate the
  performance cost

Owner:

- startup recovery for unpublished summary cleanup
- GC only if a future rebuild-and-reclaim policy is adopted explicitly

## C. Inventory And Control-Plane Artifacts

These exist to coordinate writes, compaction, or operations.

Examples:

- `publication_state`
- `open_stream_page/<shard>/<page_start>/<stream_id>`

Deletion rule:

- `publication_state` is never a cleanup target
- `open_stream_page/*` may be deleted automatically once the page is
  sealed and compaction has succeeded

Owner:

- startup recovery for stale sealed markers present at startup
- periodic maintenance for bounded marker cleanup during steady state

## Ownership Matrix

| Cleanup class | Startup recovery | Periodic maintenance | GC | Operator-driven |
|---|---|---|---|---|
| Unpublished authoritative artifacts above published head | Yes | No | No | No |
| Unpublished acceleration artifacts for the same suffix | Yes | No | No | No |
| Stale sealed `open_stream_page/*` markers | Yes | Yes | No | No |
| Deferred bounded inventory hygiene | No | Yes | No | No |
| Long-lived cleanup debt / debris measurement | No | No | Yes | No |
| Old but valid published indexes such as block-hash lookup entries | No | No | Optional reporting | Yes |

## Safe Deletion Rules

## Rule 1. Publication state is the only visibility boundary

Automatic cleanup must determine "unpublished" solely from
`publication_state.indexed_finalized_head`, not from inferred local
progress.

## Rule 2. Delete unpublished summaries before unpublished authoritative blocks

When cleaning an unpublished suffix:

1. discover unpublished blocks above the published head
2. derive the log-id span of that unpublished suffix
3. delete summaries that cover only that unpublished span
4. delete unpublished authoritative artifacts in reverse block order

This matches the current startup cleanup logic and should remain the
canonical ordering.

## Rule 3. Treat source fragments as authoritative for frontier recovery

Directory and stream-page summaries may be deleted safely for unpublished
ranges because source fragments remain available for recovery or rebuild.

## Rule 4. Marker deletion requires completed sealing

`open_stream_page/*` markers are only safe to delete after the page is
sealed and the relevant page compaction has either:

- already been written successfully, or
- is being written successfully in the same cleanup flow

## Rule 5. Published authoritative data is retained by default

This design does not introduce automatic TTL-based deletion for
published authoritative data-path artifacts.

Any future retention or pruning policy for published data must be an
explicit operator or product decision, not an incidental GC behavior.

## Automatic GC Versus Operator Pruning

The cleanup model should distinguish three cases.

### Automatic cleanup

Use for:

- unpublished suffix artifacts
- unpublished summaries
- stale sealed open-page markers
- future clearly-orphaned debris classes that cannot affect visible data

### Operator-driven pruning

Use for:

- old but still valid published helper indexes where removal is a policy
  decision rather than a correctness repair

Current example:

- `prune_block_hash_index_below(min_block_num)`

### Retain indefinitely by default

Use for:

- published authoritative data-path artifacts
- published acceleration artifacts unless a future rebuild policy is
  explicitly adopted

## Recommended Execution Boundaries

## Startup Recovery Responsibilities

- discover unpublished suffix from `block_meta`
- derive unpublished summary coverage from the unpublished log-id span
- delete unpublished summaries
- delete unpublished authoritative block artifacts in reverse order
- seal-and-remove stale open-page markers whose pages are now sealed

## Periodic Maintenance Responsibilities

- scan a bounded number of open-page markers
- for markers that now refer to sealed pages:
  - compact the page if needed
  - delete the marker
- report counts of pages sealed and markers removed

Recommendation:

- keep periodic maintenance focused on inventory and bounded frontier
  hygiene
- do not make it responsible for wide historical scanning

## GC Responsibilities

- measure cleanup debt classes
- optionally delete clearly orphaned or stale non-visible classes
- expose guardrail stats
- keep operator-pruning tasks separate from automatic GC decisions even
  if implemented in the same worker type

Recommendation:

- keep `prune_block_hash_index_below(...)` available as an explicit
  operator path
- do not treat it as the model for general-purpose automatic GC

## Reporting And Guardrails

The current GC guardrail names do not match the current storage model
well. Implementation should rename or remap them to current cleanup
classes before relying on them operationally.

Recommended stats categories:

- unpublished_suffix_blocks
- unpublished_summary_objects
- stale_open_page_markers
- operator_prunable_block_hash_entries
- orphan_acceleration_objects

Recommended policy split:

- startup recovery reports what it removed
- maintenance reports bounded work completed
- GC reports measured debt, automatic deletions, and guardrail breaches

## Open Questions

## 1. Should periodic maintenance compact sealed pages proactively or only repair stale markers?

Possible answers:

- A. Only repair markers that already exist
- B. Also proactively scan for newly sealed pages without relying on
  marker presence
- C. Move all page sealing work to GC

Recommendation:

- A for the first implementation

Why:

- it keeps periodic maintenance bounded
- it matches the current inventory model
- it avoids turning maintenance into a wide historical scan

## 2. Should GC automatically delete published acceleration artifacts that can be rebuilt?

Possible answers:

- A. Never; published acceleration artifacts remain until an explicit
  future retention policy exists
- B. Yes, once they are older than a TTL
- C. Yes, when storage-pressure guardrails are exceeded

Recommendation:

- A for now

Why:

- the current crate has no rebuild budget model, no TTL model, and no
  explicit reader-performance fallback policy for such reclamation

## 3. Should block-hash index pruning live inside GC or as a distinct operator API?

Possible answers:

- A. Keep it as a GC worker method invoked explicitly by operators
- B. Move it into periodic maintenance
- C. Add automatic age-based pruning in GC

Recommendation:

- A

Why:

- the current API already expresses pruning as an explicit boundary-based
  action
- it is policy-driven, not correctness-repair-driven

## 4. How much startup cleanup should be synchronous before the writer is considered ready?

Possible answers:

- A. All unpublished suffix cleanup and stale sealed marker repair
- B. Only unpublished suffix cleanup; marker repair may defer
- C. Only detect cleanup debt at startup and defer all deletion

Recommendation:

- A for correctness-sensitive startup behavior

Why:

- the current model expects a clean write frontier before new ingest
- stale sealed markers can interfere with later compaction behavior if
  left behind

## Implementation Order

1. codify the cleanup taxonomy in code and tests
2. align `GcStats` and config guardrails with real cleanup classes
3. implement bounded periodic maintenance for stale sealed marker repair
4. implement real GC discovery/reporting for debt classes
5. keep operator pruning explicit and separate from automatic GC policy

## Relationship To Other Plans

- aligns with `docs/plans/recovery-gc-and-maintenance.md`
- should inform the cleanup portions of
  `docs/plans/correctness-verification-matrix.md`
- should feed startup-cost and cleanup-budget expectations into
  `docs/plans/performance-capacity-and-deployment.md`
