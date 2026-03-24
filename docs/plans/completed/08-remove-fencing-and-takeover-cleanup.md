# Remove Fencing And Takeover Cleanup

## Summary

This plan removes artifact-write fencing and takeover cleanup from the
codebase completely.

The target model is simpler:

- `publication_state.indexed_finalized_head` is the only reader-visible
  publication boundary
- stale unpublished writes are tolerated
- immutable artifacts and summary artifacts above the published head are
  harmless junk until or unless they become published by the active
  writer
- takeover does not need to fence old writers before ordinary metadata
  writes
- takeover does not need to clean unpublished suffix artifacts before
  resuming ingest

This discipline is not required yet for current non-production use. It
only becomes an operational requirement once the system is deployed.

## Work Packages

### 1. Remove Artifact-Write Fencing

- remove `FenceToken` from ordinary metadata writes/deletes
- remove `FenceStore` from the main write path
- remove backend fence advancement and cached fence checks
- delete tests that exist only to prove stale-writer rejection on
  artifact writes

After this change, `publication_state` CAS/LWT remains the only
ownership and visibility control.

### 2. Remove Takeover Cleanup

- remove unpublished-suffix cleanup on takeover/startup
- remove takeover-specific stale-marker cleanup that only exists to
  restore a clean suffix before reuse
- keep only the startup/recovery work that is still required under the
  publication-only model

The new invariant is that unpublished artifacts can remain in storage
without affecting correctness.

### 3. Tighten The Artifact Contract

Document and enforce the assumptions that justify removing fencing and
cleanup:

- immutable per-block artifacts are idempotent
- summary artifacts are immutable
- summary artifacts are pure functions of their authoritative immutable
  inputs
- readers trust only the published head
- unpublished artifacts are ignored unless later published through the
  normal head-advance path

### 4. Rework Tests Around Publication-Only Safety

Replace fence/cleanup-oriented tests with tests that prove the simpler
model:

- stale unpublished writes do not affect visible reads
- takeover without cleanup still converges correctly
- repeated summary writes are harmless
- restart and retry work without requiring suffix deletion

## Change Classes To Gate

Once this system is deployed, the following changes must be treated as
write-gated compatibility changes:

- byte-level layout changes for persisted metadata or blobs
- fragment-to-summary function changes
- key layout or naming changes
- shard, page, bucket, or range-boundary interpretation changes
- publication or visibility rule changes
- recovery interpretation changes
- query-semantics changes that reinterpret persisted history

## Rollout Policy

Use a read-compatible, write-gated rollout for gated changes:

1. roll out readers that can read both old and new formats while
   writers still write the old format
2. once that rollout is complete, stop writers
3. start a writer on the new version
4. start the remaining standby writers on the new version

This is the default operational model for persisted-format or
interpretation changes once production deployment exists.

