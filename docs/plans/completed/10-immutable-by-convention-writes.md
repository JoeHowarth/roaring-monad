# Immutable By Convention Writes

## Summary

This plan removes storage-enforced immutability from artifact writes and
replaces it with an immutable-by-convention model.

The target model is:

- artifact keys are intended to be immutable
- blob and metadata stores use unconditional writes on the normal path
- correctness relies on deterministic/idempotent rewrites plus
  `publication_state` CAS for visibility
- stale unpublished writes are tolerated
- rollout discipline, not backend rejection, protects against
  incompatible semantic changes

This is a simplification plan, not a production runbook. The stricter
operational discipline only matters once the system is deployed.

## Work Packages

### 1. Remove Conditional Create Requirements

- remove create-if-absent requirements from artifact write helpers
- switch normal artifact writes to unconditional writes
- simplify blob-store adapters that only exist to enforce immutability
  at write time

### 2. Make The Convention Explicit

Document the intended invariant:

- within one artifact version/keyspace, rewrites are expected to be
  deterministic and reader-compatible
- reusing the same key for incompatible semantics is not allowed

### 3. Preserve Publication As The Only Visibility Boundary

Keep `publication_state.indexed_finalized_head` as the only
reader-visible publication boundary.

Readers must continue to trust only published state, not the mere
existence of artifacts above the published head.

### 4. Gate Incompatible Changes Operationally

For any incompatible persisted-format or interpretation change, do not
reuse the same artifact semantics under the same write rollout.

Instead, use a read-compatible, write-gated rollout:

1. roll out readers that can read both old and new formats while
   writers still write the old format
2. once that rollout is complete, stop writers
3. start a writer on the new version
4. start the remaining standby writers on the new version

## Changes To Gate

Once deployed, the following changes must be treated as gated
compatibility changes:

- byte-level layout changes for persisted metadata or blobs
- fragment/by-block to summary/page function changes
- key layout or naming changes
- shard, page, bucket, or range-boundary interpretation changes
- publication or visibility rule changes
- recovery interpretation changes
- query-semantics changes that reinterpret persisted history

## Exit Criteria

- normal artifact writes no longer depend on storage-enforced
  create-if-absent semantics
- the code and docs clearly describe immutability as a convention rather
  than a backend-enforced guarantee
- publication remains the only visibility boundary
- incompatible changes have an explicit rollout-gating policy

