# Writer Lease Publication Gap

## Summary

The immutable-frontier implementation introduced a persisted `writer_lease` record, but visible publication still does not depend on proving ownership of that lease at publish time.

Today, stale-writer rejection still comes from the existing fence-token mechanism in the storage layer, not from the new `writer_lease` record itself.

The active design follow-up for fixing this is:

- `docs/plans/publication-and-write-authority.md`

## Current Behavior

The ingest path in [`crates/finalized-history-query/src/ingest/engine.rs`](/home/jhow/roaring-monad/crates/finalized-history-query/src/ingest/engine.rs) currently does this:

1. writes `writer_lease` with `PutCond::Any`
2. writes authoritative artifacts for the block
3. advances `indexed_head` with the existing head-row CAS

That means:

- `writer_lease` is written as advisory metadata
- `indexed_head` publication does not verify that the caller still owns the lease it wrote
- the design goal in `docs/plans/completed/immutable-frontier-index-architecture.md` is not fully implemented

## Why This Matters

The architecture document intended a stronger split:

- lease ownership decides who may publish
- `indexed_head` decides what readers may see

The current code only partially achieves that split.

If the project wants the lease record to be meaningful for correctness, the final publication step must reject stale ownership based on the lease protocol itself, not just on the older fence-token behavior.

## What Is Still Protecting The System

This is not an immediate loss of stale-writer protection.

The storage layer still validates `FenceToken(epoch)` on metadata writes, so stale-writer rejection continues to rely on the existing fence/min-epoch mechanism in:

- [`crates/finalized-history-query/src/store/meta.rs`](/home/jhow/roaring-monad/crates/finalized-history-query/src/store/meta.rs)
- [`crates/finalized-history-query/src/store/fs.rs`](/home/jhow/roaring-monad/crates/finalized-history-query/src/store/fs.rs)
- [`crates/finalized-history-query/src/store/scylla.rs`](/home/jhow/roaring-monad/crates/finalized-history-query/src/store/scylla.rs)

So the current state is:

- fence-based stale-writer protection exists
- lease-record-based publication control does not

## Required End-State Properties

A real lease/publication model should guarantee:

1. lease acquisition is conditional and exclusive
2. lease renewal preserves a monotonically increasing ownership epoch
3. ordinary artifact writes are fenced by that epoch
4. the final `indexed_head` publication step proves the writer still owns the active lease
5. readers treat `indexed_head` as the only visibility watermark

## Straightforward Fixes That Are Not Enough

These do not solve the problem by themselves:

- writing `writer_lease` more often
- changing `PutCond::Any` to a plain version CAS on the lease row without tying publication to it
- keeping the current `indexed_head` CAS but also storing lease metadata nearby

The missing piece is a publish-time ownership check.

## Plausible Implementation Directions

Possible directions:

1. make `writer_lease` a real conditional lease row and require `indexed_head` publication to validate the current lease epoch
2. introduce a dedicated publication fence row whose epoch is set by the lease owner and must match during `indexed_head` advancement
3. stage block artifacts under the lease epoch and publish only via a lease-validated commit step

The right choice depends on the backend-specific conditional-write guarantees and the intended handoff model.

## Recommended Follow-Up

Before treating `writer_lease` as complete architecture work:

1. define the exact lease acquire / renew / lose protocol
2. define the publish-time validation rule for `indexed_head`
3. add crash and takeover tests that specifically exercise lease loss between artifact writes and final publication
