# Indexed Query And Log ID Architecture

## Summary

This document records the completed query-execution architecture for
`crates/finalized-history-query`.

The landed design has three linked decisions:

- global log IDs are explicit typed values (`LogId`, `LogShard`, `LogLocalId`)
- indexed execution keeps shard-local roaring structure until materialization
- queries execute shard-by-shard in ascending `log_id` order instead of loading full-window clause
  state up front

Together these changes give the current behavior:

- exact ascending pagination
- exact `next_resume_log_id`
- exact `has_more`
- bounded per-request clause/intersection state
- correct shard handling for the configured 24-bit local ID layout

## Landed Shape

### Typed log identities

The internal query and ingest path uses:

- `LogId`
- `LogShard`
- `LogLocalId`
- `PrimaryIdRange`

This fixes the old shard-width truncation bug and centralizes compose/split logic.

The public query boundary still uses primitive `u64` for resume-token fields, with typed conversion
at the internal boundary.

### Shard-aware candidate execution

Indexed execution keeps roaring bitmaps shard-local until the point where concrete candidate IDs
must be materialized.

Conceptually:

1. resolve block window
2. map to `log_id` window
3. visit overlapping shards in ascending order
4. load and intersect clause bitmaps only for the current shard
5. materialize candidates in ascending `log_id` order
6. stop once `limit + 1` matches are found

This replaced the older whole-window load/intersect shape.

### Public semantics preserved

The landed architecture kept:

- existing `query_logs` semantics
- `log_id` as the pagination identity
- exact `cursor_block`
- exact `next_resume_log_id`
- exact `has_more`

Non-indexed queries continue to follow the current rejection / guardrail behavior rather than
falling back to a broad scan.

## Why These Changes Belong Together

The newtypes and the shard-streaming executor were not separate independent improvements.

They were one coherent query-architecture shift:

- typed IDs made shard-local reasoning explicit and safe
- shard-aware roaring execution preserved compact set operations
- shard-streaming traversal turned that representation into lower-latency exact pagination

That is why these two older completed plans are now replaced by this single completed summary.

## Relationship To Other Plans

- The immutable artifact cache plan that complements this query architecture is
  `docs/plans/completed/zero-copy-types-and-bytes-cache.md`.
- Historical step-by-step planning docs for this work now live under `docs/plans/superceded/`.
