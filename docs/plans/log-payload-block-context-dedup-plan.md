# Log Payload Block-Context Dedup Plan

## Summary

This document proposes a small focused refactor to remove block-level fields from the persisted per-log payload bytes.

Today each encoded log stores:

- `block_num`
- `block_hash`

Those fields are redundant with the surrounding storage layout:

- `block_num` is derivable from the block-keyed blob being read
- `block_hash` is derivable from `block_meta/<block_num>`

The intended end state is:

- persist only per-log fields in the log payload blob
- inject block context during materialization
- keep the public `Log` type unchanged
- reduce blob storage and range-read bytes per log

## Goals

- remove duplicated block-level bytes from persisted log payloads
- preserve external query semantics and returned `Log` shape
- keep the refactor small and independent from larger storage redesign work
- make materialization explicitly responsible for reconstructing full `Log`

## Non-Goals

- changing `query_logs` semantics
- redesigning block-keyed payload storage
- changing `BlockMeta` or `BlockLogHeader`
- redesigning locator or directory structures

## Current State

The current `encode_log` format in [codec/log.rs](/home/jhow/roaring-monad/crates/finalized-history-query/src/codec/log.rs) stores:

- `address`
- `topic_count`
- `topics`
- `data_len`
- `data`
- `block_num`
- `tx_idx`
- `log_idx`
- `block_hash`

That means every stored log duplicates:

- `8` bytes of `block_num`
- `32` bytes of `block_hash`

Total removable overhead:

- `40` bytes per log

Current encoded size formula:

```text
73 + 32 * topic_count + data_len
```

Proposed encoded size formula:

```text
33 + 32 * topic_count + data_len
```

Examples:

- `2` topics, `32` bytes data:
  - current `169`
  - proposed `129`
- `3` topics, `64` bytes data:
  - current `233`
  - proposed `193`
- `4` topics, `128` bytes data:
  - current `329`
  - proposed `289`

## Proposed Model

### Persisted payload

Introduce a persisted payload type conceptually equivalent to:

```rust
struct StoredLogPayload {
    address: [u8; 20],
    topics: Vec<[u8; 32]>,
    data: Vec<u8>,
    tx_idx: u32,
    log_idx: u32,
}
```

This is the exact information that varies per log inside one block blob.

### Materialized query object

Keep the public query object as the existing full `Log`:

```rust
struct Log {
    address: [u8; 20],
    topics: Vec<[u8; 32]>,
    data: Vec<u8>,
    block_num: u64,
    tx_idx: u32,
    log_idx: u32,
    block_hash: [u8; 32],
}
```

Materialization becomes:

1. resolve `log_id -> block_num, local_ordinal`
2. load `BlockLogHeader`
3. range-read one encoded `StoredLogPayload` from `block_logs/<block_num>`
4. decode `StoredLogPayload`
5. load or reuse `BlockMeta` / `BlockRef` for `block_num`
6. construct full `Log` by injecting:
   - `block_num`
   - `block_hash`

## Why This Is Safe

`block_num` and `block_hash` are already authoritative elsewhere:

- `block_num` comes from the block-keyed blob and the log-ID resolution path
- `block_hash` comes from `block_meta/<block_num>`

Removing those fields from the payload does not remove information from the system. It only stops storing it redundantly per log.

## Code Shape

### Codec changes

Refactor [codec/log.rs](/home/jhow/roaring-monad/crates/finalized-history-query/src/codec/log.rs) so the persisted codec is explicit about payload-only bytes.

Recommended direction:

- add `encode_stored_log_payload(...)`
- add `decode_stored_log_payload(...)`
- keep `encode_log(...)` only if still needed by tests or transition helpers

### Type changes

Add a small internal-only type for the persisted payload, for example:

- `StoredLogPayload`

Do not change the public `Log` type in this refactor.

### Materializer changes

Update [logs/materialize.rs](/home/jhow/roaring-monad/crates/finalized-history-query/src/logs/materialize.rs) so it:

- decodes the stored payload
- injects `block_num`
- injects `block_hash`

The existing block-ref cache can continue to help with repeated lookups in the same block.

One likely improvement is to add a small cached `BlockMeta` or `block_hash` lookup in the materializer so repeated logs from the same block do not repeatedly decode the same metadata.

## Storage and Performance Impact

Expected storage reduction:

- `40` bytes per log in `block_logs/<block_num>`

Expected effects:

- lower blob storage footprint
- lower byte-range read volume for point materialization
- slightly more work in materialization because block context is injected rather than decoded directly

That tradeoff is favorable because block context is shared per block while payload bytes are repeated per log.

## Compatibility / Migration

This refactor changes the on-disk log payload encoding.

Because this project does not require production backward compatibility yet, the preferred path is:

- change the codec directly
- update tests and fixtures
- treat existing data as disposable / reindexable

If mixed old/new data ever becomes necessary, add a version byte and dual decoder only then.

## Implementation Steps

1. introduce `StoredLogPayload`
2. split payload-only codec helpers from full `Log` construction
3. update block-log encoding on ingest to persist payload-only bytes
4. update materialization to inject block context from block-level metadata
5. update codec and end-to-end tests

## Verification

Verify at least:

- codec round-trips for stored payload bytes
- ingest/query end-to-end tests still return the same `Log` values
- storage-oriented assertions where useful:
  - encoded payload length shrinks by `40` bytes relative to the old format for the same sample log

## Relationship To Other Plans

This is a small local optimization/refactor that fits cleanly under the current block-keyed log storage design in:

- `docs/04-block-keyed-log-storage.md`

It is independent of the larger immutable-frontier indexing plan.
