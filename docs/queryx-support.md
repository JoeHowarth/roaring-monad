# QueryX Support

Read this after [reference/queryX](reference/queryX) and before
[overview.md](overview.md).

This doc explains how `finalized-history-query` maps onto the `queryX`
product shape: what this crate already provides, what still belongs in the RPC
service and transport layer, and how the current substrate is meant to grow.

## Purpose

The reference `queryX` doc defines a broader RPC product shape than this crate
currently exposes:

- `eth_queryBlocks`
- `eth_queryTransactions`
- `eth_queryLogs`
- `eth_queryTraces`
- `eth_queryTransfers`

This crate is not trying to implement that whole RPC surface directly. It is
the storage/query substrate underneath that surface:

- one shared finalized-history runtime
- one shared finalized block envelope for ingest
- one shared publication and pagination model
- one concrete family registry for `logs`, `txs`, and `traces`
- family-owned schema, indexing, and materialization logic

The RPC crate remains responsible for transport concerns such as JSON-RPC
request parsing, tag resolution policy, field selection syntax, joins and
response envelopes, and error mapping.

## Where We Are Today

The current crate already has much of the core substrate shape that the
reference document wants:

- a concrete logs query API
- a shared `FinalizedBlock` ingest envelope
- one service-owned runtime and typed table bundle
- one ingest coordinator that validates block continuity once and publishes
  once per batch
- one concrete `Families { logs, txs, traces }` registry
- logs and traces as real family implementations

Current family status:

- `logs`: real family, with storage layout, codecs, indexing, query execution,
  materialization, and ingest
- `txs`: scaffold family slot only
- `traces`: real family, with trace-owned storage, indexing, query execution,
  materialization, and ingest

The current public surface is still narrower than the reference:

- logs and traces only
- finalized history only
- no descending traversal
- no field selection
- no relation joins
- no tag-based range inputs on the transport-free surface

That is acceptable. The important thing is that the crate now has a shared
multi-family substrate instead of a logs-only architecture.

## Target Shape

The intended long-term split is:

- shared substrate:
  - range resolution
  - pagination and cursor metadata
  - finalized-head/publication logic
  - service-owned runtime and caches
  - shared ingest coordination
- family-owned logic:
  - filter semantics
  - family-local schema
  - family-local indexes
  - materialization
  - zero-copy field extraction
  - ingest behavior

That maps naturally onto the reference `queryX` model:

- blocks, transactions, logs, traces, and transfers all share a common query
  envelope
- each family decides how to index, filter, and materialize its own primary
  objects
- related-object joins and field selection are layered above the family
  materializers rather than baked into one giant generic object model

## Family Outlook

### Logs

Logs are already close to the intended end state.

They have:

- family-owned schema and codecs
- immutable artifacts
- indexed clause execution
- exact-match materialization
- shared pagination and publication behavior

Logs are the reference implementation for the other families.

### Transactions

Transactions are the hardest family.

The difficulty is not the query model itself. The difficulty is that Ethereum
transactions come in multiple envelope variants, and a storage-conscious design
cannot afford to persist both:

- the full authoritative transaction bytes
- a second full decoded transaction object

At long-term throughput, even small per-transaction duplication adds up to many
terabytes per year.

The design goal for transactions is therefore:

- store the full transaction exactly once
- index only the fields needed for query/filtering
- materialize requested fields from the authoritative bytes

### Traces

Traces are now a concrete family implementation rather than a placeholder.

They currently have:

- raw per-block `trace_rlp` blob storage plus compact trace headers
- trace-owned directory and bitmap index artifacts
- trace query execution and exact-match materialization
- shared pagination and publication behavior

Remaining trace work is additive product/runtime expansion rather than basic
family bring-up.

### Transfers

Transfers are expected to be a derived family rather than a fundamentally new
storage problem.

The main questions are:

- whether they are derived from traces, transaction execution metadata, or both
- whether they deserve their own physical artifacts or are materialized from
  another family

That makes transfers more of a product-modeling question than a storage-format
question.

## Transaction Blob Strategy

Transactions should be stored in a block- or chunk-scoped raw blob, similar in
spirit to the logs payload blob plus offset header pattern.

The authoritative transaction storage should look like:

- one raw transaction blob per block or chunk
- one compact offset/length header for random access
- optional small block/chunk metadata for counts and lookup

This should be the only full-fidelity transaction storage.

The crate should not persist a second full decoded transaction record alongside
that blob.

Instead, transaction-specific persisted state should be limited to:

- block/chunk metadata
- query indexes
- any tiny sidecars that are proven necessary for performance

Examples of transaction indexes:

- `from`
- `to`
- `selector`
- `tx_hash` if direct lookup is needed

If one field turns out to be too expensive to recover repeatedly, it can be
persisted as a small sidecar. That should be the exception, not the default.

## Transaction Parsing And Extraction

Even in the storage-conscious design above, the transaction blob still has to
be parsed in order to build indexes and answer queries.

The intended approach is:

- parse raw transaction bytes at ingest time to build indexes
- store the raw bytes once as the authoritative artifact
- materialize requested fields from the raw bytes at query time

The recommended parser shape is a family-owned zero-copy extractor layer.

### Zero-Copy Transaction Extractors

The transaction family should not begin with a large fully-owned enum that
reconstructs every transaction into a heavyweight in-memory object.

Instead, it should use borrowed views over the raw transaction bytes:

- an outer dispatcher that identifies the transaction envelope variant
- per-variant zero-copy extractors that expose borrowed field views
- small helpers that normalize family-relevant fields across variants

Conceptually:

```rust
enum TxView<'a> {
    Legacy(LegacyTxView<'a>),
    Eip2930(Eip2930TxView<'a>),
    Eip1559(Eip1559TxView<'a>),
    Eip4844(Eip4844TxView<'a>),
}
```

Each variant view should expose only what the family needs:

- `nonce`
- `to`
- `value`
- `input`
- `selector`
- fee fields when required
- transaction type
- signature fields when sender recovery is required

The extractor should branch on the transaction variant once, then provide a
uniform family-facing interface over the borrowed view.

That preserves the desirable properties:

- one authoritative raw representation
- no second full persisted decoded object
- no unnecessary owned allocations during query materialization
- clear handling of multiple transaction variants

## Dependency Stance

The preferred end state is for `finalized-history-query` to own a small
family-specific extractor layer rather than adopting a heavyweight external
transaction domain model throughout the crate.

That means:

- keep the crate's stored schema and query schema independent of external
  transaction library types
- keep parsing concerns isolated to the transaction family
- avoid making the rest of the crate depend on a large typed transaction stack

If an external library is used, it should stay at the ingest/parser edge and
convert into the transaction family's own compact internal indexing and
materialization structures. It should not become the storage model.

## Practical Transaction Plan

The transaction family should be built in this order:

1. define the authoritative raw transaction blob and offset header format
2. define the minimal transaction indexes needed for the first query shape
3. implement zero-copy variant dispatch and borrowed extractors over the raw
   bytes
4. use the extractors at ingest time to populate indexes
5. use the same extractors at query time to materialize only the requested
   fields
6. add small persisted sidecars only when profiling shows they are necessary

This avoids committing early to either:

- duplicated full decoded storage
- a heavyweight external typed transaction model

## Implications For The Shared Substrate

The shared substrate should continue to assume as little as possible about the
family payloads.

The family boundary should support:

- family-owned state derived from the published head
- family-owned per-block ingest behavior
- family-owned filter/index/materialization logic
- family-owned zero-copy extraction over authoritative blobs

The shared layer should not grow transaction-specific parsing logic.

## Summary

The reference `queryX` document remains the product destination.

`finalized-history-query` is moving toward being the shared finalized-history
engine underneath that destination:

- logs are the first full family
- txs are the hardest remaining family because of multiple transaction
  envelope variants and storage pressure
- traces and transfers are expected to be structurally simpler

For transactions, the intended design is:

- authoritative raw blob storage
- minimal persisted indexes
- zero-copy per-variant extractors over raw bytes
- query-time field materialization from those borrowed views

That gives the crate a path to support the reference query model without paying
the long-term storage cost of persisting both raw and fully decoded
transactions.
