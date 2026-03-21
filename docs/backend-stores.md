# Backend Stores

This document describes the store trait abstractions and their implementations.

## MetaStore

The `MetaStore` trait provides two metadata access shapes:

- point tables keyed by `table + key`
- scannable tables keyed by `table + partition + clustering`

The core trait supports both:

```rust
pub struct TableId(&'static str);
pub struct ScannableTableId(&'static str);

pub trait MetaStore: Send + Sync {
    async fn get(&self, table: TableId, key: &[u8]) -> Result<Option<Record>>;
    async fn put(
        &self,
        table: TableId,
        key: &[u8],
        value: Bytes,
        cond: PutCond,
    ) -> Result<PutResult>;
    async fn delete(&self, table: TableId, key: &[u8], cond: DelCond) -> Result<()>;

    async fn scan_get(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
    ) -> Result<Option<Record>>;
    async fn scan_put(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
        value: Bytes,
        cond: PutCond,
    ) -> Result<PutResult>;
    async fn scan_delete(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
        cond: DelCond,
    ) -> Result<()>;
    async fn scan_list(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page>;
}
```

The generic storage boundary also exposes table-scoped handles:

- `KvTable<M>` / `KvTableRef<'_, M>` for point tables
- `ScannableKvTable<M>` / `ScannableKvTableRef<'_, M>` for scannable tables

Higher-level code binds a table once, then works only with suffix keys or with
explicit `(partition, clustering)` components.

- `Record { value: Bytes, version: u64 }` — every stored value carries a monotonic version
- `PutCond` — `Any`, `IfAbsent`, or `IfVersion(u64)` for conditional writes
- `DelCond` — `Any` or `IfVersion(u64)` for conditional deletes

## BlobStore

The `BlobStore` trait provides unversioned object storage with range reads:

```rust
pub trait BlobStore: Send + Sync {
    async fn put_blob(&self, key: &[u8], value: Bytes) -> Result<()>;
    async fn get_blob(&self, key: &[u8]) -> Result<Option<Bytes>>;
    async fn read_range(&self, key: &[u8], start: u64, end_exclusive: u64) -> Result<Option<Bytes>>;
    async fn delete_blob(&self, key: &[u8]) -> Result<()>;
    async fn list_prefix(&self, prefix: &[u8], cursor: Option<Vec<u8>>, limit: usize) -> Result<Page>;
}
```

`read_range` has a default implementation that loads the full blob and slices locally. Backends with native range-read support can override this.

Normal artifact writes use unconditional blob puts. Immutability is a
convention enforced by writer behavior and rollout discipline rather
than a blob-store create-if-absent requirement.

## PublicationStore

Separate trait for publication-state CAS:

```rust
pub trait PublicationStore: Send + Sync {
    async fn load(&self) -> Result<Option<PublicationState>>;
    async fn create_if_absent(&self, initial: &PublicationState) -> Result<CasOutcome<PublicationState>>;
    async fn compare_and_set(&self, expected: &PublicationState, next: &PublicationState) -> Result<CasOutcome<PublicationState>>;
}
```

`CasOutcome` is either `Applied(T)` or `Failed { current: Option<T> }`.

Backends no longer implement `PublicationStore` directly. The crate provides a
`MetaPublicationStore<M>` wrapper that stores publication state in the
`publication_state` metadata table on top of any `MetaStore`.

Scannable tables are only used for metadata tables that actually enumerate:

- `log_dir_by_block`
- `bitmap_by_block`
- `open_bitmap_page`

All other metadata tables stay on the simpler point-table shape.

## InMemoryMetaStore / InMemoryBlobStore

Test doubles backed by in-memory collections.

- `InMemoryMetaStore` — point records in `BTreeMap<(TableId, Vec<u8>), Record>` and scannable records in `BTreeMap<(ScannableTableId, Vec<u8>, Vec<u8>), Record>` behind `RwLock`
- `InMemoryBlobStore` — `HashMap<Vec<u8>, Bytes>` behind `RwLock`, implements `BlobStore`

## FsMetaStore / FsBlobStore

File-system backed stores for local development and testing.

- Keys are hex-encoded into file paths under a `meta/` or `blob/` root directory
- Metadata table names map to directories under `meta/<table>/`
- Metadata files are keyed by the hex-encoded suffix bytes within that table
- Scannable metadata tables live under `meta_scan/<table>/<hex_partition>/<hex_clustering>`
- Versions are stored in `.ver` sidecar files
- `F_NOCACHE` (`fcntl(F_NOCACHE, 1)`) is set on all file I/O on macOS to avoid polluting the OS page cache

Implements `MetaStore` (meta) and `BlobStore` (blob).

## ScyllaMetaStore

Scylla (Cassandra-compatible) implementation for `MetaStore`.

### Schema

Three tables are created:

- `meta_kv (grp text, bucket smallint, k blob, v blob, version bigint, PRIMARY KEY ((grp, bucket), k))` — point metadata table
- `meta_scan_kv (grp text, pk blob, ck blob, v blob, version bigint, PRIMARY KEY ((grp, pk), ck))` — scannable metadata table
- `meta_fence (id text PRIMARY KEY, min_epoch bigint)` — auxiliary compatibility/operations state

### Key partitioning

Point-table keys are partitioned by:

1. **Group** — the explicit logical metadata table (for example `block_record` or `log_dir_sub_bucket`)
2. **Bucket** — FNV-1a hash of the suffix key modulo 256

This distributes load across partitions for point lookups and conditional
writes.

Scannable-table rows are partitioned by `(table, partition)` and ordered by the
clustering key. This lets the backend support exact lookup and ordered scans for
the actual enumerable metadata tables without the old table-wide bucket
fanout.

### CAS operations

`PutCond::IfAbsent` uses Scylla's `IF NOT EXISTS` lightweight transactions.
`PutCond::IfVersion` uses `IF version = ?` LWT. Both point and scannable tables
track CAS attempts and conflicts in telemetry. Normal artifact writes now use
unconditional puts on the write path; conditional writes remain for
publication-state CAS, open-page markers, and any callers that still need them
through `MetaPublicationStore`.

### Retry policy

Retryable errors (timeout, temporary, connection, reset, refused, unavailable, overloaded, connection-setup failure) use exponential backoff with configurable `max_retries`, `base_delay_ms`, and `max_delay_ms`.

## MinioBlobStore

S3-compatible object storage implementation for `BlobStore`.

- Objects are stored under `<object_prefix>/<group>/<hex_key>`
- Bucket is auto-created if it doesn't exist
- `list_prefix` uses S3 `ListObjectsV2` with continuation tokens
- Retryable errors use the same exponential backoff pattern as Scylla
