# Backend Stores

This document describes the store trait abstractions and their implementations.

## MetaStore

The `MetaStore` trait provides family-scoped key-value storage with conditional
writes:

```rust
pub struct FamilyId(&'static str);

pub trait MetaStore: Send + Sync {
    async fn get(&self, family: FamilyId, key: &[u8]) -> Result<Option<Record>>;
    async fn put(
        &self,
        family: FamilyId,
        key: &[u8],
        value: Bytes,
        cond: PutCond,
    ) -> Result<PutResult>;
    async fn delete(&self, family: FamilyId, key: &[u8], cond: DelCond) -> Result<()>;
    async fn list_prefix(
        &self,
        family: FamilyId,
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page>;
}
```

The generic storage boundary also exposes family-scoped handles such as
`KvTable<M>` and `KvTableRef<'_, M>` so higher-level code can bind a family once
and then operate only on suffix keys.

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
`publication_state` metadata family on top of any `MetaStore`.

## InMemoryMetaStore / InMemoryBlobStore

Test doubles backed by in-memory collections.

- `InMemoryMetaStore` — `BTreeMap<(FamilyId, Vec<u8>), Record>` behind `RwLock`, implements `MetaStore`
- `InMemoryBlobStore` — `HashMap<Vec<u8>, Bytes>` behind `RwLock`, implements `BlobStore`

## FsMetaStore / FsBlobStore

File-system backed stores for local development and testing.

- Keys are hex-encoded into file paths under a `meta/` or `blob/` root directory
- Metadata family names map to directories under `meta/<family>/`
- Metadata files are keyed by the hex-encoded suffix bytes within that family
- Versions are stored in `.ver` sidecar files
- `F_NOCACHE` (`fcntl(F_NOCACHE, 1)`) is set on all file I/O on macOS to avoid polluting the OS page cache

Implements `MetaStore` (meta) and `BlobStore` (blob).

## ScyllaMetaStore

Scylla (Cassandra-compatible) implementation for `MetaStore`.

### Schema

Two metadata tables are created:

- `meta_kv (grp text, bucket smallint, k blob, v blob, version bigint, PRIMARY KEY ((grp, bucket), k))` — main key-value store
- `meta_fence (id text PRIMARY KEY, min_epoch bigint)` — auxiliary compatibility/operations state

### Key partitioning

Keys are partitioned by:

1. **Group** — the explicit metadata family (for example `block_record` or `log_dir_sub_bucket`)
2. **Bucket** — FNV-1a hash of the suffix key modulo 256

This distributes load across partitions while keeping related keys in the same
family for efficient family-local prefix listing.

### CAS operations

`PutCond::IfAbsent` uses Scylla's `IF NOT EXISTS` lightweight transactions.
`PutCond::IfVersion` uses `IF version = ?` LWT. Both track CAS attempts and
conflicts in telemetry. Normal artifact writes now use unconditional puts on the
write path; conditional writes remain for publication-state CAS and any callers
that still need them through `MetaPublicationStore`.

### Retry policy

Retryable errors (timeout, temporary, connection, reset, refused, unavailable, overloaded, connection-setup failure) use exponential backoff with configurable `max_retries`, `base_delay_ms`, and `max_delay_ms`.

## MinioBlobStore

S3-compatible object storage implementation for `BlobStore`.

- Objects are stored under `<object_prefix>/<group>/<hex_key>`
- Bucket is auto-created if it doesn't exist
- `list_prefix` uses S3 `ListObjectsV2` with continuation tokens
- Retryable errors use the same exponential backoff pattern as Scylla
