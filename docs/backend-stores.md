# Backend Stores

This document describes the store trait abstractions and their implementations.

## MetaStore

The `MetaStore` trait provides key-value storage with conditional writes and fencing:

```rust
pub trait MetaStore: Send + Sync {
    async fn get(&self, key: &[u8]) -> Result<Option<Record>>;
    async fn put(&self, key: &[u8], value: Bytes, cond: PutCond, fence: FenceToken) -> Result<PutResult>;
    async fn delete(&self, key: &[u8], cond: DelCond, fence: FenceToken) -> Result<()>;
    async fn list_prefix(&self, prefix: &[u8], cursor: Option<Vec<u8>>, limit: usize) -> Result<Page>;
}
```

- `Record { value: Bytes, version: u64 }` — every stored value carries a monotonic version
- `PutCond` — `Any`, `IfAbsent`, or `IfVersion(u64)` for conditional writes
- `DelCond` — `Any` or `IfVersion(u64)` for conditional deletes
- `FenceToken(u64)` — epoch-based fencing; operations with a token below the stored minimum epoch return `LeaseLost`

## BlobStore

The `BlobStore` trait provides unversioned object storage with range reads:

```rust
pub trait BlobStore: Send + Sync {
    async fn put_blob(&self, key: &[u8], value: Bytes) -> Result<()>;
    async fn put_blob_if_absent(&self, key: &[u8], value: Bytes) -> Result<CreateOutcome>;
    async fn get_blob(&self, key: &[u8]) -> Result<Option<Bytes>>;
    async fn read_range(&self, key: &[u8], start: u64, end_exclusive: u64) -> Result<Option<Bytes>>;
    async fn delete_blob(&self, key: &[u8]) -> Result<()>;
    async fn list_prefix(&self, prefix: &[u8], cursor: Option<Vec<u8>>, limit: usize) -> Result<Page>;
}
```

`read_range` has a default implementation that loads the full blob and slices locally. Backends with native range-read support can override this.

## PublicationStore + FenceStore

Separate traits for publication-state CAS and fence management:

```rust
pub trait PublicationStore: Send + Sync {
    async fn load(&self) -> Result<Option<PublicationState>>;
    async fn create_if_absent(&self, initial: &PublicationState) -> Result<CasOutcome<PublicationState>>;
    async fn compare_and_set(&self, expected: &PublicationState, next: &PublicationState) -> Result<CasOutcome<PublicationState>>;
}

pub trait FenceStore: Send + Sync {
    async fn advance_fence(&self, min_epoch: u64) -> Result<()>;
    async fn current_fence(&self) -> Result<u64>;
}
```

`CasOutcome` is either `Applied(T)` or `Failed { current: Option<T> }`.

## InMemoryMetaStore / InMemoryBlobStore

Test doubles backed by in-memory collections.

- `InMemoryMetaStore` — `BTreeMap<Vec<u8>, Record>` behind `RwLock`, implements `MetaStore` + `PublicationStore` + `FenceStore`
- `InMemoryBlobStore` — `HashMap<Vec<u8>, Bytes>` behind `RwLock`, implements `BlobStore`

Fence enforcement uses `AtomicU64` for the minimum epoch.

## FsMetaStore / FsBlobStore

File-system backed stores for local development and testing.

- Keys are hex-encoded into file paths under a `meta/` or `blob/` root directory
- Keys are grouped by the prefix before the first `/` (e.g., `block_meta/...` goes into the `block_meta` group directory)
- Versions are stored in `.ver` sidecar files
- `F_NOCACHE` (`fcntl(F_NOCACHE, 1)`) is set on all file I/O on macOS to avoid polluting the OS page cache
- `PublicationStore` CAS operations are serialized via a per-path mutex to prevent races
- Atomic writes use temp-file + rename for crash safety

Implements `MetaStore` + `PublicationStore` + `FenceStore` (meta) and `BlobStore` (blob).

## ScyllaMetaStore

Scylla (Cassandra-compatible) implementation for `MetaStore` + `PublicationStore` + `FenceStore`.

### Schema

Two tables:

- `meta_kv (grp text, bucket smallint, k blob, v blob, version bigint, PRIMARY KEY ((grp, bucket), k))` — main key-value store
- `meta_fence (id text PRIMARY KEY, min_epoch bigint)` — fence state

### Key partitioning

Keys are partitioned by:

1. **Group** — derived from the key prefix before the first `/` (e.g., `block_meta`, `log_dir_sub`)
2. **Bucket** — FNV-1a hash of the full key modulo 256

This distributes load across partitions while keeping related keys in the same group for efficient prefix listing.

### CAS operations

`PutCond::IfAbsent` uses Scylla's `IF NOT EXISTS` lightweight transactions. `PutCond::IfVersion` uses `IF version = ?` LWT. Both track CAS attempts and conflicts in telemetry.

### Fence validation

The fence is checked against a cached `min_epoch` value. The cached value is refreshed from Scylla at most once per `fence_check_interval_ms` (default: 1000ms) to avoid excessive reads.

### Retry policy

Retryable errors (timeout, connection, unavailable, overloaded) use exponential backoff with configurable `max_retries`, `base_delay_ms`, and `max_delay_ms`.

## MinioBlobStore

S3-compatible object storage implementation for `BlobStore`.

- Objects are stored under `<object_prefix>/<group>/<hex_key>`
- Bucket is auto-created if it doesn't exist
- `put_blob_if_absent` is not implemented (returns `Unsupported`)
- `list_prefix` uses S3 `ListObjectsV2` with continuation tokens
- Retryable errors use the same exponential backoff pattern as Scylla
