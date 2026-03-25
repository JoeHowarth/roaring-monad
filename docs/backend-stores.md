# Backend Stores

Read this after [storage-model.md](storage-model.md) and
[write-authority.md](write-authority.md).

This doc describes the store trait abstractions and their implementations.

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

The `BlobStore` trait provides table-scoped unversioned object storage with
range reads:

```rust
pub struct BlobTableId(&'static str);

pub trait BlobStore: Send + Sync {
    async fn put_blob(&self, table: BlobTableId, key: &[u8], value: Bytes) -> Result<()>;
    async fn get_blob(&self, table: BlobTableId, key: &[u8]) -> Result<Option<Bytes>>;
    async fn read_range(
        &self,
        table: BlobTableId,
        key: &[u8],
        start: u64,
        end_exclusive: u64,
    ) -> Result<Option<Bytes>>;
    async fn delete_blob(&self, table: BlobTableId, key: &[u8]) -> Result<()>;
    async fn list_prefix(
        &self,
        table: BlobTableId,
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page>;
}
```

The storage boundary also exposes `BlobTable<B>` / `BlobTableRef<'_, B>` so
higher-level code can bind a blob table once and then work with suffix keys.

`read_range` has a default implementation that loads the full blob and slices locally. Backends with native range-read support can override this. Those partial-read fast paths are not required to perform end-to-end payload verification.

`get_blob` integrity is guaranteed at the `BlobStore` boundary. Backends verify
full-object bytes before returning them with native checksum facilities when
available or with side metadata when not. Blob values remain
backend-transparent: implementations store and return the exact artifact bytes
rather than injecting checksum headers into the payload format.

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
- `tx_dir_by_block`
- `tx_bitmap_by_block`
- `tx_open_bitmap_page`
- `trace_dir_by_block`
- `trace_bitmap_by_block`
- `trace_open_bitmap_page`

All other metadata tables stay on the simpler point-table shape.

## InMemoryMetaStore / InMemoryBlobStore

Test doubles backed by in-memory collections.

- `InMemoryMetaStore` — point records in `BTreeMap<(TableId, Vec<u8>), Record>` and scannable records in `BTreeMap<(ScannableTableId, Vec<u8>, Vec<u8>), Record>` behind `RwLock`
- `InMemoryBlobStore` — `HashMap<(BlobTableId, Vec<u8>), Bytes>` behind `RwLock`, implements `BlobStore`

## FsMetaStore / FsBlobStore

File-system backed stores for local development and testing.

- Keys are hex-encoded into file paths under a `meta/` or `blob/` root directory
- Metadata table names map to directories under `meta/<table>/`
- Metadata files are keyed by the hex-encoded suffix bytes within that table
- Scannable metadata tables live under `meta_scan/<table>/<hex_partition>/<hex_clustering>`
- Blob tables map to directories under `blob/<table>/`
- Versions are stored in `.ver` sidecar files
- Blob integrity uses sidecar checksum metadata so reads reject corrupted blob contents
- By default the filesystem store uses normal buffered I/O on macOS, matching other platforms
- Enabling the `macos-fs-nocache` crate feature sets `F_NOCACHE` (`fcntl(F_NOCACHE, 1)`) on all file I/O handles on macOS to avoid polluting the OS page cache

Implements `MetaStore` (meta) and `BlobStore` (blob).

## ScyllaMetaStore

Scylla (Cassandra-compatible) implementation for `MetaStore`.

### Schema

Each logical metadata table becomes its own physical Scylla table. Point tables
use:

- `<table_name> (bucket smallint, k blob, v blob, version bigint, PRIMARY KEY ((bucket), k))`

Scannable tables use:

- `<table_name> (pk blob, ck blob, v blob, version bigint, PRIMARY KEY ((pk), ck))`

The backend currently creates physical tables for:

- point tables: `publication_state`, `block_header`, `block_record`, `block_log_header`, `block_tx_header`, `block_hash_index`, `tx_hash_index`, `log_dir_bucket`, `log_dir_sub_bucket`, `tx_dir_bucket`, `tx_dir_sub_bucket`, `bitmap_page_meta`, `tx_bitmap_page_meta`, `block_trace_header`, `trace_dir_bucket`, `trace_dir_sub_bucket`, `trace_bitmap_page_meta`
- scannable tables: `log_dir_by_block`, `bitmap_by_block`, `open_bitmap_page`, `tx_dir_by_block`, `tx_bitmap_by_block`, `tx_open_bitmap_page`, `trace_dir_by_block`, `trace_bitmap_by_block`, `trace_open_bitmap_page`
- auxiliary state: `meta_fence (id text PRIMARY KEY, min_epoch bigint)`

### Key partitioning

Point-table rows are partitioned by:

1. **Table** — one physical Scylla table per logical metadata table
2. **Bucket** — FNV-1a hash of the suffix key modulo 256

This distributes load across partitions for point lookups and conditional
writes.

Scannable tables are keyed by the natural scan scope of each logical table:

- `log_dir_by_block`: partition = `sub_bucket_start`, clustering = `block_num`
- `bitmap_by_block`: partition = `(stream_id, page_start_local)`, clustering = `block_num`
- `open_bitmap_page`: partition = `shard`, clustering = `(page_start_local, stream_id)`
- `tx_dir_by_block`: partition = `tx_sub_bucket_start`, clustering = `block_num`
- `tx_bitmap_by_block`: partition = `(stream_id, page_start_local)`, clustering = `block_num`
- `tx_open_bitmap_page`: partition = `shard`, clustering = `(page_start_local, stream_id)`
- `trace_dir_by_block`: partition = `trace_sub_bucket_start`, clustering = `block_num`
- `trace_bitmap_by_block`: partition = `(stream_id, page_start_local)`, clustering = `block_num`
- `trace_open_bitmap_page`: partition = `shard`, clustering = `(page_start_local, stream_id)`

This lets the backend support exact lookup and ordered scans without a shared
generic scan table or family-wide fanout.

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

- Objects are stored under `<object_prefix>/<table>/<hex_key>`
- Bucket is auto-created if it doesn't exist
- Puts request S3-managed object checksums, `get_blob` enables checksum validation, and `read_range` uses native partial reads without full-object verification
- `list_prefix` uses S3 `ListObjectsV2` with continuation tokens
- Retryable errors use the same exponential backoff pattern as Scylla
