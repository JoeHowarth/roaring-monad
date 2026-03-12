# Zero-Copy Types and Bytes-Aware Cache

## Summary

Replace the current decode-into-owned-struct pattern with zero-copy reference types
that hold a `Bytes` handle and provide accessor methods over the underlying buffer.
Introduce a type-unaware bytes cache that stores raw `Bytes` values, enabling
trivial cloning (Arc refcount bump), transparent compression, and a future path
to arena-backed allocation.

This plan is a refinement of the caching strategy described in
`docs/plans/metadata-caching-architecture.md`. That document defines the cache
engine shape, table partitioning, miss coordination, and family facades. This
document focuses on the representation layer beneath it: what the cache stores,
how types are constructed from cached bytes, and how compression fits between
storage and cache.

## Goals

- eliminate heap allocation on the decode path for all high-traffic read types
- make cache entries trivially cloneable (Arc increment, not deep copy)
- keep the cache engine type-unaware so it stores only `Bytes` values with
  caller-supplied weights
- enable transparent compression between the storage backend and the cache
  without affecting zero-copy access patterns
- preserve the existing wire format — no migration required
- design for a future arena-backed allocator without requiring it now

## Non-Goals

- changing the on-disk/wire encoding format
- implementing arena allocation in this phase
- implementing mutable tip caching (covered by the metadata caching plan)
- changing query semantics, pagination, or API response types
- adding alignment constraints to the storage format

## Motivation

### Current decode path

The current flow allocates at every layer:

```
Store returns Bytes (Arc-backed, zero-copy slicing available)
    ↓
&[u8] passed to decode_*()
    ↓
decode_log():           Vec::with_capacity for topics, .to_vec() for data
decode_block_log_header():  Vec::with_capacity for offsets
decode_log_directory_bucket():  Vec::with_capacity for first_log_ids
    ↓
Owned struct stored in HashMap cache
    ↓
.cloned() on every cache hit (deep copy of inner Vecs)
```

For `BlockLogHeader` with 200 offsets, each cache hit clones 800 bytes of Vec
data plus the Vec header. For `LogDirectoryBucket` with 10,000 entries, each
cache hit clones 80 KB. For `Log`, topics and data are heap-copied on every
materialization even though `read_range` already returned a zero-copy `Bytes`
slice of exactly those bytes.

### Proposed decode path

```
Store returns Bytes (Arc-backed)
    ↓
decompress if needed (one allocation into new Bytes)
    ↓
cache.put(key, Bytes)  — stores the decompressed buffer
    ↓
cache.get(key) → Bytes  — Arc::clone, no data copy
    ↓
BlockLogHeaderRef::new(bytes)?  — validates, stores Bytes + precomputed count
    ↓
ref.offset(i) → u32  — reads directly from buffer, no allocation
```

Cache hits become an atomic refcount increment. Type construction becomes
validation only. Field access becomes arithmetic indexing into the buffer.

## Design

### Zero-Copy Reference Types

Each reference type holds a `Bytes` handle plus a small number of precomputed
values extracted during validation. Accessors read directly from the buffer.

Since `Bytes` is reference-counted (not lifetime-bounded), these types are
`Clone + Send + Sync` without lifetime parameters.

#### LogRef

```rust
/// Zero-copy view over an encoded log record.
///
/// Wire layout:
///   address:     [u8; 20]
///   topic_count: u8
///   topics:      topic_count * [u8; 32]
///   data_len:    u32 BE
///   data:        data_len bytes
///   block_num:   u64 BE
///   tx_idx:      u32 BE
///   log_idx:     u32 BE
///   block_hash:  [u8; 32]
pub struct LogRef {
    buf: Bytes,
    topic_count: u8,
    data_offset: u32,
    data_len: u32,
    // Offset to the block_num field (after data).
    trailer_offset: u32,
}
```

Construction (`LogRef::new(bytes: Bytes) -> Result<LogRef>`) performs the same
validation as the current `decode_log`, but stores computed offsets instead of
copying data. All validation happens upfront so accessors are infallible.

Accessors:

```rust
impl LogRef {
    pub fn address(&self) -> &[u8; 20];
    pub fn topic_count(&self) -> usize;
    pub fn topic(&self, i: usize) -> &[u8; 32];  // panics if i >= topic_count
    pub fn topics(&self) -> impl Iterator<Item = &[u8; 32]>;
    pub fn data(&self) -> &[u8];
    pub fn block_num(&self) -> u64;
    pub fn tx_idx(&self) -> u32;
    pub fn log_idx(&self) -> u32;
    pub fn block_hash(&self) -> &[u8; 32];
}
```

Each accessor is a bounds-checked slice plus optional `from_be_bytes` for
integer fields. No heap allocation.

#### BlockLogHeaderRef

```rust
/// Zero-copy view over an encoded block log header.
///
/// Wire layout:
///   version: u8 (must be 1)
///   count:   u32 BE
///   offsets: count * u32 BE
pub struct BlockLogHeaderRef {
    buf: Bytes,
    count: u32,
}
```

Accessors:

```rust
impl BlockLogHeaderRef {
    pub fn count(&self) -> usize;
    pub fn offset(&self, i: usize) -> u32;  // panics if i >= count
    pub fn offsets(&self) -> impl Iterator<Item = u32>;
}
```

The `offset(i)` method reads `u32::from_be_bytes` at position `5 + i * 4`.
This is one unaligned load instruction — negligible cost compared to the
cache-miss or I/O that produced the bytes.

#### LogDirectoryBucketRef

```rust
/// Zero-copy view over an encoded log directory bucket.
///
/// Wire layout:
///   version:       u8 (must be 1)
///   start_block:   u64 BE
///   count:         u32 BE
///   first_log_ids: count * u64 BE
pub struct LogDirectoryBucketRef {
    buf: Bytes,
    start_block: u64,
    count: u32,
}
```

Accessors:

```rust
impl LogDirectoryBucketRef {
    pub fn start_block(&self) -> u64;
    pub fn count(&self) -> usize;
    pub fn first_log_id(&self, i: usize) -> u64;
    pub fn first_log_ids(&self) -> impl Iterator<Item = u64>;

    /// Binary search over the first_log_ids array.
    /// Used by containing_bucket_entry to find which block owns a log_id.
    pub fn partition_point(&self, f: impl Fn(u64) -> bool) -> usize;
}
```

The `partition_point` method enables the existing bucket lookup logic without
materializing a `Vec<u64>`. It performs binary search by reading `u64` values
directly from the buffer at computed offsets.

#### Types not converted

These types remain owned structs because the zero-copy benefit is negligible:

- **`BlockMeta`**: fixed 76 bytes, all stack-copied arrays and integers.
  No inner `Vec`, no deep-clone cost. Keeping it owned is simpler.
- **`LogDirFragment`**: fixed 25 bytes, three integers. Same rationale.
- **`StreamBitmapMeta`**: fixed 21 bytes, four integers.
- **`PublicationState`**: fixed 49 bytes, infrequently read, mutable.

These fixed-size types can still benefit from the bytes cache (avoiding
redundant store reads), but the decode step is so cheap that a zero-copy
wrapper adds complexity without meaningful performance gain.

### Bytes-Aware Cache

The cache stores `Bytes` values keyed by opaque byte keys. It does not know
about `LogRef`, `BlockLogHeaderRef`, or any domain type. This is consistent
with the engine design in the metadata caching architecture plan.

```rust
pub trait BytesCache: Send + Sync {
    /// Returns a cached value if present. Cloning Bytes is an Arc increment.
    fn get(&self, table: TableId, key: &[u8]) -> Option<Bytes>;

    /// Inserts a value with its weight in bytes for capacity accounting.
    fn put(&self, table: TableId, key: &[u8], value: Bytes, weight: usize);
}
```

`TableId` is a lightweight enum identifying the logical cache table (e.g.,
`BlockLogHeaders`, `LogDirectoryBuckets`, `BlockLogBlobs`). This preserves the
per-table capacity budgets and metrics defined in the metadata caching plan.

The initial implementation is a sharded `HashMap<(TableId, Vec<u8>), Bytes>`
with LRU eviction per table. The in-flight miss coordination described in the
metadata caching plan applies here unchanged — the cache engine deduplicates
concurrent fetches for the same `(table, key)` pair.

#### Why bytes-only, not typed values

Storing `Bytes` instead of decoded types means:

1. **One cache shape** — the engine stores one value type regardless of how many
   domain types exist. No generics, no trait objects, no `Any` downcasting.
2. **Trivial clone** — `Bytes::clone()` is an atomic refcount increment.
   No deep copy regardless of the logical type or its size.
3. **Compression-compatible** — the cache stores decompressed bytes. The
   decompression boundary is between storage and cache, not between cache and
   caller. Zero-copy types read directly from the decompressed buffer.
4. **Arena-compatible** — a future arena allocator produces `Bytes` handles
   (backed by `Arc<Vec<u8>>` arena chunks). The cache interface is unchanged.
5. **Weight accounting is natural** — the weight of a `Bytes` entry is
   `bytes.len()`, which is exact and trivially computed.

### Integration with LogMaterializer

The `LogMaterializer` currently owns five `HashMap` caches. Under the new
design, it receives a `&BytesCache` reference and constructs zero-copy views
on demand:

```rust
pub struct LogMaterializer<'a, M: MetaStore, B: BlobStore> {
    meta_store: &'a M,
    blob_store: &'a B,
    cache: &'a dyn BytesCache,
    range_resolver: RangeResolver,
    // Per-request caches removed for types served by BytesCache.
    // block_ref_cache remains because BlockRef is a small Copy type
    // computed from multiple sources, not a direct decode of stored bytes.
    block_ref_cache: HashMap<u64, BlockRef>,
}
```

Load methods become:

```rust
async fn load_block_header(&mut self, block_num: u64) -> Result<Option<BlockLogHeaderRef>> {
    let key = block_log_header_key(block_num);
    if let Some(bytes) = self.cache.get(TableId::BlockLogHeaders, &key) {
        return Ok(Some(BlockLogHeaderRef::new(bytes)?));
    }
    let Some(record) = self.meta_store.get(&key).await? else {
        return Ok(None);
    };
    self.cache.put(
        TableId::BlockLogHeaders,
        &key,
        record.value.clone(),
        record.value.len(),
    );
    Ok(Some(BlockLogHeaderRef::new(record.value)?))
}
```

The `PrimaryMaterializer` trait's associated type changes from `Log` to `LogRef`:

```rust
impl<M: MetaStore, B: BlobStore> PrimaryMaterializer for LogMaterializer<'_, M, B> {
    type Primary = LogRef;
    type Filter = LogFilter;

    async fn load_by_id(&mut self, id: LogId) -> Result<Option<LogRef>> { ... }
    async fn block_ref_for(&mut self, item: &LogRef) -> Result<BlockRef> { ... }
    fn exact_match(&self, item: &LogRef, filter: &LogFilter) -> bool { ... }
}
```

The `exact_match` function adapts to use accessor methods instead of field
access. The `block_ref_for` method reads `block_num()` and `block_hash()` via
accessors.

### API Boundary

The public `QueryPage<Log>` return type becomes `QueryPage<LogRef>`. Since
`LogRef` is `Clone + Send + Sync` and provides the same data through accessors,
callers adapt by changing field access to method calls.

If a downstream consumer requires an owned `Log` (e.g., for serialization into
a different format), `LogRef` provides a `to_owned()` method that performs the
allocation:

```rust
impl LogRef {
    pub fn to_owned(&self) -> Log {
        Log {
            address: *self.address(),
            topics: self.topics().copied().collect(),
            data: self.data().to_vec(),
            block_num: self.block_num(),
            tx_idx: self.tx_idx(),
            log_idx: self.log_idx(),
            block_hash: *self.block_hash(),
        }
    }
}
```

This makes the zero-copy path opt-out rather than opt-in. Internal query
execution never allocates; external serialization allocates only when needed.

### Transparent Compression

Compression sits between the storage backend and the cache. The cache always
stores decompressed bytes so that zero-copy accessors work directly.

```
BlobStore.get_blob(key)
    → compressed Bytes (from disk/network)
    → decompress into new Bytes (one allocation per cache miss)
    → cache.put(key, decompressed_bytes)

cache.get(key)
    → Bytes (Arc clone, zero-copy)
    → LogRef::new(bytes) (validation only)
    → accessor methods (direct buffer reads)
```

#### Where decompression lives

Decompression belongs in the store layer, not the cache layer. The store
implementations (`FsBlobStore`, `S3BlobStore`, `ScyllaBlobStore`) handle
compression internally:

```rust
// Inside a blob store implementation
async fn get_blob(&self, key: &[u8]) -> Result<Option<Bytes>> {
    let raw = self.backend_read(key).await?;
    match raw {
        Some(bytes) => Ok(Some(decompress(bytes)?)),
        None => Ok(None),
    }
}

async fn put_blob(&self, key: &[u8], value: Bytes) -> Result<()> {
    let compressed = compress(&value)?;
    self.backend_write(key, compressed).await
}
```

This way the cache, zero-copy types, and all callers above the store layer
never see compressed data. The compression format is a storage implementation
detail.

#### Codec choice

LZ4 (via `lz4_flex`) is the recommended codec:

- 3-4 GB/s decompression throughput
- negligible CPU cost per cache miss
- 2-4x compression ratio on log blobs (repeated address and topic bytes)
- no framing overhead for small buffers (use block mode, not frame mode)

For block_logs blobs (the largest stored objects), the savings are material:
a 100 KB blob compresses to ~30-50 KB on disk/network, then decompresses once
into the cache and serves zero-copy reads from there indefinitely.

For small metadata (block headers, directory buckets), compression is optional.
The store can skip compression for values below a threshold (e.g., 256 bytes)
to avoid the overhead on already-small payloads. This is a per-store-impl
decision, not a cache concern.

### Alignment

Zero-copy access to integer fields uses `u32::from_be_bytes` /
`u64::from_be_bytes` on byte slices copied to stack-aligned locals. This works
at any alignment and compiles to a single unaligned load instruction on x86-64
and a short sequence on ARM.

No alignment constraints are imposed on the storage format, cache buffers, or
arena chunks. The existing packed big-endian wire format is preserved as-is.

If a future optimization requires aligned access (e.g., SIMD scanning over
large offset arrays), that would require:

- native-endian storage (breaking wire format change)
- aligned arena chunk allocation
- neither is justified by current workloads

The recommendation is to not pursue alignment-aware storage. The `from_be_bytes`
path is well within performance requirements.

### Future: Arena-Backed Allocation

The `Bytes`-only cache interface is designed to accommodate arena allocation
as a drop-in replacement without changing the ref type implementations or
the cache trait.

#### Design sketch

```rust
struct TableArena {
    /// One chunk per sealed block range (e.g., per 10K blocks).
    /// Each chunk is a contiguous allocation holding many cache entries.
    chunks: BTreeMap<u64, Arc<ArenaChunk>>,
}

struct ArenaChunk {
    data: Vec<u8>,
    // Entry index: maps key hash → (offset, len) within data
    entries: HashMap<u64, (u32, u32)>,
}
```

An arena entry is returned as `Bytes::from(arc_chunk.clone()).slice(offset..offset+len)`,
which shares the arena's single Arc allocation. This gives:

- **One allocation per chunk** instead of one per entry
- **Cache-line locality** for sequential access patterns (e.g., iterating
  block headers for consecutive blocks)
- **Bulk eviction** — dropping a chunk frees all its entries at once, which
  matches the append-only / evict-old-ranges access pattern of finalized data

The `BytesCache` trait is unchanged. The arena is an implementation detail of
a specific `BytesCache` impl. Zero-copy ref types receive `Bytes` handles
regardless of whether the backing store is per-entry `Bytes` or arena slices.

#### When to pursue

Arena allocation is worth pursuing when profiling shows that:

- per-entry `Bytes` overhead (32 bytes per handle + Arc header per entry) is
  a meaningful fraction of cache memory
- cache-line misses from scattered allocations measurably affect scan
  throughput on hot directory bucket lookups

Until then, the standard `Bytes`-per-entry approach is simpler and sufficient.

## Migration Strategy

### Encoding functions unchanged

The `encode_*` functions in `codec/log.rs` and `codec/finalized_state.rs` are
unchanged. They produce `Bytes` from owned types. The ingest path continues to
construct owned `Log`, `BlockLogHeader`, etc., encode them, and write to stores.

### Decode functions preserved alongside ref constructors

The existing `decode_*` functions are kept for use in:

- ingest-side validation and testing
- any path that needs owned types (e.g., test assertions with `PartialEq`)
- the `LogRef::to_owned()` fallback

New `*Ref::new(Bytes)` constructors are added alongside them, not as
replacements.

### Phased rollout

**Phase 1: Ref types + bytes cache (this plan)**

1. Add `LogRef`, `BlockLogHeaderRef`, `LogDirectoryBucketRef` in `codec/`
   alongside existing decode functions.
2. Add `BytesCache` trait and `HashMap`-based implementation.
3. Refactor `LogMaterializer` to use `BytesCache` + ref types.
4. Update `PrimaryMaterializer` impl to use `LogRef`.
5. Update `exact_match` and `block_ref_for` to use accessor methods.
6. Update `QueryPage<Log>` → `QueryPage<LogRef>` at the API boundary.
7. Update benchmarks and tests.

**Phase 2: Compression**

1. Add LZ4 compression/decompression to blob store implementations.
2. Add a size threshold to skip compression for small values.
3. Benchmark decompression overhead per cache miss vs. I/O savings.

**Phase 3: Cache engine integration**

1. Integrate `BytesCache` with the full cache engine described in the metadata
   caching plan (miss coordination, eviction, metrics, eager population).
2. Wire ingest to eagerly populate cache after successful writes.

**Phase 4: Arena allocation (future)**

1. Profile per-entry overhead and locality impact.
2. Implement `ArenaBackedCache` behind the `BytesCache` trait.
3. Benchmark vs. standard `Bytes`-per-entry.

## File Change Map

### New files

| File | Contents |
|------|----------|
| `src/codec/log_ref.rs` | `LogRef`, `BlockLogHeaderRef`, `LogDirectoryBucketRef` |
| `src/cache/mod.rs` | `BytesCache` trait, `TableId` enum |
| `src/cache/hash_map.rs` | Initial `HashMap`-based `BytesCache` implementation |

### Modified files

| File | Changes |
|------|---------|
| `src/codec/mod.rs` | Add `pub mod log_ref;` |
| `src/logs/materialize.rs` | Replace per-type HashMaps with `BytesCache`, return ref types |
| `src/logs/filter.rs` | Adapt `exact_match` to accept `&LogRef` |
| `src/logs/query.rs` | Update `MatchedPrimary<Log>` → `MatchedPrimary<LogRef>` |
| `src/core/execution.rs` | No change to generic shape, `Primary = LogRef` flows through |
| `src/api/query_logs.rs` | `QueryPage<Log>` → `QueryPage<LogRef>` |
| `src/api/service.rs` | Pass `BytesCache` to materializer |
| `src/lib.rs` | Add `pub mod cache;` |
| `src/logs/types.rs` | Re-export ref types |
| `tests/finalized_index.rs` | Adapt assertions to use accessor methods or `.to_owned()` |
| `benches/materialize_bench.rs` | Update to use ref types and `BytesCache` |

### Unchanged files

| File | Rationale |
|------|-----------|
| `src/codec/log.rs` | Encode functions unchanged; owned decode functions kept |
| `src/codec/finalized_state.rs` | Fixed-size types remain owned |
| `src/domain/types.rs` | Owned types preserved for ingest path and tests |
| `src/ingest/` | Continues to work with owned types for construction |
| `src/store/traits.rs` | Already returns `Bytes`; no changes needed |
| `src/streams/` | Chunk decode produces `RoaringBitmap` (external type, not our wire format) |

## Open Questions

- Should `QueryPage<LogRef>` be the public API type, or should the API boundary
  convert to `QueryPage<Log>` to keep the external interface stable? The
  recommendation is to expose `LogRef` since it is `Clone + Send + Sync` and
  avoids a mandatory allocation at the boundary. A `to_owned()` escape hatch
  covers callers that need owned data.

- Should the bytes cache store block_logs blobs (large, per-block concatenated
  log payloads) or only metadata? Caching block_logs blobs means
  `LogRef::new()` can be constructed from a cached `Bytes` slice without any
  blob store read. The tradeoff is cache memory pressure from large blobs.
  Recommendation: cache block_logs blobs in a separate table with its own
  capacity budget, since queries are tip-biased and recent blocks will see
  high reuse.

- The `exact_match` function currently accesses `log.topics` as a slice with
  `.first()`, `.get(1)`, etc. The ref type equivalent uses `topic(0)`,
  `topic(1)`. Should `LogRef` expose a `topics_slice()` method that returns
  `&[[u8; 32]]`? This would require the topics to be contiguous and properly
  aligned in the buffer (they already are — 32-byte aligned naturally). This
  is a minor ergonomic decision.

## Relationship to Other Plans

This plan refines the value representation layer beneath the cache engine
described in `docs/plans/metadata-caching-architecture.md`. That plan's
design for tables, miss coordination, eviction, eager population, and family
facades applies unchanged. The key delta is that cached values are `Bytes`
(not decoded domain structs), and callers construct zero-copy ref types on
demand.
