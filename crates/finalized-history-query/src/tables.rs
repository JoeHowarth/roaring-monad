use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use quick_cache::sync::Cache;
use quick_cache::{DefaultHashBuilder, Lifecycle, OptionsBuilder, Weighter};

use crate::codec::StorageCodec;
use crate::codec::encode_u64;
use crate::error::{Error, Result};
use crate::logs::keys::read_u64_be;
use crate::store::traits::{BlobTableId, ScannableTableId, TableId};

pub trait PointTableSpec {
    const TABLE: TableId;
}

pub trait ScannableTableSpec {
    const TABLE: ScannableTableId;
}

pub trait BlobTableSpec {
    const TABLE: BlobTableId;
}

pub trait TableValueCodec: StorageCodec {}
impl<T: StorageCodec> TableValueCodec for T {}
use crate::logs::log_ref::{BlockLogHeaderRef, DirBucketRef, LogRef};
use crate::logs::table_specs::{
    BitmapByBlockSpec, BitmapPageBlobSpec, BitmapPageMetaSpec, BlockHashIndexSpec,
    BlockLogBlobSpec, BlockLogHeaderSpec, BlockRecordSpec, LogDirBucketSpec, LogDirByBlockSpec,
    LogDirSubBucketSpec,
};
use crate::logs::types::{BlockRecord, DirByBlock, StreamBitmapMeta};
use crate::store::traits::{BlobStore, BlobTable, KvTable, MetaStore, ScannableKvTable};
use crate::traces::table_specs::{
    BlockTraceBlobSpec, BlockTraceHeaderSpec, TraceBitmapByBlockSpec, TraceBitmapPageBlobSpec,
    TraceBitmapPageMetaSpec, TraceBlockRecordSpec, TraceDirBucketSpec, TraceDirByBlockSpec,
    TraceDirSubBucketSpec,
};
use crate::traces::types::{
    BlockTraceHeader, DirBucket as TraceDirBucket, DirByBlock as TraceDirByBlock,
    StreamBitmapMeta as TraceStreamBitmapMeta, TraceBlockRecord,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableCacheConfig {
    pub max_bytes: u64,
}

impl TableCacheConfig {
    pub const fn disabled() -> Self {
        Self { max_bytes: 0 }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BytesCacheConfig {
    pub block_records: TableCacheConfig,
    pub block_log_header: TableCacheConfig,
    pub log_dir_buckets: TableCacheConfig,
    pub log_dir_sub_buckets: TableCacheConfig,
    pub point_log_payloads: TableCacheConfig,
    pub bitmap_page_meta: TableCacheConfig,
    pub bitmap_page_blobs: TableCacheConfig,
}

impl BytesCacheConfig {
    pub const fn disabled() -> Self {
        Self {
            block_records: TableCacheConfig::disabled(),
            block_log_header: TableCacheConfig::disabled(),
            log_dir_buckets: TableCacheConfig::disabled(),
            log_dir_sub_buckets: TableCacheConfig::disabled(),
            point_log_payloads: TableCacheConfig::disabled(),
            bitmap_page_meta: TableCacheConfig::disabled(),
            bitmap_page_blobs: TableCacheConfig::disabled(),
        }
    }
}

impl Default for BytesCacheConfig {
    fn default() -> Self {
        Self::disabled()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TableCacheMetrics {
    pub hits: u64,
    pub misses: u64,
    pub inserts: u64,
    pub evictions: u64,
    pub bytes_used: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BytesCacheMetrics {
    pub block_records: TableCacheMetrics,
    pub block_log_header: TableCacheMetrics,
    pub log_dir_buckets: TableCacheMetrics,
    pub log_dir_sub_buckets: TableCacheMetrics,
    pub point_log_payloads: TableCacheMetrics,
    pub bitmap_page_meta: TableCacheMetrics,
    pub bitmap_page_blobs: TableCacheMetrics,
}

#[derive(Clone, Debug)]
struct WeightedBytes {
    bytes: Bytes,
    weight: u64,
}

#[derive(Debug, Default)]
struct Metrics {
    inserts: AtomicU64,
    evictions: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default)]
struct BytesWeighter;

impl Weighter<Vec<u8>, WeightedBytes> for BytesWeighter {
    fn weight(&self, _key: &Vec<u8>, value: &WeightedBytes) -> u64 {
        value.weight
    }
}

#[derive(Clone, Debug, Default)]
struct MetricsLifecycle {
    metrics: Arc<Metrics>,
}

impl Lifecycle<Vec<u8>, WeightedBytes> for MetricsLifecycle {
    type RequestState = ();

    fn begin_request(&self) -> Self::RequestState {}

    fn on_evict(&self, _state: &mut Self::RequestState, _key: Vec<u8>, _val: WeightedBytes) {
        self.metrics.evictions.fetch_add(1, Ordering::Relaxed);
    }
}

type QuickBytesCache =
    Cache<Vec<u8>, WeightedBytes, BytesWeighter, DefaultHashBuilder, MetricsLifecycle>;

#[derive(Clone, Debug)]
pub struct HashMapTableBytesCache {
    max_bytes: u64,
    inner: Option<Arc<QuickBytesCache>>,
    metrics: Arc<Metrics>,
}

impl HashMapTableBytesCache {
    pub fn new(max_bytes: u64) -> Self {
        let metrics = Arc::new(Metrics::default());
        let inner = (max_bytes > 0).then(|| {
            let mut options = OptionsBuilder::new();
            options
                .estimated_items_capacity(estimated_items_capacity(max_bytes))
                .weight_capacity(max_bytes);
            Arc::new(QuickBytesCache::with_options(
                options.build().expect("valid quick_cache options"),
                BytesWeighter,
                DefaultHashBuilder::default(),
                MetricsLifecycle {
                    metrics: Arc::clone(&metrics),
                },
            ))
        });
        Self {
            max_bytes,
            inner,
            metrics,
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        let inner = self.inner.as_ref()?;
        inner.get(key).map(|value| value.bytes)
    }

    pub fn put(&self, key: &[u8], value: Bytes, weight: usize) {
        let Some(inner) = self.inner.as_ref() else {
            return;
        };
        let weight = u64::try_from(weight).unwrap_or(u64::MAX);
        if weight > self.max_bytes {
            return;
        }
        inner.insert(
            key.to_vec(),
            WeightedBytes {
                bytes: value,
                weight,
            },
        );
        self.metrics.inserts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn metrics_snapshot(&self) -> TableCacheMetrics {
        let (hits, misses, bytes_used) = match self.inner.as_ref() {
            Some(inner) => (inner.hits(), inner.misses(), inner.weight()),
            None => (0, 0, 0),
        };
        TableCacheMetrics {
            hits,
            misses,
            inserts: self.metrics.inserts.load(Ordering::Relaxed),
            evictions: self.metrics.evictions.load(Ordering::Relaxed),
            bytes_used,
        }
    }
}

impl Default for HashMapTableBytesCache {
    fn default() -> Self {
        Self::new(0)
    }
}

fn estimated_items_capacity(max_bytes: u64) -> usize {
    const DEFAULT_ESTIMATED_ENTRY_BYTES: u64 = 256;
    let estimate = (max_bytes / DEFAULT_ESTIMATED_ENTRY_BYTES).max(1);
    usize::try_from(estimate).unwrap_or(usize::MAX)
}

fn point_log_payload_cache_key(block_num: u64, local_ordinal: u64) -> Vec<u8> {
    let mut key = b"point_log_payload/".to_vec();
    key.extend_from_slice(&block_num.to_be_bytes());
    key.extend_from_slice(&local_ordinal.to_be_bytes());
    key
}

#[derive(Clone)]
struct CachedPointTable<M: MetaStore, V> {
    table: KvTable<M>,
    cache: HashMapTableBytesCache,
    _marker: PhantomData<V>,
}

impl<M: MetaStore, V> CachedPointTable<M, V> {
    fn new(table: KvTable<M>, cache: HashMapTableBytesCache) -> Self {
        Self {
            table,
            cache,
            _marker: PhantomData,
        }
    }

    fn metrics(&self) -> TableCacheMetrics {
        self.cache.metrics_snapshot()
    }
}

impl<M: MetaStore, V: TableValueCodec> CachedPointTable<M, V> {
    async fn get_decoded(&self, key: &[u8]) -> Result<Option<V>> {
        if let Some(bytes) = self.cache.get(key) {
            return Ok(Some(V::decode(&bytes)?));
        }

        let Some(record) = self.table.get(key).await? else {
            return Ok(None);
        };
        self.cache
            .put(key, record.value.clone(), record.value.len());
        Ok(Some(V::decode(&record.value)?))
    }

    async fn put_encoded(&self, key: &[u8], value: &V) -> Result<()> {
        let encoded = value.encode();
        let _ = self
            .table
            .put(key, encoded.clone(), crate::store::traits::PutCond::Any)
            .await?;
        self.cache.put(key, encoded.clone(), encoded.len());
        Ok(())
    }
}

impl<M: MetaStore> CachedPointTable<M, Bytes> {
    async fn get_bytes(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if let Some(bytes) = self.cache.get(key) {
            return Ok(Some(bytes));
        }

        let Some(record) = self.table.get(key).await? else {
            return Ok(None);
        };
        self.cache
            .put(key, record.value.clone(), record.value.len());
        Ok(Some(record.value))
    }

    async fn put_bytes(&self, key: &[u8], value: Bytes) -> Result<()> {
        let _ = self
            .table
            .put(key, value.clone(), crate::store::traits::PutCond::Any)
            .await?;
        self.cache.put(key, value.clone(), value.len());
        Ok(())
    }
}

struct ScannableFragmentTable<M: MetaStore> {
    table: ScannableKvTable<M>,
}

impl<M: MetaStore> ScannableFragmentTable<M> {
    fn new(table: ScannableKvTable<M>) -> Self {
        Self { table }
    }

    async fn load_partition_values(&self, partition: &[u8]) -> Result<Vec<Bytes>> {
        let mut cursor = None;
        let mut values = Vec::new();

        loop {
            let page = self
                .table
                .list_prefix(partition, b"", cursor.take(), 1_024)
                .await?;
            for clustering in page.keys {
                let Some(record) = self.table.get(partition, &clustering).await? else {
                    continue;
                };
                values.push(record.value);
            }
            if page.next_cursor.is_none() {
                break;
            }
            cursor = page.next_cursor;
        }

        Ok(values)
    }

    async fn put_value(&self, partition: &[u8], clustering: &[u8], value: Bytes) -> Result<()> {
        let _ = self
            .table
            .put(
                partition,
                clustering,
                value,
                crate::store::traits::PutCond::Any,
            )
            .await?;
        Ok(())
    }
}

struct CachedBlobTable<B: BlobStore> {
    blob_table: BlobTable<B>,
    cache: HashMapTableBytesCache,
}

impl<B: BlobStore> CachedBlobTable<B> {
    fn new(blob_table: BlobTable<B>, cache: HashMapTableBytesCache) -> Self {
        Self { blob_table, cache }
    }

    async fn get_by_key(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if let Some(bytes) = self.cache.get(key) {
            return Ok(Some(bytes));
        }
        let Some(bytes) = self.blob_table.get(key).await? else {
            return Ok(None);
        };
        self.cache.put(key, bytes.clone(), bytes.len());
        Ok(Some(bytes))
    }

    async fn put_by_key(&self, key: &[u8], bytes: Bytes) -> Result<()> {
        self.blob_table.put(key, bytes.clone()).await?;
        self.cache.put(key, bytes.clone(), bytes.len());
        Ok(())
    }
}

fn cached_point_table<M: MetaStore, V>(
    table: KvTable<M>,
    cache: HashMapTableBytesCache,
) -> CachedPointTable<M, V> {
    CachedPointTable::new(table, cache)
}

fn cache_for(max_bytes: u64) -> HashMapTableBytesCache {
    HashMapTableBytesCache::new(max_bytes)
}

fn no_cache() -> HashMapTableBytesCache {
    HashMapTableBytesCache::default()
}

pub struct Tables<M: MetaStore, B: BlobStore> {
    block_hash_index: BlockHashIndexTable<M>,
    block_records: BlockRecordTable<M>,
    block_log_headers: BlockLogHeaderTable<M>,
    trace_block_records: TraceBlockRecordTable<M>,
    block_trace_headers: BlockTraceHeaderTable<M>,
    dir_buckets: DirBucketTable<M>,
    log_dir_sub_buckets: LogDirSubBucketTable<M>,
    directory_fragments: DirectoryFragmentTable<M>,
    trace_dir_buckets: TraceDirBucketTable<M>,
    trace_dir_sub_buckets: TraceDirSubBucketTable<M>,
    trace_directory_fragments: TraceDirectoryFragmentTable<M>,
    point_log_payloads: PointLogPayloadTable<M, B>,
    block_trace_blobs: BlockTraceBlobTable<M, B>,
    bitmap_by_block: BitmapByBlockTable<M>,
    bitmap_page_meta: BitmapPageMetaTable<M>,
    bitmap_page_blobs: BitmapPageBlobTable<B>,
    trace_bitmap_by_block: TraceBitmapByBlockTable<M>,
    trace_bitmap_page_meta: TraceBitmapPageMetaTable<M>,
    trace_bitmap_page_blobs: TraceBitmapPageBlobTable<B>,
}

impl<M: MetaStore, B: BlobStore> Tables<M, B> {
    pub fn without_cache(meta_store: M, blob_store: B) -> Self {
        let block_records = BlockRecordTable(cached_point_table(
            meta_store.table(BlockRecordSpec::TABLE),
            no_cache(),
        ));
        let block_log_headers = BlockLogHeaderTable(cached_point_table(
            meta_store.table(BlockLogHeaderSpec::TABLE),
            no_cache(),
        ));
        let block_trace_headers = BlockTraceHeaderTable(cached_point_table(
            meta_store.table(BlockTraceHeaderSpec::TABLE),
            no_cache(),
        ));
        Self {
            block_hash_index: BlockHashIndexTable {
                table: meta_store.table(BlockHashIndexSpec::TABLE),
            },
            block_records,
            block_log_headers: block_log_headers.clone(),
            trace_block_records: TraceBlockRecordTable(cached_point_table(
                meta_store.table(TraceBlockRecordSpec::TABLE),
                no_cache(),
            )),
            block_trace_headers: block_trace_headers.clone(),
            dir_buckets: DirBucketTable(cached_point_table(
                meta_store.table(LogDirBucketSpec::TABLE),
                no_cache(),
            )),
            log_dir_sub_buckets: LogDirSubBucketTable(cached_point_table(
                meta_store.table(LogDirSubBucketSpec::TABLE),
                no_cache(),
            )),
            directory_fragments: DirectoryFragmentTable {
                inner: ScannableFragmentTable::new(
                    meta_store.scannable_table(LogDirByBlockSpec::TABLE),
                ),
            },
            trace_dir_buckets: TraceDirBucketTable(cached_point_table(
                meta_store.table(TraceDirBucketSpec::TABLE),
                no_cache(),
            )),
            trace_dir_sub_buckets: TraceDirSubBucketTable(cached_point_table(
                meta_store.table(TraceDirSubBucketSpec::TABLE),
                no_cache(),
            )),
            trace_directory_fragments: TraceDirectoryFragmentTable {
                inner: ScannableFragmentTable::new(
                    meta_store.scannable_table(TraceDirByBlockSpec::TABLE),
                ),
            },
            point_log_payloads: PointLogPayloadTable {
                blob_table: blob_store.table(BlockLogBlobSpec::TABLE),
                cache: no_cache(),
                block_log_headers,
            },
            block_trace_blobs: BlockTraceBlobTable {
                blob_table: blob_store.table(BlockTraceBlobSpec::TABLE),
                block_trace_headers,
            },
            bitmap_by_block: BitmapByBlockTable {
                inner: ScannableFragmentTable::new(
                    meta_store.scannable_table(BitmapByBlockSpec::TABLE),
                ),
            },
            bitmap_page_meta: BitmapPageMetaTable(cached_point_table(
                meta_store.table(BitmapPageMetaSpec::TABLE),
                no_cache(),
            )),
            bitmap_page_blobs: BitmapPageBlobTable {
                inner: CachedBlobTable::new(
                    blob_store.table(BitmapPageBlobSpec::TABLE),
                    no_cache(),
                ),
            },
            trace_bitmap_by_block: TraceBitmapByBlockTable {
                inner: ScannableFragmentTable::new(
                    meta_store.scannable_table(TraceBitmapByBlockSpec::TABLE),
                ),
            },
            trace_bitmap_page_meta: TraceBitmapPageMetaTable(cached_point_table(
                meta_store.table(TraceBitmapPageMetaSpec::TABLE),
                no_cache(),
            )),
            trace_bitmap_page_blobs: TraceBitmapPageBlobTable {
                inner: CachedBlobTable::new(
                    blob_store.table(TraceBitmapPageBlobSpec::TABLE),
                    no_cache(),
                ),
            },
        }
    }

    pub fn new(meta_store: M, blob_store: B, config: BytesCacheConfig) -> Self {
        let block_records = BlockRecordTable(cached_point_table(
            meta_store.table(BlockRecordSpec::TABLE),
            cache_for(config.block_records.max_bytes),
        ));
        let block_log_headers = BlockLogHeaderTable(cached_point_table(
            meta_store.table(BlockLogHeaderSpec::TABLE),
            cache_for(config.block_log_header.max_bytes),
        ));
        let block_trace_headers = BlockTraceHeaderTable(cached_point_table(
            meta_store.table(BlockTraceHeaderSpec::TABLE),
            no_cache(),
        ));
        Self {
            block_hash_index: BlockHashIndexTable {
                table: meta_store.table(BlockHashIndexSpec::TABLE),
            },
            block_records,
            block_log_headers: block_log_headers.clone(),
            trace_block_records: TraceBlockRecordTable(cached_point_table(
                meta_store.table(TraceBlockRecordSpec::TABLE),
                no_cache(),
            )),
            block_trace_headers: block_trace_headers.clone(),
            dir_buckets: DirBucketTable(cached_point_table(
                meta_store.table(LogDirBucketSpec::TABLE),
                cache_for(config.log_dir_buckets.max_bytes),
            )),
            log_dir_sub_buckets: LogDirSubBucketTable(cached_point_table(
                meta_store.table(LogDirSubBucketSpec::TABLE),
                cache_for(config.log_dir_sub_buckets.max_bytes),
            )),
            directory_fragments: DirectoryFragmentTable {
                inner: ScannableFragmentTable::new(
                    meta_store.scannable_table(LogDirByBlockSpec::TABLE),
                ),
            },
            trace_dir_buckets: TraceDirBucketTable(cached_point_table(
                meta_store.table(TraceDirBucketSpec::TABLE),
                no_cache(),
            )),
            trace_dir_sub_buckets: TraceDirSubBucketTable(cached_point_table(
                meta_store.table(TraceDirSubBucketSpec::TABLE),
                no_cache(),
            )),
            trace_directory_fragments: TraceDirectoryFragmentTable {
                inner: ScannableFragmentTable::new(
                    meta_store.scannable_table(TraceDirByBlockSpec::TABLE),
                ),
            },
            point_log_payloads: PointLogPayloadTable {
                blob_table: blob_store.table(BlockLogBlobSpec::TABLE),
                cache: cache_for(config.point_log_payloads.max_bytes),
                block_log_headers,
            },
            block_trace_blobs: BlockTraceBlobTable {
                blob_table: blob_store.table(BlockTraceBlobSpec::TABLE),
                block_trace_headers,
            },
            bitmap_by_block: BitmapByBlockTable {
                inner: ScannableFragmentTable::new(
                    meta_store.scannable_table(BitmapByBlockSpec::TABLE),
                ),
            },
            bitmap_page_meta: BitmapPageMetaTable(cached_point_table(
                meta_store.table(BitmapPageMetaSpec::TABLE),
                cache_for(config.bitmap_page_meta.max_bytes),
            )),
            bitmap_page_blobs: BitmapPageBlobTable {
                inner: CachedBlobTable::new(
                    blob_store.table(BitmapPageBlobSpec::TABLE),
                    cache_for(config.bitmap_page_blobs.max_bytes),
                ),
            },
            trace_bitmap_by_block: TraceBitmapByBlockTable {
                inner: ScannableFragmentTable::new(
                    meta_store.scannable_table(TraceBitmapByBlockSpec::TABLE),
                ),
            },
            trace_bitmap_page_meta: TraceBitmapPageMetaTable(cached_point_table(
                meta_store.table(TraceBitmapPageMetaSpec::TABLE),
                no_cache(),
            )),
            trace_bitmap_page_blobs: TraceBitmapPageBlobTable {
                inner: CachedBlobTable::new(
                    blob_store.table(TraceBitmapPageBlobSpec::TABLE),
                    no_cache(),
                ),
            },
        }
    }

    pub fn block_records(&self) -> &BlockRecordTable<M> {
        &self.block_records
    }

    pub fn block_hash_index(&self) -> &BlockHashIndexTable<M> {
        &self.block_hash_index
    }

    pub fn block_log_headers(&self) -> &BlockLogHeaderTable<M> {
        &self.block_log_headers
    }

    pub fn trace_block_records(&self) -> &TraceBlockRecordTable<M> {
        &self.trace_block_records
    }

    pub fn block_trace_headers(&self) -> &BlockTraceHeaderTable<M> {
        &self.block_trace_headers
    }

    pub fn dir_buckets(&self) -> &DirBucketTable<M> {
        &self.dir_buckets
    }

    pub fn log_dir_sub_buckets(&self) -> &LogDirSubBucketTable<M> {
        &self.log_dir_sub_buckets
    }

    pub fn directory_fragments(&self) -> &DirectoryFragmentTable<M> {
        &self.directory_fragments
    }

    pub fn trace_dir_buckets(&self) -> &TraceDirBucketTable<M> {
        &self.trace_dir_buckets
    }

    pub fn trace_dir_sub_buckets(&self) -> &TraceDirSubBucketTable<M> {
        &self.trace_dir_sub_buckets
    }

    pub fn trace_directory_fragments(&self) -> &TraceDirectoryFragmentTable<M> {
        &self.trace_directory_fragments
    }

    pub fn point_log_payloads(&self) -> &PointLogPayloadTable<M, B> {
        &self.point_log_payloads
    }

    pub fn block_trace_blobs(&self) -> &BlockTraceBlobTable<M, B> {
        &self.block_trace_blobs
    }

    pub fn bitmap_by_block(&self) -> &BitmapByBlockTable<M> {
        &self.bitmap_by_block
    }

    pub fn bitmap_page_meta(&self) -> &BitmapPageMetaTable<M> {
        &self.bitmap_page_meta
    }

    pub fn bitmap_page_blobs(&self) -> &BitmapPageBlobTable<B> {
        &self.bitmap_page_blobs
    }

    pub fn trace_bitmap_by_block(&self) -> &TraceBitmapByBlockTable<M> {
        &self.trace_bitmap_by_block
    }

    pub fn trace_bitmap_page_meta(&self) -> &TraceBitmapPageMetaTable<M> {
        &self.trace_bitmap_page_meta
    }

    pub fn trace_bitmap_page_blobs(&self) -> &TraceBitmapPageBlobTable<B> {
        &self.trace_bitmap_page_blobs
    }

    pub fn metrics_snapshot(&self) -> BytesCacheMetrics {
        BytesCacheMetrics {
            block_records: self.block_records.metrics(),
            block_log_header: self.block_log_headers.metrics(),
            log_dir_buckets: self.dir_buckets.metrics(),
            log_dir_sub_buckets: self.log_dir_sub_buckets.metrics(),
            point_log_payloads: self.point_log_payloads.cache.metrics_snapshot(),
            bitmap_page_meta: self.bitmap_page_meta.metrics(),
            bitmap_page_blobs: self.bitmap_page_blobs.inner.cache.metrics_snapshot(),
        }
    }
}

pub struct BlockRecordTable<M: MetaStore>(CachedPointTable<M, BlockRecord>);

pub struct BlockHashIndexTable<M> {
    table: KvTable<M>,
}

impl<M: MetaStore> BlockHashIndexTable<M> {
    pub async fn get(&self, block_hash: &[u8; 32]) -> Result<Option<u64>> {
        let Some(record) = self.table.get(&BlockHashIndexSpec::key(block_hash)).await? else {
            return Ok(None);
        };
        read_u64_be(&record.value)
            .ok_or(Error::Decode("invalid block_hash_index value"))
            .map(Some)
    }

    pub async fn put(&self, block_hash: &[u8; 32], block_num: u64) -> Result<()> {
        let _ = self
            .table
            .put(
                &BlockHashIndexSpec::key(block_hash),
                encode_u64(block_num),
                crate::store::traits::PutCond::Any,
            )
            .await?;
        Ok(())
    }
}

impl<M: MetaStore> BlockRecordTable<M> {
    pub async fn get(&self, block_num: u64) -> Result<Option<BlockRecord>> {
        self.0.get_decoded(&BlockRecordSpec::key(block_num)).await
    }

    pub async fn put(&self, block_num: u64, block_record: &BlockRecord) -> Result<()> {
        self.0
            .put_encoded(&BlockRecordSpec::key(block_num), block_record)
            .await
    }

    fn metrics(&self) -> TableCacheMetrics {
        self.0.metrics()
    }
}

pub struct BlockLogHeaderTable<M: MetaStore>(CachedPointTable<M, Bytes>);

pub struct TraceBlockRecordTable<M: MetaStore>(CachedPointTable<M, TraceBlockRecord>);

impl<M: MetaStore> TraceBlockRecordTable<M> {
    pub async fn get(&self, block_num: u64) -> Result<Option<TraceBlockRecord>> {
        self.0
            .get_decoded(&TraceBlockRecordSpec::key(block_num))
            .await
    }

    pub async fn put(&self, block_num: u64, block_record: &TraceBlockRecord) -> Result<()> {
        self.0
            .put_encoded(&TraceBlockRecordSpec::key(block_num), block_record)
            .await
    }
}

pub struct BlockTraceHeaderTable<M: MetaStore>(CachedPointTable<M, BlockTraceHeader>);

impl<M: MetaStore> Clone for BlockTraceHeaderTable<M> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<M: MetaStore> BlockTraceHeaderTable<M> {
    pub async fn get(&self, block_num: u64) -> Result<Option<BlockTraceHeader>> {
        self.0
            .get_decoded(&BlockTraceHeaderSpec::key(block_num))
            .await
    }

    pub async fn put(&self, block_num: u64, header: &BlockTraceHeader) -> Result<()> {
        self.0
            .put_encoded(&BlockTraceHeaderSpec::key(block_num), header)
            .await
    }
}

impl<M: MetaStore> Clone for BlockLogHeaderTable<M> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<M: MetaStore> BlockLogHeaderTable<M> {
    pub async fn get(&self, block_num: u64) -> Result<Option<BlockLogHeaderRef>> {
        self.0
            .get_bytes(&BlockLogHeaderSpec::key(block_num))
            .await?
            .map(BlockLogHeaderRef::new)
            .transpose()
    }

    pub async fn put(
        &self,
        block_num: u64,
        header: &crate::logs::types::BlockLogHeader,
    ) -> Result<()> {
        self.0
            .put_bytes(&BlockLogHeaderSpec::key(block_num), header.encode())
            .await
    }

    fn metrics(&self) -> TableCacheMetrics {
        self.0.metrics()
    }
}

pub struct DirBucketTable<M: MetaStore>(CachedPointTable<M, Bytes>);

pub struct TraceDirBucketTable<M: MetaStore>(CachedPointTable<M, TraceDirBucket>);

impl<M: MetaStore> TraceDirBucketTable<M> {
    pub async fn get(&self, bucket_start: u64) -> Result<Option<TraceDirBucket>> {
        self.0
            .get_decoded(&TraceDirBucketSpec::key(bucket_start))
            .await
    }

    pub async fn put(&self, bucket_start: u64, bucket: &TraceDirBucket) -> Result<()> {
        self.0
            .put_encoded(&TraceDirBucketSpec::key(bucket_start), bucket)
            .await
    }
}

impl<M: MetaStore> DirBucketTable<M> {
    pub async fn get(&self, bucket_start: u64) -> Result<Option<DirBucketRef>> {
        self.0
            .get_bytes(&LogDirBucketSpec::key(bucket_start))
            .await?
            .map(DirBucketRef::new)
            .transpose()
    }

    pub async fn put(
        &self,
        bucket_start: u64,
        bucket: &crate::logs::types::DirBucket,
    ) -> Result<()> {
        self.0
            .put_bytes(&LogDirBucketSpec::key(bucket_start), bucket.encode())
            .await
    }

    fn metrics(&self) -> TableCacheMetrics {
        self.0.metrics()
    }
}

pub struct LogDirSubBucketTable<M: MetaStore>(CachedPointTable<M, Bytes>);

pub struct TraceDirSubBucketTable<M: MetaStore>(CachedPointTable<M, TraceDirBucket>);

impl<M: MetaStore> TraceDirSubBucketTable<M> {
    pub async fn get(&self, sub_bucket_start: u64) -> Result<Option<TraceDirBucket>> {
        self.0
            .get_decoded(&TraceDirSubBucketSpec::key(sub_bucket_start))
            .await
    }

    pub async fn put(&self, sub_bucket_start: u64, bucket: &TraceDirBucket) -> Result<()> {
        self.0
            .put_encoded(&TraceDirSubBucketSpec::key(sub_bucket_start), bucket)
            .await
    }
}

impl<M: MetaStore> LogDirSubBucketTable<M> {
    pub async fn get(&self, sub_bucket_start: u64) -> Result<Option<DirBucketRef>> {
        self.0
            .get_bytes(&LogDirSubBucketSpec::key(sub_bucket_start))
            .await?
            .map(DirBucketRef::new)
            .transpose()
    }

    pub async fn put(
        &self,
        sub_bucket_start: u64,
        bucket: &crate::logs::types::DirBucket,
    ) -> Result<()> {
        self.0
            .put_bytes(&LogDirSubBucketSpec::key(sub_bucket_start), bucket.encode())
            .await
    }

    fn metrics(&self) -> TableCacheMetrics {
        self.0.metrics()
    }
}

pub struct DirectoryFragmentTable<M: MetaStore> {
    inner: ScannableFragmentTable<M>,
}

pub struct TraceDirectoryFragmentTable<M: MetaStore> {
    inner: ScannableFragmentTable<M>,
}

impl<M: MetaStore> TraceDirectoryFragmentTable<M> {
    pub async fn load_sub_bucket_fragments(
        &self,
        sub_bucket_start: u64,
    ) -> Result<Vec<TraceDirByBlock>> {
        let partition = TraceDirByBlockSpec::partition(sub_bucket_start);
        let mut fragments = self
            .inner
            .load_partition_values(&partition)
            .await?
            .into_iter()
            .map(|bytes| TraceDirByBlock::decode(&bytes))
            .collect::<Result<Vec<_>>>()?;

        fragments.sort_by_key(|fragment| fragment.block_num);
        Ok(fragments)
    }

    pub async fn put(
        &self,
        sub_bucket_start: u64,
        block_num: u64,
        fragment: &TraceDirByBlock,
    ) -> Result<()> {
        let partition = TraceDirByBlockSpec::partition(sub_bucket_start);
        let clustering = TraceDirByBlockSpec::clustering(block_num);
        self.inner
            .put_value(&partition, &clustering, fragment.encode())
            .await
    }
}

impl<M: MetaStore> DirectoryFragmentTable<M> {
    pub async fn load_sub_bucket_fragments(
        &self,
        sub_bucket_start: u64,
    ) -> Result<Vec<DirByBlock>> {
        let partition = LogDirByBlockSpec::partition(sub_bucket_start);
        let mut fragments = self
            .inner
            .load_partition_values(&partition)
            .await?
            .into_iter()
            .map(|bytes| DirByBlock::decode(&bytes))
            .collect::<Result<Vec<_>>>()?;

        fragments.sort_by_key(|fragment| fragment.block_num);
        Ok(fragments)
    }

    pub async fn put(
        &self,
        sub_bucket_start: u64,
        block_num: u64,
        fragment: &DirByBlock,
    ) -> Result<()> {
        let partition = LogDirByBlockSpec::partition(sub_bucket_start);
        let clustering = LogDirByBlockSpec::clustering(block_num);
        self.inner
            .put_value(&partition, &clustering, fragment.encode())
            .await
    }
}

pub struct BitmapByBlockTable<M: MetaStore> {
    inner: ScannableFragmentTable<M>,
}

pub struct TraceBitmapByBlockTable<M: MetaStore> {
    inner: ScannableFragmentTable<M>,
}

impl<M: MetaStore> TraceBitmapByBlockTable<M> {
    pub async fn load_page_fragments(&self, stream: &str, page_start: u32) -> Result<Vec<Bytes>> {
        self.inner
            .load_partition_values(&TraceBitmapByBlockSpec::partition(stream, page_start))
            .await
    }

    pub async fn put(
        &self,
        stream: &str,
        page_start: u32,
        block_num: u64,
        bytes: Bytes,
    ) -> Result<()> {
        let partition = TraceBitmapByBlockSpec::partition(stream, page_start);
        let clustering = TraceBitmapByBlockSpec::clustering(block_num);
        self.inner.put_value(&partition, &clustering, bytes).await
    }
}

impl<M: MetaStore> BitmapByBlockTable<M> {
    pub async fn load_page_fragments(&self, stream: &str, page_start: u32) -> Result<Vec<Bytes>> {
        self.inner
            .load_partition_values(&BitmapByBlockSpec::partition(stream, page_start))
            .await
    }

    pub async fn put(
        &self,
        stream: &str,
        page_start: u32,
        block_num: u64,
        bytes: Bytes,
    ) -> Result<()> {
        let partition = BitmapByBlockSpec::partition(stream, page_start);
        let clustering = BitmapByBlockSpec::clustering(block_num);
        self.inner.put_value(&partition, &clustering, bytes).await
    }
}

pub struct BitmapPageMetaTable<M: MetaStore>(CachedPointTable<M, StreamBitmapMeta>);

pub struct TraceBitmapPageMetaTable<M: MetaStore>(CachedPointTable<M, TraceStreamBitmapMeta>);

impl<M: MetaStore> TraceBitmapPageMetaTable<M> {
    pub async fn get(
        &self,
        stream: &str,
        page_start: u32,
    ) -> Result<Option<TraceStreamBitmapMeta>> {
        self.0
            .get_decoded(&TraceBitmapPageMetaSpec::key(stream, page_start))
            .await
    }

    pub async fn put(
        &self,
        stream: &str,
        page_start: u32,
        meta: &TraceStreamBitmapMeta,
    ) -> Result<()> {
        self.0
            .put_encoded(&TraceBitmapPageMetaSpec::key(stream, page_start), meta)
            .await
    }
}

impl<M: MetaStore> BitmapPageMetaTable<M> {
    pub async fn get(&self, stream: &str, page_start: u32) -> Result<Option<StreamBitmapMeta>> {
        self.0
            .get_decoded(&BitmapPageMetaSpec::key(stream, page_start))
            .await
    }

    pub async fn put(&self, stream: &str, page_start: u32, meta: &StreamBitmapMeta) -> Result<()> {
        self.0
            .put_encoded(&BitmapPageMetaSpec::key(stream, page_start), meta)
            .await
    }

    fn metrics(&self) -> TableCacheMetrics {
        self.0.metrics()
    }
}

pub struct BitmapPageBlobTable<B: BlobStore> {
    inner: CachedBlobTable<B>,
}

pub struct TraceBitmapPageBlobTable<B: BlobStore> {
    inner: CachedBlobTable<B>,
}

impl<B: BlobStore> TraceBitmapPageBlobTable<B> {
    pub async fn get_for_page(&self, stream: &str, page_start: u32) -> Result<Option<Bytes>> {
        self.inner
            .get_by_key(&TraceBitmapPageBlobSpec::key(stream, page_start))
            .await
    }

    pub async fn get_by_key(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get_by_key(key).await
    }

    pub async fn put(&self, stream: &str, page_start: u32, bytes: Bytes) -> Result<()> {
        self.inner
            .put_by_key(&TraceBitmapPageBlobSpec::key(stream, page_start), bytes)
            .await
    }
}

pub struct BlockTraceBlobTable<M: MetaStore, B: BlobStore> {
    blob_table: BlobTable<B>,
    block_trace_headers: BlockTraceHeaderTable<M>,
}

impl<M: MetaStore, B: BlobStore> BlockTraceBlobTable<M, B> {
    pub async fn get(&self, block_num: u64) -> Result<Option<Bytes>> {
        self.blob_table
            .get(&BlockTraceBlobSpec::key(block_num))
            .await
    }

    pub async fn put_block(
        &self,
        block_num: u64,
        block_blob: Bytes,
        header: &BlockTraceHeader,
    ) -> Result<()> {
        if !block_blob.is_empty() {
            self.blob_table
                .put(&BlockTraceBlobSpec::key(block_num), block_blob)
                .await?;
        }
        self.block_trace_headers.put(block_num, header).await
    }
}

impl<B: BlobStore> BitmapPageBlobTable<B> {
    pub async fn get_for_page(&self, stream: &str, page_start: u32) -> Result<Option<Bytes>> {
        self.inner
            .get_by_key(&BitmapPageBlobSpec::key(stream, page_start))
            .await
    }

    pub async fn get_by_key(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get_by_key(key).await
    }

    pub async fn put(&self, stream: &str, page_start: u32, bytes: Bytes) -> Result<()> {
        self.inner
            .put_by_key(&BitmapPageBlobSpec::key(stream, page_start), bytes)
            .await
    }
}

pub struct PointLogPayloadTable<M: MetaStore, B: BlobStore> {
    blob_table: BlobTable<B>,
    cache: HashMapTableBytesCache,
    block_log_headers: BlockLogHeaderTable<M>,
}

impl<M: MetaStore, B: BlobStore> PointLogPayloadTable<M, B> {
    pub async fn load_contiguous_run(
        &self,
        block_num: u64,
        start_local_ordinal: usize,
        end_local_ordinal_inclusive: usize,
    ) -> Result<Vec<LogRef>> {
        if end_local_ordinal_inclusive < start_local_ordinal {
            return Ok(Vec::new());
        }

        let mut cached = Vec::with_capacity(
            end_local_ordinal_inclusive
                .saturating_sub(start_local_ordinal)
                .saturating_add(1),
        );
        let mut all_cached = true;
        for local_ordinal in start_local_ordinal..=end_local_ordinal_inclusive {
            let payload_key = point_log_payload_key(block_num, local_ordinal)?;
            let maybe_cached = self.cache.get(&payload_key);
            cached.push((payload_key, maybe_cached));
            if cached
                .last()
                .and_then(|(_, value)| value.as_ref())
                .is_none()
            {
                all_cached = false;
            }
        }
        if all_cached {
            return cached
                .into_iter()
                .map(|(_, bytes)| bytes.map(LogRef::new).transpose()?.ok_or(Error::NotFound))
                .collect();
        }

        let Some(header) = self.block_log_headers.get(block_num).await? else {
            return Ok(Vec::new());
        };
        if end_local_ordinal_inclusive + 1 >= header.count() {
            return Ok(Vec::new());
        }

        let start = header.offset(start_local_ordinal);
        let end = header.offset(end_local_ordinal_inclusive + 1);
        let Some(run_bytes) = self
            .blob_table
            .read_range(
                &BlockLogBlobSpec::key(block_num),
                u64::from(start),
                u64::from(end),
            )
            .await?
        else {
            return Ok(Vec::new());
        };

        let mut out = Vec::with_capacity(cached.len());
        for (index, (payload_key, maybe_cached)) in cached.into_iter().enumerate() {
            if let Some(bytes) = maybe_cached {
                out.push(LogRef::new(bytes)?);
                continue;
            }

            let local_ordinal = start_local_ordinal + index;
            let relative_start = header
                .offset(local_ordinal)
                .checked_sub(start)
                .ok_or(Error::Decode("invalid block log range"))?;
            let relative_end = header
                .offset(local_ordinal + 1)
                .checked_sub(start)
                .ok_or(Error::Decode("invalid block log range"))?;
            let payload = slice_relative(&run_bytes, relative_start, relative_end)?;
            self.cache.put(&payload_key, payload.clone(), payload.len());
            out.push(LogRef::new(payload)?);
        }

        Ok(out)
    }

    pub async fn put_block(
        &self,
        block_num: u64,
        block_blob: Bytes,
        header: &crate::logs::types::BlockLogHeader,
    ) -> Result<()> {
        self.blob_table
            .put(&BlockLogBlobSpec::key(block_num), block_blob.clone())
            .await?;
        self.block_log_headers.put(block_num, header).await?;

        for local_ordinal in 0..header.offsets.len().saturating_sub(1) {
            let payload = slice_relative(
                &block_blob,
                header.offsets[local_ordinal],
                header.offsets[local_ordinal + 1],
            )?;
            let payload_key = point_log_payload_cache_key(block_num, local_ordinal as u64);
            self.cache.put(&payload_key, payload.clone(), payload.len());
        }

        Ok(())
    }
}

fn point_log_payload_key(block_num: u64, local_ordinal: usize) -> Result<Vec<u8>> {
    let local_ordinal =
        u64::try_from(local_ordinal).map_err(|_| Error::Decode("local ordinal overflow"))?;
    Ok(point_log_payload_cache_key(block_num, local_ordinal))
}

fn slice_relative(bytes: &Bytes, start: u32, end: u32) -> Result<Bytes> {
    let start = usize::try_from(start).map_err(|_| Error::Decode("block log range overflow"))?;
    let end = usize::try_from(end).map_err(|_| Error::Decode("block log range overflow"))?;
    if start > end || end > bytes.len() {
        return Err(Error::Decode("invalid block log range"));
    }
    Ok(bytes.slice(start..end))
}
