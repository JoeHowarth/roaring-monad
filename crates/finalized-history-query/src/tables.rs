use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use quick_cache::sync::Cache;
use quick_cache::{DefaultHashBuilder, Lifecycle, OptionsBuilder, Weighter};

use crate::error::{Error, Result};
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
use crate::logs::log_ref::{BlockLogHeaderRef, DirBucketRef, LogRef};
use crate::logs::table_specs::{
    BitmapByBlockSpec, BitmapPageBlobSpec, BitmapPageMetaSpec, BlockLogBlobSpec,
    BlockLogHeaderSpec, BlockRecordSpec, LogDirBucketSpec, LogDirByBlockSpec, LogDirSubBucketSpec,
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

pub struct Tables<M: MetaStore, B: BlobStore> {
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
        let block_records = BlockRecordTable {
            table: meta_store.table(BlockRecordSpec::TABLE),
            cache: HashMapTableBytesCache::default(),
        };
        let block_log_headers = BlockLogHeaderTable {
            table: meta_store.table(BlockLogHeaderSpec::TABLE),
            cache: HashMapTableBytesCache::default(),
        };
        let block_trace_headers = BlockTraceHeaderTable {
            table: meta_store.table(BlockTraceHeaderSpec::TABLE),
            cache: HashMapTableBytesCache::default(),
        };
        Self {
            block_records,
            block_log_headers: block_log_headers.clone(),
            trace_block_records: TraceBlockRecordTable {
                table: meta_store.table(TraceBlockRecordSpec::TABLE),
                cache: HashMapTableBytesCache::default(),
            },
            block_trace_headers: block_trace_headers.clone(),
            dir_buckets: DirBucketTable {
                table: meta_store.table(LogDirBucketSpec::TABLE),
                cache: HashMapTableBytesCache::default(),
            },
            log_dir_sub_buckets: LogDirSubBucketTable {
                table: meta_store.table(LogDirSubBucketSpec::TABLE),
                cache: HashMapTableBytesCache::default(),
            },
            directory_fragments: DirectoryFragmentTable {
                table: meta_store.scannable_table(LogDirByBlockSpec::TABLE),
            },
            trace_dir_buckets: TraceDirBucketTable {
                table: meta_store.table(TraceDirBucketSpec::TABLE),
                cache: HashMapTableBytesCache::default(),
            },
            trace_dir_sub_buckets: TraceDirSubBucketTable {
                table: meta_store.table(TraceDirSubBucketSpec::TABLE),
                cache: HashMapTableBytesCache::default(),
            },
            trace_directory_fragments: TraceDirectoryFragmentTable {
                table: meta_store.scannable_table(TraceDirByBlockSpec::TABLE),
            },
            point_log_payloads: PointLogPayloadTable {
                blob_table: blob_store.table(BlockLogBlobSpec::TABLE),
                cache: HashMapTableBytesCache::default(),
                block_log_headers,
            },
            block_trace_blobs: BlockTraceBlobTable {
                blob_table: blob_store.table(BlockTraceBlobSpec::TABLE),
                block_trace_headers,
            },
            bitmap_by_block: BitmapByBlockTable {
                table: meta_store.scannable_table(BitmapByBlockSpec::TABLE),
            },
            bitmap_page_meta: BitmapPageMetaTable {
                table: meta_store.table(BitmapPageMetaSpec::TABLE),
                cache: HashMapTableBytesCache::default(),
            },
            bitmap_page_blobs: BitmapPageBlobTable {
                blob_table: blob_store.table(BitmapPageBlobSpec::TABLE),
                cache: HashMapTableBytesCache::default(),
            },
            trace_bitmap_by_block: TraceBitmapByBlockTable {
                table: meta_store.scannable_table(TraceBitmapByBlockSpec::TABLE),
            },
            trace_bitmap_page_meta: TraceBitmapPageMetaTable {
                table: meta_store.table(TraceBitmapPageMetaSpec::TABLE),
                cache: HashMapTableBytesCache::default(),
            },
            trace_bitmap_page_blobs: TraceBitmapPageBlobTable {
                blob_table: blob_store.table(TraceBitmapPageBlobSpec::TABLE),
                cache: HashMapTableBytesCache::default(),
            },
        }
    }

    pub fn new(meta_store: M, blob_store: B, config: BytesCacheConfig) -> Self {
        let block_records = BlockRecordTable {
            table: meta_store.table(BlockRecordSpec::TABLE),
            cache: HashMapTableBytesCache::new(config.block_records.max_bytes),
        };
        let block_log_headers = BlockLogHeaderTable {
            table: meta_store.table(BlockLogHeaderSpec::TABLE),
            cache: HashMapTableBytesCache::new(config.block_log_header.max_bytes),
        };
        let block_trace_headers = BlockTraceHeaderTable {
            table: meta_store.table(BlockTraceHeaderSpec::TABLE),
            cache: HashMapTableBytesCache::default(),
        };
        Self {
            block_records,
            block_log_headers: block_log_headers.clone(),
            trace_block_records: TraceBlockRecordTable {
                table: meta_store.table(TraceBlockRecordSpec::TABLE),
                cache: HashMapTableBytesCache::default(),
            },
            block_trace_headers: block_trace_headers.clone(),
            dir_buckets: DirBucketTable {
                table: meta_store.table(LogDirBucketSpec::TABLE),
                cache: HashMapTableBytesCache::new(config.log_dir_buckets.max_bytes),
            },
            log_dir_sub_buckets: LogDirSubBucketTable {
                table: meta_store.table(LogDirSubBucketSpec::TABLE),
                cache: HashMapTableBytesCache::new(config.log_dir_sub_buckets.max_bytes),
            },
            directory_fragments: DirectoryFragmentTable {
                table: meta_store.scannable_table(LogDirByBlockSpec::TABLE),
            },
            trace_dir_buckets: TraceDirBucketTable {
                table: meta_store.table(TraceDirBucketSpec::TABLE),
                cache: HashMapTableBytesCache::default(),
            },
            trace_dir_sub_buckets: TraceDirSubBucketTable {
                table: meta_store.table(TraceDirSubBucketSpec::TABLE),
                cache: HashMapTableBytesCache::default(),
            },
            trace_directory_fragments: TraceDirectoryFragmentTable {
                table: meta_store.scannable_table(TraceDirByBlockSpec::TABLE),
            },
            point_log_payloads: PointLogPayloadTable {
                blob_table: blob_store.table(BlockLogBlobSpec::TABLE),
                cache: HashMapTableBytesCache::new(config.point_log_payloads.max_bytes),
                block_log_headers: block_log_headers.clone(),
            },
            block_trace_blobs: BlockTraceBlobTable {
                blob_table: blob_store.table(BlockTraceBlobSpec::TABLE),
                block_trace_headers,
            },
            bitmap_by_block: BitmapByBlockTable {
                table: meta_store.scannable_table(BitmapByBlockSpec::TABLE),
            },
            bitmap_page_meta: BitmapPageMetaTable {
                table: meta_store.table(BitmapPageMetaSpec::TABLE),
                cache: HashMapTableBytesCache::new(config.bitmap_page_meta.max_bytes),
            },
            bitmap_page_blobs: BitmapPageBlobTable {
                blob_table: blob_store.table(BitmapPageBlobSpec::TABLE),
                cache: HashMapTableBytesCache::new(config.bitmap_page_blobs.max_bytes),
            },
            trace_bitmap_by_block: TraceBitmapByBlockTable {
                table: meta_store.scannable_table(TraceBitmapByBlockSpec::TABLE),
            },
            trace_bitmap_page_meta: TraceBitmapPageMetaTable {
                table: meta_store.table(TraceBitmapPageMetaSpec::TABLE),
                cache: HashMapTableBytesCache::default(),
            },
            trace_bitmap_page_blobs: TraceBitmapPageBlobTable {
                blob_table: blob_store.table(TraceBitmapPageBlobSpec::TABLE),
                cache: HashMapTableBytesCache::default(),
            },
        }
    }

    pub fn block_records(&self) -> &BlockRecordTable<M> {
        &self.block_records
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
            block_records: self.block_records.cache.metrics_snapshot(),
            block_log_header: self.block_log_headers.cache.metrics_snapshot(),
            log_dir_buckets: self.dir_buckets.cache.metrics_snapshot(),
            log_dir_sub_buckets: self.log_dir_sub_buckets.cache.metrics_snapshot(),
            point_log_payloads: self.point_log_payloads.cache.metrics_snapshot(),
            bitmap_page_meta: self.bitmap_page_meta.cache.metrics_snapshot(),
            bitmap_page_blobs: self.bitmap_page_blobs.cache.metrics_snapshot(),
        }
    }
}

pub struct BlockRecordTable<M> {
    table: KvTable<M>,
    cache: HashMapTableBytesCache,
}

impl<M: MetaStore> BlockRecordTable<M> {
    pub async fn get(&self, block_num: u64) -> Result<Option<BlockRecord>> {
        let key = BlockRecordSpec::key(block_num);
        if let Some(bytes) = self.cache.get(&key) {
            return Ok(Some(BlockRecord::decode(&bytes)?));
        }

        let Some(record) = self.table.get(&key).await? else {
            return Ok(None);
        };
        self.cache
            .put(&key, record.value.clone(), record.value.len());
        Ok(Some(BlockRecord::decode(&record.value)?))
    }

    pub async fn put(&self, block_num: u64, block_record: &BlockRecord) -> Result<()> {
        let key = BlockRecordSpec::key(block_num);
        let encoded = block_record.encode();
        let _ = self
            .table
            .put(&key, encoded.clone(), crate::store::traits::PutCond::Any)
            .await?;
        self.cache.put(&key, encoded.clone(), encoded.len());
        Ok(())
    }
}

pub struct BlockLogHeaderTable<M> {
    table: KvTable<M>,
    cache: HashMapTableBytesCache,
}

pub struct TraceBlockRecordTable<M> {
    table: KvTable<M>,
    cache: HashMapTableBytesCache,
}

impl<M: MetaStore> TraceBlockRecordTable<M> {
    pub async fn get(&self, block_num: u64) -> Result<Option<TraceBlockRecord>> {
        let key = TraceBlockRecordSpec::key(block_num);
        if let Some(bytes) = self.cache.get(&key) {
            return Ok(Some(TraceBlockRecord::decode(&bytes)?));
        }
        let Some(record) = self.table.get(&key).await? else {
            return Ok(None);
        };
        self.cache.put(&key, record.value.clone(), record.value.len());
        Ok(Some(TraceBlockRecord::decode(&record.value)?))
    }

    pub async fn put(&self, block_num: u64, block_record: &TraceBlockRecord) -> Result<()> {
        let key = TraceBlockRecordSpec::key(block_num);
        let encoded = block_record.encode();
        let _ = self
            .table
            .put(&key, encoded.clone(), crate::store::traits::PutCond::Any)
            .await?;
        self.cache.put(&key, encoded.clone(), encoded.len());
        Ok(())
    }
}

pub struct BlockTraceHeaderTable<M> {
    table: KvTable<M>,
    cache: HashMapTableBytesCache,
}

impl<M: MetaStore> Clone for BlockTraceHeaderTable<M> {
    fn clone(&self) -> Self {
        Self {
            table: self.table.clone(),
            cache: self.cache.clone(),
        }
    }
}

impl<M: MetaStore> BlockTraceHeaderTable<M> {
    pub async fn get(&self, block_num: u64) -> Result<Option<BlockTraceHeader>> {
        let key = BlockTraceHeaderSpec::key(block_num);
        if let Some(bytes) = self.cache.get(&key) {
            return Ok(Some(BlockTraceHeader::decode(&bytes)?));
        }
        let Some(record) = self.table.get(&key).await? else {
            return Ok(None);
        };
        self.cache.put(&key, record.value.clone(), record.value.len());
        Ok(Some(BlockTraceHeader::decode(&record.value)?))
    }

    pub async fn put(&self, block_num: u64, header: &BlockTraceHeader) -> Result<()> {
        let key = BlockTraceHeaderSpec::key(block_num);
        let encoded = header.encode();
        let _ = self
            .table
            .put(&key, encoded.clone(), crate::store::traits::PutCond::Any)
            .await?;
        self.cache.put(&key, encoded.clone(), encoded.len());
        Ok(())
    }
}

impl<M: MetaStore> Clone for BlockLogHeaderTable<M> {
    fn clone(&self) -> Self {
        Self {
            table: self.table.clone(),
            cache: self.cache.clone(),
        }
    }
}

impl<M: MetaStore> BlockLogHeaderTable<M> {
    pub async fn get(&self, block_num: u64) -> Result<Option<BlockLogHeaderRef>> {
        let key = BlockLogHeaderSpec::key(block_num);
        if let Some(bytes) = self.cache.get(&key) {
            return Ok(Some(BlockLogHeaderRef::new(bytes)?));
        }
        let Some(record) = self.table.get(&key).await? else {
            return Ok(None);
        };
        self.cache
            .put(&key, record.value.clone(), record.value.len());
        Ok(Some(BlockLogHeaderRef::new(record.value)?))
    }

    pub async fn put(
        &self,
        block_num: u64,
        header: &crate::logs::types::BlockLogHeader,
    ) -> Result<()> {
        let key = BlockLogHeaderSpec::key(block_num);
        let encoded = header.encode();
        let _ = self
            .table
            .put(&key, encoded.clone(), crate::store::traits::PutCond::Any)
            .await?;
        self.cache.put(&key, encoded.clone(), encoded.len());
        Ok(())
    }
}

pub struct DirBucketTable<M> {
    table: KvTable<M>,
    cache: HashMapTableBytesCache,
}

pub struct TraceDirBucketTable<M> {
    table: KvTable<M>,
    cache: HashMapTableBytesCache,
}

impl<M: MetaStore> TraceDirBucketTable<M> {
    pub async fn get(&self, bucket_start: u64) -> Result<Option<TraceDirBucket>> {
        let key = TraceDirBucketSpec::key(bucket_start);
        if let Some(bytes) = self.cache.get(&key) {
            return Ok(Some(TraceDirBucket::decode(&bytes)?));
        }
        let Some(record) = self.table.get(&key).await? else {
            return Ok(None);
        };
        self.cache.put(&key, record.value.clone(), record.value.len());
        Ok(Some(TraceDirBucket::decode(&record.value)?))
    }

    pub async fn put(&self, bucket_start: u64, bucket: &TraceDirBucket) -> Result<()> {
        let key = TraceDirBucketSpec::key(bucket_start);
        let encoded = bucket.encode();
        let _ = self
            .table
            .put(&key, encoded.clone(), crate::store::traits::PutCond::Any)
            .await?;
        self.cache.put(&key, encoded.clone(), encoded.len());
        Ok(())
    }
}

impl<M: MetaStore> DirBucketTable<M> {
    pub async fn get(&self, bucket_start: u64) -> Result<Option<DirBucketRef>> {
        let key = LogDirBucketSpec::key(bucket_start);
        if let Some(bytes) = self.cache.get(&key) {
            return Ok(Some(DirBucketRef::new(bytes)?));
        }
        let Some(record) = self.table.get(&key).await? else {
            return Ok(None);
        };
        self.cache
            .put(&key, record.value.clone(), record.value.len());
        Ok(Some(DirBucketRef::new(record.value)?))
    }

    pub async fn put(
        &self,
        bucket_start: u64,
        bucket: &crate::logs::types::DirBucket,
    ) -> Result<()> {
        let key = LogDirBucketSpec::key(bucket_start);
        let encoded = bucket.encode();
        let _ = self
            .table
            .put(&key, encoded.clone(), crate::store::traits::PutCond::Any)
            .await?;
        self.cache.put(&key, encoded.clone(), encoded.len());
        Ok(())
    }
}

pub struct LogDirSubBucketTable<M> {
    table: KvTable<M>,
    cache: HashMapTableBytesCache,
}

pub struct TraceDirSubBucketTable<M> {
    table: KvTable<M>,
    cache: HashMapTableBytesCache,
}

impl<M: MetaStore> TraceDirSubBucketTable<M> {
    pub async fn get(&self, sub_bucket_start: u64) -> Result<Option<TraceDirBucket>> {
        let key = TraceDirSubBucketSpec::key(sub_bucket_start);
        if let Some(bytes) = self.cache.get(&key) {
            return Ok(Some(TraceDirBucket::decode(&bytes)?));
        }
        let Some(record) = self.table.get(&key).await? else {
            return Ok(None);
        };
        self.cache.put(&key, record.value.clone(), record.value.len());
        Ok(Some(TraceDirBucket::decode(&record.value)?))
    }

    pub async fn put(&self, sub_bucket_start: u64, bucket: &TraceDirBucket) -> Result<()> {
        let key = TraceDirSubBucketSpec::key(sub_bucket_start);
        let encoded = bucket.encode();
        let _ = self
            .table
            .put(&key, encoded.clone(), crate::store::traits::PutCond::Any)
            .await?;
        self.cache.put(&key, encoded.clone(), encoded.len());
        Ok(())
    }
}

impl<M: MetaStore> LogDirSubBucketTable<M> {
    pub async fn get(&self, sub_bucket_start: u64) -> Result<Option<DirBucketRef>> {
        let key = LogDirSubBucketSpec::key(sub_bucket_start);
        if let Some(bytes) = self.cache.get(&key) {
            return Ok(Some(DirBucketRef::new(bytes)?));
        }
        let Some(record) = self.table.get(&key).await? else {
            return Ok(None);
        };
        self.cache
            .put(&key, record.value.clone(), record.value.len());
        Ok(Some(DirBucketRef::new(record.value)?))
    }

    pub async fn put(
        &self,
        sub_bucket_start: u64,
        bucket: &crate::logs::types::DirBucket,
    ) -> Result<()> {
        let key = LogDirSubBucketSpec::key(sub_bucket_start);
        let encoded = bucket.encode();
        let _ = self
            .table
            .put(&key, encoded.clone(), crate::store::traits::PutCond::Any)
            .await?;
        self.cache.put(&key, encoded.clone(), encoded.len());
        Ok(())
    }
}

pub struct DirectoryFragmentTable<M> {
    table: ScannableKvTable<M>,
}

pub struct TraceDirectoryFragmentTable<M> {
    table: ScannableKvTable<M>,
}

impl<M: MetaStore> TraceDirectoryFragmentTable<M> {
    pub async fn load_sub_bucket_fragments(
        &self,
        sub_bucket_start: u64,
    ) -> Result<Vec<TraceDirByBlock>> {
        let partition = TraceDirByBlockSpec::partition(sub_bucket_start);
        let mut cursor = None;
        let mut fragments = Vec::new();

        loop {
            let page = self
                .table
                .list_prefix(&partition, b"", cursor.take(), 1_024)
                .await?;
            for clustering in page.keys {
                let Some(record) = self.table.get(&partition, &clustering).await? else {
                    continue;
                };
                fragments.push(TraceDirByBlock::decode(&record.value)?);
            }
            if page.next_cursor.is_none() {
                break;
            }
            cursor = page.next_cursor;
        }

        fragments.sort_by_key(|fragment| fragment.block_num);
        Ok(fragments)
    }

    pub async fn put(&self, sub_bucket_start: u64, block_num: u64, fragment: &TraceDirByBlock) -> Result<()> {
        let partition = TraceDirByBlockSpec::partition(sub_bucket_start);
        let clustering = TraceDirByBlockSpec::clustering(block_num);
        let _ = self
            .table
            .put(
                &partition,
                &clustering,
                fragment.encode(),
                crate::store::traits::PutCond::Any,
            )
            .await?;
        Ok(())
    }
}

impl<M: MetaStore> DirectoryFragmentTable<M> {
    pub async fn load_sub_bucket_fragments(
        &self,
        sub_bucket_start: u64,
    ) -> Result<Vec<DirByBlock>> {
        let partition = LogDirByBlockSpec::partition(sub_bucket_start);
        let mut cursor = None;
        let mut fragments = Vec::new();

        loop {
            let page = self
                .table
                .list_prefix(&partition, b"", cursor.take(), 1_024)
                .await?;
            for clustering in page.keys {
                let Some(record) = self.table.get(&partition, &clustering).await? else {
                    continue;
                };
                fragments.push(DirByBlock::decode(&record.value)?);
            }
            if page.next_cursor.is_none() {
                break;
            }
            cursor = page.next_cursor;
        }

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
        let _ = self
            .table
            .put(
                &partition,
                &clustering,
                fragment.encode(),
                crate::store::traits::PutCond::Any,
            )
            .await?;
        Ok(())
    }
}

pub struct BitmapByBlockTable<M> {
    table: ScannableKvTable<M>,
}

pub struct TraceBitmapByBlockTable<M> {
    table: ScannableKvTable<M>,
}

impl<M: MetaStore> TraceBitmapByBlockTable<M> {
    pub async fn load_page_fragments(&self, stream: &str, page_start: u32) -> Result<Vec<Bytes>> {
        let partition = TraceBitmapByBlockSpec::partition(stream, page_start);
        let mut cursor = None;
        let mut fragments = Vec::new();

        loop {
            let page = self
                .table
                .list_prefix(&partition, b"", cursor.take(), 1_024)
                .await?;
            for clustering in page.keys {
                let Some(record) = self.table.get(&partition, &clustering).await? else {
                    continue;
                };
                fragments.push(record.value);
            }
            if page.next_cursor.is_none() {
                break;
            }
            cursor = page.next_cursor;
        }

        Ok(fragments)
    }

    pub async fn put(&self, stream: &str, page_start: u32, block_num: u64, bytes: Bytes) -> Result<()> {
        let partition = TraceBitmapByBlockSpec::partition(stream, page_start);
        let clustering = TraceBitmapByBlockSpec::clustering(block_num);
        let _ = self
            .table
            .put(
                &partition,
                &clustering,
                bytes,
                crate::store::traits::PutCond::Any,
            )
            .await?;
        Ok(())
    }
}

impl<M: MetaStore> BitmapByBlockTable<M> {
    pub async fn load_page_fragments(&self, stream: &str, page_start: u32) -> Result<Vec<Bytes>> {
        let partition = BitmapByBlockSpec::partition(stream, page_start);
        let mut cursor = None;
        let mut fragments = Vec::new();

        loop {
            let page = self
                .table
                .list_prefix(&partition, b"", cursor.take(), 1_024)
                .await?;
            for clustering in page.keys {
                let Some(record) = self.table.get(&partition, &clustering).await? else {
                    continue;
                };
                fragments.push(record.value);
            }
            if page.next_cursor.is_none() {
                break;
            }
            cursor = page.next_cursor;
        }

        Ok(fragments)
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
        let _ = self
            .table
            .put(
                &partition,
                &clustering,
                bytes,
                crate::store::traits::PutCond::Any,
            )
            .await?;
        Ok(())
    }
}

pub struct BitmapPageMetaTable<M> {
    table: KvTable<M>,
    cache: HashMapTableBytesCache,
}

pub struct TraceBitmapPageMetaTable<M> {
    table: KvTable<M>,
    cache: HashMapTableBytesCache,
}

impl<M: MetaStore> TraceBitmapPageMetaTable<M> {
    pub async fn get(&self, stream: &str, page_start: u32) -> Result<Option<TraceStreamBitmapMeta>> {
        let key = TraceBitmapPageMetaSpec::key(stream, page_start);
        if let Some(bytes) = self.cache.get(&key) {
            return Ok(Some(TraceStreamBitmapMeta::decode(&bytes)?));
        }

        let Some(record) = self.table.get(&key).await? else {
            return Ok(None);
        };
        self.cache.put(&key, record.value.clone(), record.value.len());
        Ok(Some(TraceStreamBitmapMeta::decode(&record.value)?))
    }

    pub async fn put(&self, stream: &str, page_start: u32, meta: &TraceStreamBitmapMeta) -> Result<()> {
        let key = TraceBitmapPageMetaSpec::key(stream, page_start);
        let encoded = meta.encode();
        let _ = self
            .table
            .put(&key, encoded.clone(), crate::store::traits::PutCond::Any)
            .await?;
        self.cache.put(&key, encoded.clone(), encoded.len());
        Ok(())
    }
}

impl<M: MetaStore> BitmapPageMetaTable<M> {
    pub async fn get(&self, stream: &str, page_start: u32) -> Result<Option<StreamBitmapMeta>> {
        let key = BitmapPageMetaSpec::key(stream, page_start);
        if let Some(bytes) = self.cache.get(&key) {
            return Ok(Some(StreamBitmapMeta::decode(&bytes)?));
        }

        let Some(record) = self.table.get(&key).await? else {
            return Ok(None);
        };
        self.cache
            .put(&key, record.value.clone(), record.value.len());
        Ok(Some(StreamBitmapMeta::decode(&record.value)?))
    }

    pub async fn put(&self, stream: &str, page_start: u32, meta: &StreamBitmapMeta) -> Result<()> {
        let key = BitmapPageMetaSpec::key(stream, page_start);
        let encoded = meta.encode();
        let _ = self
            .table
            .put(&key, encoded.clone(), crate::store::traits::PutCond::Any)
            .await?;
        self.cache.put(&key, encoded.clone(), encoded.len());
        Ok(())
    }
}

pub struct BitmapPageBlobTable<B: BlobStore> {
    blob_table: BlobTable<B>,
    cache: HashMapTableBytesCache,
}

pub struct TraceBitmapPageBlobTable<B: BlobStore> {
    blob_table: BlobTable<B>,
    cache: HashMapTableBytesCache,
}

impl<B: BlobStore> TraceBitmapPageBlobTable<B> {
    pub async fn get_for_page(&self, stream: &str, page_start: u32) -> Result<Option<Bytes>> {
        let key = TraceBitmapPageBlobSpec::key(stream, page_start);
        self.get_by_key(&key).await
    }

    pub async fn get_by_key(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if let Some(bytes) = self.cache.get(key) {
            return Ok(Some(bytes));
        }
        let Some(bytes) = self.blob_table.get(key).await? else {
            return Ok(None);
        };
        self.cache.put(key, bytes.clone(), bytes.len());
        Ok(Some(bytes))
    }

    pub async fn put(&self, stream: &str, page_start: u32, bytes: Bytes) -> Result<()> {
        let key = TraceBitmapPageBlobSpec::key(stream, page_start);
        self.blob_table.put(&key, bytes.clone()).await?;
        self.cache.put(&key, bytes.clone(), bytes.len());
        Ok(())
    }
}

pub struct BlockTraceBlobTable<M: MetaStore, B: BlobStore> {
    blob_table: BlobTable<B>,
    block_trace_headers: BlockTraceHeaderTable<M>,
}

impl<M: MetaStore, B: BlobStore> BlockTraceBlobTable<M, B> {
    pub async fn get(&self, block_num: u64) -> Result<Option<Bytes>> {
        self.blob_table.get(&BlockTraceBlobSpec::key(block_num)).await
    }

    pub async fn put_block(&self, block_num: u64, block_blob: Bytes, header: &BlockTraceHeader) -> Result<()> {
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
        let key = BitmapPageBlobSpec::key(stream, page_start);
        self.get_by_key(&key).await
    }

    pub async fn get_by_key(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if let Some(bytes) = self.cache.get(key) {
            return Ok(Some(bytes));
        }

        let Some(bytes) = self.blob_table.get(key).await? else {
            return Ok(None);
        };
        self.cache.put(key, bytes.clone(), bytes.len());
        Ok(Some(bytes))
    }

    pub async fn put(&self, stream: &str, page_start: u32, bytes: Bytes) -> Result<()> {
        let key = BitmapPageBlobSpec::key(stream, page_start);
        self.blob_table.put(&key, bytes.clone()).await?;
        self.cache.put(&key, bytes.clone(), bytes.len());
        Ok(())
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
