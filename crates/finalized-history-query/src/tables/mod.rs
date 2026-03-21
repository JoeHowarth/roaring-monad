use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use quick_cache::sync::Cache;
use quick_cache::{DefaultHashBuilder, Lifecycle, OptionsBuilder, Weighter};

use crate::codec::log_ref::{BlockLogHeaderRef, DirBucketRef, LogRef};
use crate::domain::keys::{
    BITMAP_BY_BLOCK_FAMILY, BITMAP_PAGE_META_FAMILY, BLOCK_LOG_HEADER_FAMILY, BLOCK_RECORD_FAMILY,
    LOG_DIR_BUCKET_FAMILY, LOG_DIR_BY_BLOCK_FAMILY, LOG_DIR_SUB_BUCKET_FAMILY,
    bitmap_by_block_partition_key, bitmap_page_blob_key, bitmap_page_meta_suffix,
    block_log_blob_key, block_log_header_suffix, block_record_suffix, log_dir_bucket_suffix,
    log_dir_by_block_partition_key, log_dir_sub_bucket_suffix, point_log_payload_cache_key,
};
use crate::domain::types::{BlockRecord, DirByBlock, StreamBitmapMeta};
use crate::error::{Error, Result};
use crate::store::traits::{BlobStore, KvTable, MetaStore, ScannableKvTable};

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

pub struct Tables<M: MetaStore, B: BlobStore> {
    block_records: BlockRecordTable<M>,
    block_log_headers: BlockLogHeaderTable<M>,
    dir_buckets: DirBucketTable<M>,
    log_dir_sub_buckets: LogDirSubBucketTable<M>,
    directory_fragments: DirectoryFragmentTable<M>,
    point_log_payloads: PointLogPayloadTable<M, B>,
    bitmap_by_block: BitmapByBlockTable<M>,
    bitmap_page_meta: BitmapPageMetaTable<M>,
    bitmap_page_blobs: BitmapPageBlobTable<B>,
}

impl<M: MetaStore, B: BlobStore> Tables<M, B> {
    pub fn without_cache(meta_store: Arc<M>, blob_store: Arc<B>) -> Self {
        let block_records = BlockRecordTable {
            table: meta_store.clone().table(BLOCK_RECORD_FAMILY),
            cache: HashMapTableBytesCache::default(),
        };
        let block_log_headers = BlockLogHeaderTable {
            table: meta_store.clone().table(BLOCK_LOG_HEADER_FAMILY),
            cache: HashMapTableBytesCache::default(),
        };
        Self {
            block_records,
            block_log_headers: block_log_headers.clone(),
            dir_buckets: DirBucketTable {
                table: meta_store.clone().table(LOG_DIR_BUCKET_FAMILY),
                cache: HashMapTableBytesCache::default(),
            },
            log_dir_sub_buckets: LogDirSubBucketTable {
                table: meta_store.clone().table(LOG_DIR_SUB_BUCKET_FAMILY),
                cache: HashMapTableBytesCache::default(),
            },
            directory_fragments: DirectoryFragmentTable {
                table: meta_store.clone().scannable_table(LOG_DIR_BY_BLOCK_FAMILY),
            },
            point_log_payloads: PointLogPayloadTable {
                blob_store: Arc::clone(&blob_store),
                cache: HashMapTableBytesCache::default(),
                block_log_headers,
            },
            bitmap_by_block: BitmapByBlockTable {
                table: meta_store.clone().scannable_table(BITMAP_BY_BLOCK_FAMILY),
            },
            bitmap_page_meta: BitmapPageMetaTable {
                table: meta_store.table(BITMAP_PAGE_META_FAMILY),
                cache: HashMapTableBytesCache::default(),
            },
            bitmap_page_blobs: BitmapPageBlobTable {
                blob_store,
                cache: HashMapTableBytesCache::default(),
            },
        }
    }

    pub fn new(meta_store: Arc<M>, blob_store: Arc<B>, config: BytesCacheConfig) -> Self {
        let block_records = BlockRecordTable {
            table: meta_store.clone().table(BLOCK_RECORD_FAMILY),
            cache: HashMapTableBytesCache::new(config.block_records.max_bytes),
        };
        let block_log_headers = BlockLogHeaderTable {
            table: meta_store.clone().table(BLOCK_LOG_HEADER_FAMILY),
            cache: HashMapTableBytesCache::new(config.block_log_header.max_bytes),
        };
        Self {
            block_records,
            dir_buckets: DirBucketTable {
                table: meta_store.clone().table(LOG_DIR_BUCKET_FAMILY),
                cache: HashMapTableBytesCache::new(config.log_dir_buckets.max_bytes),
            },
            log_dir_sub_buckets: LogDirSubBucketTable {
                table: meta_store.clone().table(LOG_DIR_SUB_BUCKET_FAMILY),
                cache: HashMapTableBytesCache::new(config.log_dir_sub_buckets.max_bytes),
            },
            directory_fragments: DirectoryFragmentTable {
                table: meta_store.clone().scannable_table(LOG_DIR_BY_BLOCK_FAMILY),
            },
            point_log_payloads: PointLogPayloadTable {
                blob_store: Arc::clone(&blob_store),
                cache: HashMapTableBytesCache::new(config.point_log_payloads.max_bytes),
                block_log_headers: block_log_headers.clone(),
            },
            bitmap_by_block: BitmapByBlockTable {
                table: meta_store.clone().scannable_table(BITMAP_BY_BLOCK_FAMILY),
            },
            bitmap_page_meta: BitmapPageMetaTable {
                table: meta_store.table(BITMAP_PAGE_META_FAMILY),
                cache: HashMapTableBytesCache::new(config.bitmap_page_meta.max_bytes),
            },
            bitmap_page_blobs: BitmapPageBlobTable {
                blob_store,
                cache: HashMapTableBytesCache::new(config.bitmap_page_blobs.max_bytes),
            },
            block_log_headers,
        }
    }

    pub fn block_records(&self) -> &BlockRecordTable<M> {
        &self.block_records
    }

    pub fn block_log_headers(&self) -> &BlockLogHeaderTable<M> {
        &self.block_log_headers
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

    pub fn point_log_payloads(&self) -> &PointLogPayloadTable<M, B> {
        &self.point_log_payloads
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
        let key = block_record_suffix(block_num);
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
}

pub struct BlockLogHeaderTable<M> {
    table: KvTable<M>,
    cache: HashMapTableBytesCache,
}

impl<M> Clone for BlockLogHeaderTable<M> {
    fn clone(&self) -> Self {
        Self {
            table: self.table.clone(),
            cache: self.cache.clone(),
        }
    }
}

impl<M: MetaStore> BlockLogHeaderTable<M> {
    pub async fn get(&self, block_num: u64) -> Result<Option<BlockLogHeaderRef>> {
        let key = block_log_header_suffix(block_num);
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
}

pub struct DirBucketTable<M> {
    table: KvTable<M>,
    cache: HashMapTableBytesCache,
}

impl<M: MetaStore> DirBucketTable<M> {
    pub async fn get(&self, bucket_start: u64) -> Result<Option<DirBucketRef>> {
        let key = log_dir_bucket_suffix(bucket_start);
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
}

pub struct LogDirSubBucketTable<M> {
    table: KvTable<M>,
    cache: HashMapTableBytesCache,
}

impl<M: MetaStore> LogDirSubBucketTable<M> {
    pub async fn get(&self, sub_bucket_start: u64) -> Result<Option<DirBucketRef>> {
        let key = log_dir_sub_bucket_suffix(sub_bucket_start);
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
}

pub struct DirectoryFragmentTable<M> {
    table: ScannableKvTable<M>,
}

impl<M: MetaStore> DirectoryFragmentTable<M> {
    pub async fn load_sub_bucket_fragments(
        &self,
        sub_bucket_start: u64,
    ) -> Result<Vec<DirByBlock>> {
        let partition = log_dir_by_block_partition_key(sub_bucket_start);
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
}

pub struct BitmapByBlockTable<M> {
    table: ScannableKvTable<M>,
}

impl<M: MetaStore> BitmapByBlockTable<M> {
    pub async fn load_page_fragments(&self, stream: &str, page_start: u32) -> Result<Vec<Bytes>> {
        let partition = bitmap_by_block_partition_key(stream, page_start);
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
}

pub struct BitmapPageMetaTable<M> {
    table: KvTable<M>,
    cache: HashMapTableBytesCache,
}

impl<M: MetaStore> BitmapPageMetaTable<M> {
    pub async fn get(&self, stream: &str, page_start: u32) -> Result<Option<StreamBitmapMeta>> {
        let key = bitmap_page_meta_suffix(stream, page_start);
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
}

pub struct BitmapPageBlobTable<B: BlobStore> {
    blob_store: Arc<B>,
    cache: HashMapTableBytesCache,
}

impl<B: BlobStore> BitmapPageBlobTable<B> {
    pub async fn get_for_page(&self, stream: &str, page_start: u32) -> Result<Option<Bytes>> {
        let key = bitmap_page_blob_key(stream, page_start);
        self.get_by_key(&key).await
    }

    pub async fn get_by_key(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if let Some(bytes) = self.cache.get(key) {
            return Ok(Some(bytes));
        }

        let Some(bytes) = self.blob_store.get_blob(key).await? else {
            return Ok(None);
        };
        self.cache.put(key, bytes.clone(), bytes.len());
        Ok(Some(bytes))
    }
}

pub struct PointLogPayloadTable<M: MetaStore, B: BlobStore> {
    blob_store: Arc<B>,
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
            .blob_store
            .read_range(
                &block_log_blob_key(block_num),
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
