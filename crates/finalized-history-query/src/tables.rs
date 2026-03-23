use bytes::Bytes;

use crate::codec::StorageCodec;
use crate::codec::encode_u64;
use crate::error::{Error, Result};
use crate::kernel::blob_table::CachedBlobTable;
pub use crate::kernel::cache::{
    BytesCacheConfig, BytesCacheMetrics, HashMapTableBytesCache, TableCacheConfig,
    TableCacheMetrics,
};
use crate::kernel::cache::{cache_for, no_cache};
use crate::kernel::point_table::CachedPointTable;
use crate::kernel::scannable_table::ScannableFragmentTable;
use crate::logs::keys::read_u64_be;
use crate::logs::log_ref::{BlockLogHeaderRef, DirBucketRef, LogRef};
use crate::logs::table_specs::{
    BitmapByBlockSpec, BitmapPageBlobSpec, BitmapPageMetaSpec, BlockHashIndexSpec,
    BlockLogBlobSpec, BlockLogHeaderSpec, BlockRecordSpec, LogDirBucketSpec, LogDirByBlockSpec,
    LogDirSubBucketSpec,
};
use crate::logs::types::{BlockRecord, DirByBlock, StreamBitmapMeta};
use crate::store::traits::{
    BlobStore, BlobTable, BlobTableId, KvTable, MetaStore, ScannableKvTable, ScannableTableId,
    TableId,
};
use crate::traces::table_specs::{
    BlockTraceBlobSpec, BlockTraceHeaderSpec, TraceBitmapByBlockSpec, TraceBitmapPageBlobSpec,
    TraceBitmapPageMetaSpec, TraceBlockRecordSpec, TraceDirBucketSpec, TraceDirByBlockSpec,
    TraceDirSubBucketSpec,
};
use crate::traces::types::{
    BlockTraceHeader, DirBucket as TraceDirBucket, DirByBlock as TraceDirByBlock,
    StreamBitmapMeta as TraceStreamBitmapMeta, TraceBlockRecord,
};

pub trait PointTableSpec {
    const TABLE: TableId;
}

pub trait ScannableTableSpec {
    const TABLE: ScannableTableId;
}

pub trait BlobTableSpec {
    const TABLE: BlobTableId;
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
        Self::new(meta_store, blob_store, BytesCacheConfig::disabled())
    }

    pub fn new(meta_store: M, blob_store: B, config: BytesCacheConfig) -> Self {
        let block_records = BlockRecordTable::new(
            meta_store.table(BlockRecordSpec::TABLE),
            cache_for(config.block_records.max_bytes),
        );
        let block_log_headers = BlockLogHeaderTable::new(
            meta_store.table(BlockLogHeaderSpec::TABLE),
            cache_for(config.block_log_header.max_bytes),
        );
        let block_trace_headers =
            BlockTraceHeaderTable::new(meta_store.table(BlockTraceHeaderSpec::TABLE), no_cache());

        Self {
            block_hash_index: BlockHashIndexTable {
                table: meta_store.table(BlockHashIndexSpec::TABLE),
            },
            block_records,
            block_log_headers: block_log_headers.clone(),
            trace_block_records: TraceBlockRecordTable::new(
                meta_store.table(TraceBlockRecordSpec::TABLE),
                no_cache(),
            ),
            block_trace_headers: block_trace_headers.clone(),
            dir_buckets: DirBucketTable::new(
                meta_store.table(LogDirBucketSpec::TABLE),
                cache_for(config.log_dir_buckets.max_bytes),
            ),
            log_dir_sub_buckets: LogDirSubBucketTable::new(
                meta_store.table(LogDirSubBucketSpec::TABLE),
                cache_for(config.log_dir_sub_buckets.max_bytes),
            ),
            directory_fragments: DirectoryFragmentTable::new(
                meta_store.scannable_table(LogDirByBlockSpec::TABLE),
            ),
            trace_dir_buckets: TraceDirBucketTable::new(
                meta_store.table(TraceDirBucketSpec::TABLE),
                no_cache(),
            ),
            trace_dir_sub_buckets: TraceDirSubBucketTable::new(
                meta_store.table(TraceDirSubBucketSpec::TABLE),
                no_cache(),
            ),
            trace_directory_fragments: TraceDirectoryFragmentTable::new(
                meta_store.scannable_table(TraceDirByBlockSpec::TABLE),
            ),
            point_log_payloads: PointLogPayloadTable {
                blob_table: blob_store.table(BlockLogBlobSpec::TABLE),
                cache: cache_for(config.point_log_payloads.max_bytes),
                block_log_headers,
            },
            block_trace_blobs: BlockTraceBlobTable {
                blob_table: blob_store.table(BlockTraceBlobSpec::TABLE),
                block_trace_headers,
            },
            bitmap_by_block: BitmapByBlockTable::new(
                meta_store.scannable_table(BitmapByBlockSpec::TABLE),
            ),
            bitmap_page_meta: BitmapPageMetaTable::new(
                meta_store.table(BitmapPageMetaSpec::TABLE),
                cache_for(config.bitmap_page_meta.max_bytes),
            ),
            bitmap_page_blobs: BitmapPageBlobTable::new(
                blob_store.table(BitmapPageBlobSpec::TABLE),
                cache_for(config.bitmap_page_blobs.max_bytes),
            ),
            trace_bitmap_by_block: TraceBitmapByBlockTable::new(
                meta_store.scannable_table(TraceBitmapByBlockSpec::TABLE),
            ),
            trace_bitmap_page_meta: TraceBitmapPageMetaTable::new(
                meta_store.table(TraceBitmapPageMetaSpec::TABLE),
                no_cache(),
            ),
            trace_bitmap_page_blobs: TraceBitmapPageBlobTable::new(
                blob_store.table(TraceBitmapPageBlobSpec::TABLE),
                no_cache(),
            ),
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
            bitmap_page_blobs: self.bitmap_page_blobs.metrics(),
        }
    }
}

pub struct BlockRecordTable<M: MetaStore>(CachedPointTable<M, BlockRecord>);

impl<M: MetaStore> BlockRecordTable<M> {
    fn new(table: KvTable<M>, cache: HashMapTableBytesCache) -> Self {
        Self(CachedPointTable::new(table, cache))
    }

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

pub struct BlockLogHeaderTable<M: MetaStore>(CachedPointTable<M, Bytes>);

impl<M: MetaStore> BlockLogHeaderTable<M> {
    fn new(table: KvTable<M>, cache: HashMapTableBytesCache) -> Self {
        Self(CachedPointTable::new(table, cache))
    }

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

impl<M: MetaStore> Clone for BlockLogHeaderTable<M> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

pub struct TraceBlockRecordTable<M: MetaStore>(CachedPointTable<M, TraceBlockRecord>);

impl<M: MetaStore> TraceBlockRecordTable<M> {
    fn new(table: KvTable<M>, cache: HashMapTableBytesCache) -> Self {
        Self(CachedPointTable::new(table, cache))
    }

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

impl<M: MetaStore> BlockTraceHeaderTable<M> {
    fn new(table: KvTable<M>, cache: HashMapTableBytesCache) -> Self {
        Self(CachedPointTable::new(table, cache))
    }

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

impl<M: MetaStore> Clone for BlockTraceHeaderTable<M> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

pub struct DirBucketTable<M: MetaStore>(CachedPointTable<M, Bytes>);

impl<M: MetaStore> DirBucketTable<M> {
    fn new(table: KvTable<M>, cache: HashMapTableBytesCache) -> Self {
        Self(CachedPointTable::new(table, cache))
    }

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

pub struct TraceDirBucketTable<M: MetaStore>(CachedPointTable<M, TraceDirBucket>);

impl<M: MetaStore> TraceDirBucketTable<M> {
    fn new(table: KvTable<M>, cache: HashMapTableBytesCache) -> Self {
        Self(CachedPointTable::new(table, cache))
    }

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

pub struct LogDirSubBucketTable<M: MetaStore>(CachedPointTable<M, Bytes>);

impl<M: MetaStore> LogDirSubBucketTable<M> {
    fn new(table: KvTable<M>, cache: HashMapTableBytesCache) -> Self {
        Self(CachedPointTable::new(table, cache))
    }

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

pub struct TraceDirSubBucketTable<M: MetaStore>(CachedPointTable<M, TraceDirBucket>);

impl<M: MetaStore> TraceDirSubBucketTable<M> {
    fn new(table: KvTable<M>, cache: HashMapTableBytesCache) -> Self {
        Self(CachedPointTable::new(table, cache))
    }

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

pub struct DirectoryFragmentTable<M: MetaStore> {
    inner: ScannableFragmentTable<M>,
}

impl<M: MetaStore> DirectoryFragmentTable<M> {
    fn new(table: ScannableKvTable<M>) -> Self {
        Self {
            inner: ScannableFragmentTable::new(table),
        }
    }

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

pub struct TraceDirectoryFragmentTable<M: MetaStore> {
    inner: ScannableFragmentTable<M>,
}

impl<M: MetaStore> TraceDirectoryFragmentTable<M> {
    fn new(table: ScannableKvTable<M>) -> Self {
        Self {
            inner: ScannableFragmentTable::new(table),
        }
    }

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

pub struct BitmapByBlockTable<M: MetaStore> {
    inner: ScannableFragmentTable<M>,
}

impl<M: MetaStore> BitmapByBlockTable<M> {
    fn new(table: ScannableKvTable<M>) -> Self {
        Self {
            inner: ScannableFragmentTable::new(table),
        }
    }

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

pub struct TraceBitmapByBlockTable<M: MetaStore> {
    inner: ScannableFragmentTable<M>,
}

impl<M: MetaStore> TraceBitmapByBlockTable<M> {
    fn new(table: ScannableKvTable<M>) -> Self {
        Self {
            inner: ScannableFragmentTable::new(table),
        }
    }

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

pub struct BitmapPageMetaTable<M: MetaStore>(CachedPointTable<M, StreamBitmapMeta>);

impl<M: MetaStore> BitmapPageMetaTable<M> {
    fn new(table: KvTable<M>, cache: HashMapTableBytesCache) -> Self {
        Self(CachedPointTable::new(table, cache))
    }

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

pub struct TraceBitmapPageMetaTable<M: MetaStore>(CachedPointTable<M, TraceStreamBitmapMeta>);

impl<M: MetaStore> TraceBitmapPageMetaTable<M> {
    fn new(table: KvTable<M>, cache: HashMapTableBytesCache) -> Self {
        Self(CachedPointTable::new(table, cache))
    }

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

pub struct BitmapPageBlobTable<B: BlobStore> {
    inner: CachedBlobTable<B>,
}

impl<B: BlobStore> BitmapPageBlobTable<B> {
    fn new(table: BlobTable<B>, cache: HashMapTableBytesCache) -> Self {
        Self {
            inner: CachedBlobTable::new(table, cache),
        }
    }

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

    fn metrics(&self) -> TableCacheMetrics {
        self.inner.cache().metrics_snapshot()
    }
}

pub struct TraceBitmapPageBlobTable<B: BlobStore> {
    inner: CachedBlobTable<B>,
}

impl<B: BlobStore> TraceBitmapPageBlobTable<B> {
    fn new(table: BlobTable<B>, cache: HashMapTableBytesCache) -> Self {
        Self {
            inner: CachedBlobTable::new(table, cache),
        }
    }

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

fn point_log_payload_cache_key(block_num: u64, local_ordinal: u64) -> Vec<u8> {
    let mut key = b"point_log_payload/".to_vec();
    key.extend_from_slice(&block_num.to_be_bytes());
    key.extend_from_slice(&local_ordinal.to_be_bytes());
    key
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
