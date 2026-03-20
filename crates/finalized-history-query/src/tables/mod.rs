use bytes::Bytes;

use crate::cache::{HashMapBytesCache, HashMapTableBytesCache};
use crate::codec::log_ref::{BlockLogHeaderRef, DirBucketRef, LogRef};
use crate::domain::keys::{
    bitmap_page_blob_key, bitmap_page_meta_key, block_log_blob_key, block_log_header_key,
    log_dir_bucket_key, log_dir_sub_bucket_key, point_log_payload_cache_key,
};
use crate::domain::types::StreamBitmapMeta;
use crate::error::{Error, Result};
use crate::store::traits::{BlobStore, MetaStore};

pub struct Tables<'a, M: MetaStore, B: BlobStore> {
    meta_store: &'a M,
    blob_store: &'a B,
    caches: &'a HashMapBytesCache,
}

impl<'a, M: MetaStore, B: BlobStore> Copy for Tables<'a, M, B> {}

impl<'a, M: MetaStore, B: BlobStore> Clone for Tables<'a, M, B> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, M: MetaStore, B: BlobStore> Tables<'a, M, B> {
    pub fn new(meta_store: &'a M, blob_store: &'a B, caches: &'a HashMapBytesCache) -> Self {
        Self {
            meta_store,
            blob_store,
            caches,
        }
    }

    pub fn meta_store(&self) -> &'a M {
        self.meta_store
    }

    pub fn blob_store(&self) -> &'a B {
        self.blob_store
    }

    pub fn block_log_headers(&self) -> BlockLogHeaderTable<'a, M> {
        BlockLogHeaderTable {
            meta_store: self.meta_store,
            cache: self.caches.block_log_headers(),
        }
    }

    pub fn dir_buckets(&self) -> DirBucketTable<'a, M> {
        DirBucketTable {
            meta_store: self.meta_store,
            cache: self.caches.dir_buckets(),
        }
    }

    pub fn log_dir_sub_buckets(&self) -> LogDirSubBucketTable<'a, M> {
        LogDirSubBucketTable {
            meta_store: self.meta_store,
            cache: self.caches.log_dir_sub_buckets(),
        }
    }

    pub fn point_log_payloads(&self) -> PointLogPayloadTable<'a, M, B> {
        PointLogPayloadTable {
            blob_store: self.blob_store,
            cache: self.caches.point_log_payloads(),
            block_log_headers: self.block_log_headers(),
        }
    }

    pub fn bitmap_page_meta(&self) -> BitmapPageMetaTable<'a, M> {
        BitmapPageMetaTable {
            meta_store: self.meta_store,
            cache: self.caches.bitmap_page_meta(),
        }
    }

    pub fn bitmap_page_blobs(&self) -> BitmapPageBlobTable<'a, B> {
        BitmapPageBlobTable {
            blob_store: self.blob_store,
            cache: self.caches.bitmap_page_blobs(),
        }
    }
}

#[derive(Clone, Copy)]
pub struct BlockLogHeaderTable<'a, M: MetaStore> {
    meta_store: &'a M,
    cache: &'a HashMapTableBytesCache,
}

impl<M: MetaStore> BlockLogHeaderTable<'_, M> {
    pub async fn get(&self, block_num: u64) -> Result<Option<BlockLogHeaderRef>> {
        let key = block_log_header_key(block_num);
        if let Some(bytes) = self.cache.get(&key) {
            return Ok(Some(BlockLogHeaderRef::new(bytes)?));
        }
        let Some(record) = self.meta_store.get(&key).await? else {
            return Ok(None);
        };
        self.cache
            .put(&key, record.value.clone(), record.value.len());
        Ok(Some(BlockLogHeaderRef::new(record.value)?))
    }
}

#[derive(Clone, Copy)]
pub struct DirBucketTable<'a, M: MetaStore> {
    meta_store: &'a M,
    cache: &'a HashMapTableBytesCache,
}

impl<M: MetaStore> DirBucketTable<'_, M> {
    pub async fn get(&self, bucket_start: u64) -> Result<Option<DirBucketRef>> {
        let key = log_dir_bucket_key(bucket_start);
        if let Some(bytes) = self.cache.get(&key) {
            return Ok(Some(DirBucketRef::new(bytes)?));
        }
        let Some(record) = self.meta_store.get(&key).await? else {
            return Ok(None);
        };
        self.cache
            .put(&key, record.value.clone(), record.value.len());
        Ok(Some(DirBucketRef::new(record.value)?))
    }
}

#[derive(Clone, Copy)]
pub struct LogDirSubBucketTable<'a, M: MetaStore> {
    meta_store: &'a M,
    cache: &'a HashMapTableBytesCache,
}

impl<M: MetaStore> LogDirSubBucketTable<'_, M> {
    pub async fn get(&self, sub_bucket_start: u64) -> Result<Option<DirBucketRef>> {
        let key = log_dir_sub_bucket_key(sub_bucket_start);
        if let Some(bytes) = self.cache.get(&key) {
            return Ok(Some(DirBucketRef::new(bytes)?));
        }
        let Some(record) = self.meta_store.get(&key).await? else {
            return Ok(None);
        };
        self.cache
            .put(&key, record.value.clone(), record.value.len());
        Ok(Some(DirBucketRef::new(record.value)?))
    }
}

#[derive(Clone, Copy)]
pub struct BitmapPageMetaTable<'a, M: MetaStore> {
    meta_store: &'a M,
    cache: &'a HashMapTableBytesCache,
}

impl<M: MetaStore> BitmapPageMetaTable<'_, M> {
    pub async fn get(&self, stream: &str, page_start: u32) -> Result<Option<StreamBitmapMeta>> {
        let key = bitmap_page_meta_key(stream, page_start);
        if let Some(bytes) = self.cache.get(&key) {
            return Ok(Some(StreamBitmapMeta::decode(&bytes)?));
        }

        let Some(record) = self.meta_store.get(&key).await? else {
            return Ok(None);
        };
        self.cache
            .put(&key, record.value.clone(), record.value.len());
        Ok(Some(StreamBitmapMeta::decode(&record.value)?))
    }
}

#[derive(Clone, Copy)]
pub struct BitmapPageBlobTable<'a, B: BlobStore> {
    blob_store: &'a B,
    cache: &'a HashMapTableBytesCache,
}

impl<B: BlobStore> BitmapPageBlobTable<'_, B> {
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

#[derive(Clone, Copy)]
pub struct PointLogPayloadTable<'a, M: MetaStore, B: BlobStore> {
    blob_store: &'a B,
    cache: &'a HashMapTableBytesCache,
    block_log_headers: BlockLogHeaderTable<'a, M>,
}

impl<M: MetaStore, B: BlobStore> PointLogPayloadTable<'_, M, B> {
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
