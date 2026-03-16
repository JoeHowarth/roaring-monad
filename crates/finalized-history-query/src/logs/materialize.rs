use std::collections::HashMap;

use crate::cache::{BytesCache, TableId};
use crate::codec::log::decode_log_dir_fragment;
use crate::codec::log_ref::{BlockLogHeaderRef, LogDirectoryBucketRef, LogRef};
use crate::core::execution::PrimaryMaterializer;
use crate::core::ids::LogId;
use crate::core::range::RangeResolver;
use crate::core::refs::BlockRef;
use crate::domain::keys::{
    block_log_header_key, block_logs_blob_key, log_directory_bucket_key,
    log_directory_bucket_start, log_directory_fragment_prefix, log_directory_sub_bucket_key,
    log_directory_sub_bucket_start, point_log_payload_cache_key,
};
use crate::domain::types::LogDirFragment;
use crate::error::{Error, Result};
use crate::logs::filter::{LogFilter, exact_match};
use crate::logs::state::load_log_block_meta;
use crate::store::traits::{BlobStore, MetaStore};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ResolvedLogLocation {
    pub block_num: u64,
    pub local_ordinal: usize,
}

pub struct LogMaterializer<'a, M: MetaStore, B: BlobStore, C: BytesCache> {
    meta_store: &'a M,
    blob_store: &'a B,
    cache: &'a C,
    range_resolver: RangeResolver,
    // directory_fragment_cache stays as a per-request HashMap because fragments
    // are assembled from multiple list_prefix + get calls (not a single stored
    // value), and each LogDirFragment is only 25 bytes. Not worth BytesCache.
    directory_fragment_cache: HashMap<u64, Vec<LogDirFragment>>,
    // block_ref_cache remains because BlockRef is a small Copy type
    // computed from multiple sources, not a direct decode of stored bytes.
    block_ref_cache: HashMap<u64, BlockRef>,
}

impl<'a, M: MetaStore, B: BlobStore, C: BytesCache> LogMaterializer<'a, M, B, C> {
    pub fn new(meta_store: &'a M, blob_store: &'a B, cache: &'a C) -> Self {
        Self {
            meta_store,
            blob_store,
            cache,
            range_resolver: RangeResolver,
            directory_fragment_cache: HashMap::new(),
            block_ref_cache: HashMap::new(),
        }
    }

    pub(crate) async fn load_block_header(
        &mut self,
        block_num: u64,
    ) -> Result<Option<BlockLogHeaderRef>> {
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

    pub(crate) async fn resolve_log_id(
        &mut self,
        id: LogId,
    ) -> Result<Option<ResolvedLogLocation>> {
        let bucket_start = log_directory_bucket_start(id);
        if let Some(bucket) = self
            .load_directory_bucket(bucket_start, TableId::LogDirectoryBuckets)
            .await?
            && let Some(entry_index) = containing_bucket_entry_ref(&bucket, id)
        {
            return resolved_location_from_bucket_ref(&bucket, entry_index, id);
        }

        let sub_bucket_start = log_directory_sub_bucket_start(id);
        if let Some(bucket) = self.load_directory_sub_bucket(sub_bucket_start).await?
            && let Some(entry_index) = containing_bucket_entry_ref(&bucket, id)
        {
            return resolved_location_from_bucket_ref(&bucket, entry_index, id);
        }

        let fragments = self.load_directory_fragments(sub_bucket_start).await?;
        let Some(fragment) = fragments.iter().find(|fragment| {
            id.get() >= fragment.first_log_id && id.get() < fragment.end_log_id_exclusive
        }) else {
            return Ok(None);
        };
        Ok(Some(ResolvedLogLocation {
            block_num: fragment.block_num,
            local_ordinal: usize::try_from(id.get() - fragment.first_log_id)
                .map_err(|_| Error::Decode("local ordinal overflow"))?,
        }))
    }

    async fn load_directory_bucket(
        &self,
        bucket_start: u64,
        table: TableId,
    ) -> Result<Option<LogDirectoryBucketRef>> {
        let key = log_directory_bucket_key(bucket_start);
        if let Some(bytes) = self.cache.get(table, &key) {
            return Ok(Some(LogDirectoryBucketRef::new(bytes)?));
        }
        let Some(record) = self.meta_store.get(&key).await? else {
            return Ok(None);
        };
        self.cache
            .put(table, &key, record.value.clone(), record.value.len());
        Ok(Some(LogDirectoryBucketRef::new(record.value)?))
    }

    async fn load_directory_sub_bucket(
        &self,
        sub_bucket_start: u64,
    ) -> Result<Option<LogDirectoryBucketRef>> {
        let key = log_directory_sub_bucket_key(sub_bucket_start);
        let table = TableId::LogDirectorySubBuckets;
        if let Some(bytes) = self.cache.get(table, &key) {
            return Ok(Some(LogDirectoryBucketRef::new(bytes)?));
        }
        let Some(record) = self.meta_store.get(&key).await? else {
            return Ok(None);
        };
        self.cache
            .put(table, &key, record.value.clone(), record.value.len());
        Ok(Some(LogDirectoryBucketRef::new(record.value)?))
    }

    async fn load_directory_fragments(
        &mut self,
        sub_bucket_start: u64,
    ) -> Result<&[LogDirFragment]> {
        if let std::collections::hash_map::Entry::Vacant(entry) =
            self.directory_fragment_cache.entry(sub_bucket_start)
        {
            let page = self
                .meta_store
                .list_prefix(
                    &log_directory_fragment_prefix(sub_bucket_start),
                    None,
                    usize::MAX,
                )
                .await?;
            let mut fragments = Vec::with_capacity(page.keys.len());
            for key in page.keys {
                let Some(record) = self.meta_store.get(&key).await? else {
                    continue;
                };
                fragments.push(decode_log_dir_fragment(&record.value)?);
            }
            fragments.sort_by_key(|fragment| fragment.block_num);
            entry.insert(fragments);
        }
        Ok(self
            .directory_fragment_cache
            .get(&sub_bucket_start)
            .map(Vec::as_slice)
            .unwrap_or(&[]))
    }

    fn point_log_payload_key(&self, block_num: u64, local_ordinal: usize) -> Result<Vec<u8>> {
        let local_ordinal =
            u64::try_from(local_ordinal).map_err(|_| Error::Decode("local ordinal overflow"))?;
        Ok(point_log_payload_cache_key(block_num, local_ordinal))
    }
}

fn containing_bucket_entry_ref(bucket: &LogDirectoryBucketRef, id: LogId) -> Option<usize> {
    if bucket.count() < 2 {
        return None;
    }
    let upper = bucket.partition_point(|first_log_id| first_log_id <= id.get());
    if upper == 0 || upper >= bucket.count() {
        return None;
    }
    let entry_index = upper - 1;
    let end = bucket.first_log_id(upper);
    if id.get() < end {
        Some(entry_index)
    } else {
        None
    }
}

fn resolved_location_from_bucket_ref(
    bucket: &LogDirectoryBucketRef,
    entry_index: usize,
    id: LogId,
) -> Result<Option<ResolvedLogLocation>> {
    let block_num = bucket.start_block() + entry_index as u64;
    let local_ordinal = usize::try_from(id.get() - bucket.first_log_id(entry_index))
        .map_err(|_| Error::Decode("local ordinal overflow"))?;
    Ok(Some(ResolvedLogLocation {
        block_num,
        local_ordinal,
    }))
}

impl<M: MetaStore, B: BlobStore, C: BytesCache> PrimaryMaterializer
    for LogMaterializer<'_, M, B, C>
{
    type Primary = LogRef;
    type Filter = LogFilter;

    async fn load_by_id(&mut self, id: LogId) -> Result<Option<Self::Primary>> {
        let Some(location) = self.resolve_log_id(id).await? else {
            return Ok(None);
        };
        let payload_key = self.point_log_payload_key(location.block_num, location.local_ordinal)?;
        if let Some(bytes) = self.cache.get(TableId::PointLogPayloads, &payload_key) {
            return Ok(Some(LogRef::new(bytes)?));
        }

        let Some(header) = self.load_block_header(location.block_num).await? else {
            return Ok(None);
        };
        if location.local_ordinal >= header.count() {
            return Ok(None);
        }
        let start = header.offset(location.local_ordinal);
        if location.local_ordinal + 1 >= header.count() {
            return Ok(None);
        }
        let end = header.offset(location.local_ordinal + 1);
        let Some(bytes) = self
            .blob_store
            .read_range(
                &block_logs_blob_key(location.block_num),
                u64::from(start),
                u64::from(end),
            )
            .await?
        else {
            return Ok(None);
        };
        self.cache.put(
            TableId::PointLogPayloads,
            &payload_key,
            bytes.clone(),
            bytes.len(),
        );
        Ok(Some(LogRef::new(bytes)?))
    }

    async fn block_ref_for(&mut self, item: &Self::Primary) -> Result<BlockRef> {
        let block_num = item.block_num();
        if let Some(block_ref) = self.block_ref_cache.get(&block_num).copied() {
            return Ok(block_ref);
        }

        let block_ref = if let Some(block_ref) = self
            .range_resolver
            .load_block_ref(self.meta_store, block_num)
            .await?
        {
            block_ref
        } else {
            let Some(block_meta) = load_log_block_meta(self.meta_store, block_num).await? else {
                return Err(Error::NotFound);
            };
            BlockRef {
                number: block_num,
                hash: *item.block_hash(),
                parent_hash: block_meta.parent_hash,
            }
        };
        self.block_ref_cache.insert(block_num, block_ref);
        Ok(block_ref)
    }

    fn exact_match(&self, item: &Self::Primary, filter: &Self::Filter) -> bool {
        exact_match(item, filter)
    }
}

#[cfg(test)]
mod tests {
    use super::LogMaterializer;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use bytes::Bytes;

    use crate::cache::{BytesCacheConfig, HashMapBytesCache, NoopBytesCache, TableCacheConfig};
    use crate::codec::log::{
        encode_block_log_header, encode_log, encode_log_dir_fragment, encode_log_directory_bucket,
    };
    use crate::core::execution::PrimaryMaterializer;
    use crate::core::ids::LogId;
    use crate::domain::keys::{
        LOG_DIRECTORY_BUCKET_SIZE, LOG_DIRECTORY_SUB_BUCKET_SIZE, block_log_header_key,
        block_logs_blob_key, log_directory_bucket_key, log_directory_bucket_start,
        log_directory_fragment_key, log_directory_sub_bucket_start,
    };
    use crate::domain::types::{BlockLogHeader, Log, LogDirFragment, LogDirectoryBucket};
    use crate::store::blob::InMemoryBlobStore;
    use crate::store::meta::InMemoryMetaStore;
    use crate::store::traits::{BlobStore, CreateOutcome, FenceToken, MetaStore, Page, PutCond};
    use futures::executor::block_on;

    struct CountingBlobStore {
        inner: InMemoryBlobStore,
        get_blob_count: Arc<AtomicU64>,
        read_range_count: Arc<AtomicU64>,
    }

    impl BlobStore for CountingBlobStore {
        async fn put_blob(&self, key: &[u8], value: Bytes) -> crate::Result<()> {
            self.inner.put_blob(key, value).await
        }

        async fn put_blob_if_absent(
            &self,
            key: &[u8],
            value: Bytes,
        ) -> crate::Result<CreateOutcome> {
            self.inner.put_blob_if_absent(key, value).await
        }

        async fn get_blob(&self, key: &[u8]) -> crate::Result<Option<Bytes>> {
            if key.starts_with(b"block_logs/") {
                self.get_blob_count.fetch_add(1, Ordering::Relaxed);
            }
            self.inner.get_blob(key).await
        }

        async fn read_range(
            &self,
            key: &[u8],
            start: u64,
            end_exclusive: u64,
        ) -> crate::Result<Option<Bytes>> {
            if key.starts_with(b"block_logs/") {
                self.read_range_count.fetch_add(1, Ordering::Relaxed);
            }
            self.inner.read_range(key, start, end_exclusive).await
        }

        async fn delete_blob(&self, key: &[u8]) -> crate::Result<()> {
            self.inner.delete_blob(key).await
        }

        async fn list_prefix(
            &self,
            prefix: &[u8],
            cursor: Option<Vec<u8>>,
            limit: usize,
        ) -> crate::Result<Page> {
            self.inner.list_prefix(prefix, cursor, limit).await
        }
    }

    #[test]
    fn resolve_log_id_prefers_1m_bucket_summary_when_present() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let blob = InMemoryBlobStore::default();
            let cache = NoopBytesCache;

            meta.put(
                &log_directory_bucket_key(0),
                encode_log_directory_bucket(&LogDirectoryBucket {
                    start_block: 700,
                    first_log_ids: vec![11, 13],
                }),
                PutCond::Any,
                FenceToken(1),
            )
            .await
            .expect("write directory bucket");

            let mut materializer = LogMaterializer::new(&meta, &blob, &cache);
            let resolved = materializer
                .resolve_log_id(LogId::new(12))
                .await
                .expect("resolve log id");

            assert_eq!(
                resolved,
                Some(super::ResolvedLogLocation {
                    block_num: 700,
                    local_ordinal: 1,
                })
            );
        });
    }

    #[test]
    fn resolve_log_id_handles_sub_bucket_fragments() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let blob = InMemoryBlobStore::default();
            let cache = NoopBytesCache;
            let log_id = LogId::new(LOG_DIRECTORY_SUB_BUCKET_SIZE + 5);
            meta.put(
                &log_directory_fragment_key(log_directory_sub_bucket_start(log_id), 700),
                encode_log_dir_fragment(&LogDirFragment {
                    block_num: 700,
                    first_log_id: LOG_DIRECTORY_SUB_BUCKET_SIZE,
                    end_log_id_exclusive: LOG_DIRECTORY_SUB_BUCKET_SIZE + 10,
                }),
                PutCond::Any,
                FenceToken(1),
            )
            .await
            .expect("write directory fragment");

            let mut materializer = LogMaterializer::new(&meta, &blob, &cache);
            let resolved = materializer
                .resolve_log_id(log_id)
                .await
                .expect("resolve log id");

            assert_eq!(
                resolved,
                Some(super::ResolvedLogLocation {
                    block_num: 700,
                    local_ordinal: 5,
                })
            );
        });
    }

    #[test]
    fn resolve_log_id_handles_blocks_spanning_more_than_one_bucket() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let blob = InMemoryBlobStore::default();
            let cache = NoopBytesCache;

            let first_log_id = LOG_DIRECTORY_BUCKET_SIZE - 3;
            let log_id = LogId::new(first_log_id + LOG_DIRECTORY_BUCKET_SIZE + 5);
            meta.put(
                &log_directory_bucket_key(log_directory_bucket_start(log_id)),
                encode_log_directory_bucket(&LogDirectoryBucket {
                    start_block: 700,
                    first_log_ids: vec![
                        first_log_id,
                        first_log_id + LOG_DIRECTORY_BUCKET_SIZE + 10,
                    ],
                }),
                PutCond::Any,
                FenceToken(1),
            )
            .await
            .expect("write directory bucket");

            let mut materializer = LogMaterializer::new(&meta, &blob, &cache);
            let resolved = materializer
                .resolve_log_id(log_id)
                .await
                .expect("resolve log id");

            assert_eq!(
                resolved,
                Some(super::ResolvedLogLocation {
                    block_num: 700,
                    local_ordinal: (LOG_DIRECTORY_BUCKET_SIZE + 5) as usize,
                })
            );
        });
    }

    #[test]
    fn load_by_id_caches_point_log_payload_without_full_blob_reads() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let blob = CountingBlobStore {
                inner: InMemoryBlobStore::default(),
                get_blob_count: Arc::new(AtomicU64::new(0)),
                read_range_count: Arc::new(AtomicU64::new(0)),
            };
            let cache = HashMapBytesCache::new(BytesCacheConfig {
                point_log_payloads: TableCacheConfig {
                    max_bytes: 4 * 1024,
                },
                ..BytesCacheConfig::disabled()
            });
            let block_num = 700u64;
            let log_id = LogId::new(LOG_DIRECTORY_SUB_BUCKET_SIZE);
            let log = Log {
                address: [7u8; 20],
                topics: vec![[8u8; 32]],
                data: vec![1, 2, 3],
                block_num,
                tx_idx: 1,
                log_idx: 2,
                block_hash: [9u8; 32],
            };
            let encoded = encode_log(&log);

            meta.put(
                &log_directory_fragment_key(log_directory_sub_bucket_start(log_id), block_num),
                encode_log_dir_fragment(&LogDirFragment {
                    block_num,
                    first_log_id: LOG_DIRECTORY_SUB_BUCKET_SIZE,
                    end_log_id_exclusive: LOG_DIRECTORY_SUB_BUCKET_SIZE + 1,
                }),
                PutCond::Any,
                FenceToken(1),
            )
            .await
            .expect("write directory fragment");
            meta.put(
                &block_log_header_key(block_num),
                encode_block_log_header(&BlockLogHeader {
                    offsets: vec![0, encoded.len() as u32],
                }),
                PutCond::Any,
                FenceToken(1),
            )
            .await
            .expect("write block log header");
            blob.put_blob(&block_logs_blob_key(block_num), encoded.clone())
                .await
                .expect("write block log blob");

            let mut materializer = LogMaterializer::new(&meta, &blob, &cache);
            let first = PrimaryMaterializer::load_by_id(&mut materializer, log_id)
                .await
                .expect("first load")
                .expect("first log");
            let second = PrimaryMaterializer::load_by_id(&mut materializer, log_id)
                .await
                .expect("second load")
                .expect("second log");

            assert_eq!(first, second);
            assert_eq!(blob.get_blob_count.load(Ordering::Relaxed), 0);
            assert_eq!(blob.read_range_count.load(Ordering::Relaxed), 1);

            let metrics = cache.metrics_snapshot();
            assert_eq!(metrics.point_log_payloads.misses, 1);
            assert_eq!(metrics.point_log_payloads.hits, 1);
            assert_eq!(metrics.point_log_payloads.inserts, 1);
        });
    }
}
