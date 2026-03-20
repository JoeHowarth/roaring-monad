mod hydrate;
mod resolve;

use std::collections::HashMap;

use crate::cache::BytesCache;
use crate::core::range::RangeResolver;
use crate::core::refs::BlockRef;
use crate::domain::types::DirByBlock;
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
    // value), and each DirByBlock is only 25 bytes. Not worth BytesCache.
    directory_fragment_cache: HashMap<u64, Vec<DirByBlock>>,
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
}

#[cfg(test)]
mod tests {
    use super::LogMaterializer;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use bytes::Bytes;

    use crate::cache::{BytesCacheConfig, HashMapBytesCache, NoopBytesCache, TableCacheConfig};
    use crate::codec::log::{
        encode_block_log_header, encode_dir_by_block, encode_log, encode_log_dir_bucket,
    };
    use crate::core::execution::PrimaryMaterializer;
    use crate::core::ids::LogId;
    use crate::domain::keys::{
        LOG_DIRECTORY_BUCKET_SIZE, LOG_DIRECTORY_SUB_BUCKET_SIZE, block_log_blob_key,
        block_log_header_key, log_dir_bucket_key, log_dir_bucket_start, log_dir_by_block_key,
        log_dir_sub_bucket_start,
    };
    use crate::domain::types::{BlockLogHeader, DirBucket, DirByBlock, Log};
    use crate::store::blob::InMemoryBlobStore;
    use crate::store::meta::InMemoryMetaStore;
    use crate::store::traits::{BlobStore, CreateOutcome, MetaStore, Page, PutCond};
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
            if key.starts_with(b"block_log_blob/") {
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
            if key.starts_with(b"block_log_blob/") {
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
                &log_dir_bucket_key(0),
                encode_log_dir_bucket(&DirBucket {
                    start_block: 700,
                    first_log_ids: vec![11, 13],
                }),
                PutCond::Any,
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
                &log_dir_by_block_key(log_dir_sub_bucket_start(log_id), 700),
                encode_dir_by_block(&DirByBlock {
                    block_num: 700,
                    first_log_id: LOG_DIRECTORY_SUB_BUCKET_SIZE,
                    end_log_id_exclusive: LOG_DIRECTORY_SUB_BUCKET_SIZE + 10,
                }),
                PutCond::Any,
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
                &log_dir_bucket_key(log_dir_bucket_start(log_id)),
                encode_log_dir_bucket(&DirBucket {
                    start_block: 700,
                    first_log_ids: vec![
                        first_log_id,
                        first_log_id + LOG_DIRECTORY_BUCKET_SIZE + 10,
                    ],
                }),
                PutCond::Any,
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
                &log_dir_by_block_key(log_dir_sub_bucket_start(log_id), block_num),
                encode_dir_by_block(&DirByBlock {
                    block_num,
                    first_log_id: LOG_DIRECTORY_SUB_BUCKET_SIZE,
                    end_log_id_exclusive: LOG_DIRECTORY_SUB_BUCKET_SIZE + 1,
                }),
                PutCond::Any,
            )
            .await
            .expect("write directory fragment");
            meta.put(
                &block_log_header_key(block_num),
                encode_block_log_header(&BlockLogHeader {
                    offsets: vec![0, encoded.len() as u32],
                }),
                PutCond::Any,
            )
            .await
            .expect("write block log header");
            blob.put_blob(&block_log_blob_key(block_num), encoded.clone())
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

    #[test]
    fn load_contiguous_run_reads_one_range_and_populates_all_point_payloads() {
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
            let block_num = 701u64;
            let logs = [
                Log {
                    address: [1u8; 20],
                    topics: vec![[11u8; 32]],
                    data: vec![1],
                    block_num,
                    tx_idx: 0,
                    log_idx: 0,
                    block_hash: [9u8; 32],
                },
                Log {
                    address: [2u8; 20],
                    topics: vec![[12u8; 32]],
                    data: vec![2, 2],
                    block_num,
                    tx_idx: 0,
                    log_idx: 1,
                    block_hash: [9u8; 32],
                },
                Log {
                    address: [3u8; 20],
                    topics: vec![[13u8; 32]],
                    data: vec![3, 3, 3],
                    block_num,
                    tx_idx: 0,
                    log_idx: 2,
                    block_hash: [9u8; 32],
                },
            ];
            let encoded_logs = logs.iter().map(encode_log).collect::<Vec<_>>();
            let mut blob_bytes = Vec::new();
            let mut offsets = Vec::with_capacity(encoded_logs.len() + 1);
            offsets.push(0);
            for encoded in &encoded_logs {
                blob_bytes.extend_from_slice(encoded);
                offsets.push(blob_bytes.len() as u32);
            }

            meta.put(
                &block_log_header_key(block_num),
                encode_block_log_header(&BlockLogHeader { offsets }),
                PutCond::Any,
            )
            .await
            .expect("write block log header");
            blob.put_blob(&block_log_blob_key(block_num), Bytes::from(blob_bytes))
                .await
                .expect("write block log blob");

            let mut materializer = LogMaterializer::new(&meta, &blob, &cache);
            let first = materializer
                .load_contiguous_run(block_num, 0, 2)
                .await
                .expect("first contiguous load");
            let second = materializer
                .load_contiguous_run(block_num, 0, 2)
                .await
                .expect("second contiguous load");

            assert_eq!(first, second);
            assert_eq!(blob.get_blob_count.load(Ordering::Relaxed), 0);
            assert_eq!(blob.read_range_count.load(Ordering::Relaxed), 1);

            let metrics = cache.metrics_snapshot();
            assert_eq!(metrics.point_log_payloads.misses, 3);
            assert_eq!(metrics.point_log_payloads.hits, 3);
            assert_eq!(metrics.point_log_payloads.inserts, 3);
        });
    }
}
