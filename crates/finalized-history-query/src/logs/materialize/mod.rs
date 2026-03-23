mod hydrate;
mod resolve;

use std::collections::HashMap;

use crate::core::refs::BlockRef;
use crate::logs::types::DirByBlock;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ResolvedLogLocation {
    pub block_num: u64,
    pub local_ordinal: usize,
}

pub struct LogMaterializer<'a, M: MetaStore, B: BlobStore> {
    tables: &'a Tables<M, B>,
    // directory_fragment_cache stays as a per-request HashMap because fragments
    // are assembled from multiple scannable table reads (not a single stored
    // value), and each DirByBlock is only 25 bytes. Not worth BytesCache.
    directory_fragment_cache: HashMap<u64, Vec<DirByBlock>>,
    // block_ref_cache remains because BlockRef is a small Copy type
    // computed from multiple sources, not a direct decode of stored bytes.
    block_ref_cache: HashMap<u64, BlockRef>,
}

impl<'a, M: MetaStore, B: BlobStore> LogMaterializer<'a, M, B> {
    pub fn new(tables: &'a Tables<M, B>) -> Self {
        Self {
            tables,
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

    use crate::codec::StorageCodec;
    use crate::core::execution::PrimaryMaterializer;
    use crate::core::ids::LogId;
    use crate::logs::keys::{LOG_DIRECTORY_BUCKET_SIZE, LOG_DIRECTORY_SUB_BUCKET_SIZE};
    use crate::logs::table_specs::{
        BlobTableSpec, BlockLogBlobSpec, BlockLogHeaderSpec, LogDirBucketSpec, LogDirByBlockSpec,
        LogDirSubBucketSpec,
    };
    use crate::logs::types::{BlockLogHeader, DirBucket, DirByBlock, Log};
    use crate::store::blob::InMemoryBlobStore;
    use crate::store::meta::InMemoryMetaStore;
    use crate::store::traits::{BlobStore, BlobTableId, MetaStore, Page, PutCond};
    use crate::tables::{BytesCacheConfig, TableCacheConfig, Tables};
    use futures::executor::block_on;

    #[derive(Clone)]
    struct CountingBlobStore {
        inner: InMemoryBlobStore,
        get_blob_count: Arc<AtomicU64>,
        read_range_count: Arc<AtomicU64>,
    }

    impl BlobStore for CountingBlobStore {
        async fn put_blob(
            &self,
            table: BlobTableId,
            key: &[u8],
            value: Bytes,
        ) -> crate::Result<()> {
            self.inner.put_blob(table, key, value).await
        }

        async fn get_blob(&self, table: BlobTableId, key: &[u8]) -> crate::Result<Option<Bytes>> {
            if table == BlockLogBlobSpec::TABLE {
                self.get_blob_count.fetch_add(1, Ordering::Relaxed);
            }
            self.inner.get_blob(table, key).await
        }

        async fn read_range(
            &self,
            table: BlobTableId,
            key: &[u8],
            start: u64,
            end_exclusive: u64,
        ) -> crate::Result<Option<Bytes>> {
            if table == BlockLogBlobSpec::TABLE {
                self.read_range_count.fetch_add(1, Ordering::Relaxed);
            }
            self.inner
                .read_range(table, key, start, end_exclusive)
                .await
        }

        async fn delete_blob(&self, table: BlobTableId, key: &[u8]) -> crate::Result<()> {
            self.inner.delete_blob(table, key).await
        }

        async fn list_prefix(
            &self,
            table: BlobTableId,
            prefix: &[u8],
            cursor: Option<Vec<u8>>,
            limit: usize,
        ) -> crate::Result<Page> {
            self.inner.list_prefix(table, prefix, cursor, limit).await
        }
    }

    #[test]
    fn resolve_log_id_prefers_1m_bucket_summary_when_present() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let blob = InMemoryBlobStore::default();
            meta.put(
                crate::logs::keys::LOG_DIR_BUCKET_TABLE,
                &LogDirBucketSpec::key(0),
                DirBucket {
                    start_block: 700,
                    first_log_ids: vec![11, 13],
                }
                .encode(),
                PutCond::Any,
            )
            .await
            .expect("write directory bucket");

            let tables = Tables::without_cache(meta, blob);
            let mut materializer = LogMaterializer::new(&tables);
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
            let log_id = LogId::new(LOG_DIRECTORY_SUB_BUCKET_SIZE + 5);
            meta.scan_put(
                crate::logs::keys::LOG_DIR_BY_BLOCK_TABLE,
                &LogDirByBlockSpec::partition(LogDirSubBucketSpec::sub_bucket_start(log_id.get())),
                &LogDirByBlockSpec::clustering(700),
                DirByBlock {
                    block_num: 700,
                    first_log_id: LOG_DIRECTORY_SUB_BUCKET_SIZE,
                    end_log_id_exclusive: LOG_DIRECTORY_SUB_BUCKET_SIZE + 10,
                }
                .encode(),
                PutCond::Any,
            )
            .await
            .expect("write directory fragment");

            let tables = Tables::without_cache(meta, blob);
            let mut materializer = LogMaterializer::new(&tables);
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
            let first_log_id = LOG_DIRECTORY_BUCKET_SIZE - 3;
            let log_id = LogId::new(first_log_id + LOG_DIRECTORY_BUCKET_SIZE + 5);
            meta.put(
                crate::logs::keys::LOG_DIR_BUCKET_TABLE,
                &LogDirBucketSpec::key(LogDirBucketSpec::bucket_start(log_id.get())),
                DirBucket {
                    start_block: 700,
                    first_log_ids: vec![
                        first_log_id,
                        first_log_id + LOG_DIRECTORY_BUCKET_SIZE + 10,
                    ],
                }
                .encode(),
                PutCond::Any,
            )
            .await
            .expect("write directory bucket");

            let tables = Tables::without_cache(meta, blob);
            let mut materializer = LogMaterializer::new(&tables);
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
            let read_range_count = Arc::clone(&blob.read_range_count);
            let get_blob_count = Arc::clone(&blob.get_blob_count);
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
            let encoded = log.encode();

            meta.scan_put(
                crate::logs::keys::LOG_DIR_BY_BLOCK_TABLE,
                &LogDirByBlockSpec::partition(LogDirSubBucketSpec::sub_bucket_start(log_id.get())),
                &LogDirByBlockSpec::clustering(block_num),
                DirByBlock {
                    block_num,
                    first_log_id: LOG_DIRECTORY_SUB_BUCKET_SIZE,
                    end_log_id_exclusive: LOG_DIRECTORY_SUB_BUCKET_SIZE + 1,
                }
                .encode(),
                PutCond::Any,
            )
            .await
            .expect("write directory fragment");
            meta.put(
                crate::logs::keys::BLOCK_LOG_HEADER_TABLE,
                &BlockLogHeaderSpec::key(block_num),
                BlockLogHeader {
                    offsets: vec![0, encoded.len() as u32],
                }
                .encode(),
                PutCond::Any,
            )
            .await
            .expect("write block log header");
            blob.put_blob(
                BlockLogBlobSpec::TABLE,
                &BlockLogBlobSpec::key(block_num),
                encoded.clone(),
            )
            .await
            .expect("write block log blob");

            let tables = Tables::new(
                meta,
                blob,
                BytesCacheConfig {
                    point_log_payloads: TableCacheConfig {
                        max_bytes: 4 * 1024,
                    },
                    ..BytesCacheConfig::disabled()
                },
            );
            let mut materializer = LogMaterializer::new(&tables);
            let first = PrimaryMaterializer::load_by_id(&mut materializer, log_id)
                .await
                .expect("first load")
                .expect("first log");
            let second = PrimaryMaterializer::load_by_id(&mut materializer, log_id)
                .await
                .expect("second load")
                .expect("second log");

            assert_eq!(first, second);
            assert_eq!(get_blob_count.load(Ordering::Relaxed), 0);
            assert_eq!(read_range_count.load(Ordering::Relaxed), 1);

            let metrics = tables.metrics_snapshot().point_log_payloads;
            assert_eq!(metrics.misses, 1);
            assert_eq!(metrics.hits, 1);
            assert_eq!(metrics.inserts, 1);
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
            let read_range_count = Arc::clone(&blob.read_range_count);
            let get_blob_count = Arc::clone(&blob.get_blob_count);
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
            let encoded_logs = logs.iter().map(Log::encode).collect::<Vec<_>>();
            let mut blob_bytes = Vec::new();
            let mut offsets = Vec::with_capacity(encoded_logs.len() + 1);
            offsets.push(0);
            for encoded in &encoded_logs {
                blob_bytes.extend_from_slice(encoded);
                offsets.push(blob_bytes.len() as u32);
            }

            meta.put(
                crate::logs::keys::BLOCK_LOG_HEADER_TABLE,
                &BlockLogHeaderSpec::key(block_num),
                BlockLogHeader { offsets }.encode(),
                PutCond::Any,
            )
            .await
            .expect("write block log header");
            blob.put_blob(
                BlockLogBlobSpec::TABLE,
                &BlockLogBlobSpec::key(block_num),
                Bytes::from(blob_bytes),
            )
            .await
            .expect("write block log blob");

            let tables = Tables::new(
                meta,
                blob,
                BytesCacheConfig {
                    point_log_payloads: TableCacheConfig {
                        max_bytes: 4 * 1024,
                    },
                    ..BytesCacheConfig::disabled()
                },
            );
            let mut materializer = LogMaterializer::new(&tables);
            let first = materializer
                .load_contiguous_run(block_num, 0, 2)
                .await
                .expect("first contiguous load");
            let second = materializer
                .load_contiguous_run(block_num, 0, 2)
                .await
                .expect("second contiguous load");

            assert_eq!(first, second);
            assert_eq!(get_blob_count.load(Ordering::Relaxed), 0);
            assert_eq!(read_range_count.load(Ordering::Relaxed), 1);

            let metrics = tables.metrics_snapshot().point_log_payloads;
            assert_eq!(metrics.misses, 3);
            assert_eq!(metrics.hits, 3);
            assert_eq!(metrics.inserts, 3);
        });
    }

    #[test]
    fn directory_fragment_loading_returns_block_sorted_fragments() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let blob = InMemoryBlobStore::default();
            let sub_bucket_start = LOG_DIRECTORY_SUB_BUCKET_SIZE;

            for fragment in [
                DirByBlock {
                    block_num: 703,
                    first_log_id: sub_bucket_start + 7,
                    end_log_id_exclusive: sub_bucket_start + 9,
                },
                DirByBlock {
                    block_num: 701,
                    first_log_id: sub_bucket_start,
                    end_log_id_exclusive: sub_bucket_start + 3,
                },
                DirByBlock {
                    block_num: 702,
                    first_log_id: sub_bucket_start + 3,
                    end_log_id_exclusive: sub_bucket_start + 7,
                },
            ] {
                meta.scan_put(
                    crate::logs::keys::LOG_DIR_BY_BLOCK_TABLE,
                    &LogDirByBlockSpec::partition(sub_bucket_start),
                    &LogDirByBlockSpec::clustering(fragment.block_num),
                    fragment.encode(),
                    PutCond::Any,
                )
                .await
                .expect("write directory fragment");
            }

            let tables = Tables::without_cache(meta, blob);
            let fragments = tables
                .directory_fragments()
                .load_sub_bucket_fragments(sub_bucket_start)
                .await
                .expect("load directory fragments");

            assert_eq!(
                fragments
                    .iter()
                    .map(|fragment| fragment.block_num)
                    .collect::<Vec<_>>(),
                vec![701, 702, 703]
            );
        });
    }
}
