use std::sync::Mutex;

use bytes::Bytes;
use finalized_history_query::api::{
    ExecutionBudget, FinalizedHistoryService, QueryLogsRequest, QueryOrder,
};
use finalized_history_query::codec::finalized_state::{encode_block_meta, encode_indexed_head};
use finalized_history_query::config::{Config, IngestMode};
use finalized_history_query::domain::keys::{
    INDEXED_HEAD_KEY, LOG_DIRECTORY_SUB_BUCKET_SIZE, MAX_LOCAL_ID, STREAM_PAGE_LOCAL_ID_SPAN,
    block_meta_key, log_directory_fragment_key, log_directory_sub_bucket_key,
    stream_fragment_blob_key, stream_fragment_meta_key, stream_id, stream_page_blob_key,
    stream_page_meta_key, stream_page_start_local,
};
use finalized_history_query::domain::types::{Block, BlockMeta, IndexedHead, Log};
use finalized_history_query::error::Error;
use finalized_history_query::store::blob::InMemoryBlobStore;
use finalized_history_query::store::meta::InMemoryMetaStore;
use finalized_history_query::store::traits::{
    BlobStore, DelCond, FenceToken, MetaStore, Page, PutCond, PutResult, Record,
};
use finalized_history_query::{Clause, LogFilter};
use futures::executor::block_on;

fn mk_log(address: u8, topic0: u8, topic1: u8, block_num: u64, tx_idx: u32, log_idx: u32) -> Log {
    Log {
        address: [address; 20],
        topics: vec![[topic0; 32], [topic1; 32]],
        data: vec![address, topic0, topic1],
        block_num,
        tx_idx,
        log_idx,
        block_hash: [block_num as u8; 32],
    }
}

fn mk_block(block_num: u64, parent_hash: [u8; 32], logs: Vec<Log>) -> Block {
    Block {
        block_num,
        block_hash: [block_num as u8; 32],
        parent_hash,
        logs,
    }
}

fn indexed_address_filter(address: u8) -> LogFilter {
    LogFilter {
        address: Some(Clause::One([address; 20])),
        topic0: None,
        topic1: None,
        topic2: None,
        topic3: None,
    }
}

fn query_request(
    from_block: u64,
    to_block: u64,
    filter: LogFilter,
    limit: usize,
    resume_log_id: Option<u64>,
) -> QueryLogsRequest {
    QueryLogsRequest {
        from_block,
        to_block,
        order: QueryOrder::Ascending,
        resume_log_id,
        limit,
        filter,
    }
}

async fn query_page<M: MetaStore, B: BlobStore>(
    svc: &FinalizedHistoryService<M, B>,
    from_block: u64,
    to_block: u64,
    filter: LogFilter,
    limit: usize,
    resume_log_id: Option<u64>,
) -> finalized_history_query::Result<finalized_history_query::QueryPage<Log>> {
    svc.query_logs(
        query_request(from_block, to_block, filter, limit, resume_log_id),
        ExecutionBudget::default(),
    )
    .await
}

#[derive(Default)]
struct RecordingMetaStore {
    inner: InMemoryMetaStore,
    puts: Mutex<Vec<(Vec<u8>, PutCond)>>,
}

impl RecordingMetaStore {
    fn count_puts_with_prefix<F>(&self, prefix: &[u8], mut pred: F) -> usize
    where
        F: FnMut(PutCond) -> bool,
    {
        self.puts
            .lock()
            .map(|puts| {
                puts.iter()
                    .filter(|(key, cond)| key.starts_with(prefix) && pred(*cond))
                    .count()
            })
            .unwrap_or(0)
    }
}

impl MetaStore for RecordingMetaStore {
    async fn get(&self, key: &[u8]) -> finalized_history_query::Result<Option<Record>> {
        self.inner.get(key).await
    }

    async fn put(
        &self,
        key: &[u8],
        value: Bytes,
        cond: PutCond,
        fence: FenceToken,
    ) -> finalized_history_query::Result<PutResult> {
        self.puts
            .lock()
            .expect("recording meta puts lock")
            .push((key.to_vec(), cond));
        self.inner.put(key, value, cond, fence).await
    }

    async fn delete(
        &self,
        key: &[u8],
        cond: DelCond,
        fence: FenceToken,
    ) -> finalized_history_query::Result<()> {
        self.inner.delete(key, cond, fence).await
    }

    async fn list_prefix(
        &self,
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> finalized_history_query::Result<Page> {
        self.inner.list_prefix(prefix, cursor, limit).await
    }
}

#[test]
fn ingest_publishes_indexed_head_and_immutable_frontier_artifacts() {
    block_on(async {
        let meta = InMemoryMetaStore::default();
        let blob = InMemoryBlobStore::default();
        let svc = FinalizedHistoryService::new(Config::default(), meta, blob, 1);
        let block = mk_block(
            1,
            [0; 32],
            vec![mk_log(1, 10, 20, 1, 0, 0), mk_log(1, 10, 21, 1, 0, 1)],
        );

        svc.ingest_finalized_block(block.clone())
            .await
            .expect("ingest");

        assert_eq!(svc.indexed_finalized_head().await.expect("head"), 1);
        assert!(
            svc.ingest
                .meta_store
                .get(INDEXED_HEAD_KEY)
                .await
                .expect("indexed head get")
                .is_some()
        );
        assert!(
            svc.ingest
                .meta_store
                .get(&log_directory_fragment_key(0, 1))
                .await
                .expect("directory fragment get")
                .is_some()
        );

        let sid = stream_id(
            "addr",
            &[1; 20],
            finalized_history_query::core::ids::LogShard::new(0).unwrap(),
        );
        let page_start = stream_page_start_local(0);
        assert!(
            svc.ingest
                .meta_store
                .get(&stream_fragment_meta_key(&sid, page_start, 1))
                .await
                .expect("stream fragment meta get")
                .is_some()
        );
        assert!(
            svc.ingest
                .blob_store
                .get_blob(&stream_fragment_blob_key(&sid, page_start, 1))
                .await
                .expect("stream fragment blob get")
                .is_some()
        );
        assert!(
            svc.ingest
                .meta_store
                .get(b"manifests/addr/0101010101010101010101010101010101010101/0000000000")
                .await
                .expect("manifest get")
                .is_none()
        );
        assert!(
            svc.ingest
                .meta_store
                .get(b"tails/addr/0101010101010101010101010101010101010101/0000000000")
                .await
                .expect("tail get")
                .is_none()
        );
    });
}

#[test]
fn ingest_and_query_with_limits_and_resume() {
    block_on(async {
        let svc = FinalizedHistoryService::new(
            Config::default(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        svc.ingest_finalized_block(mk_block(
            1,
            [0; 32],
            vec![mk_log(1, 10, 20, 1, 0, 0), mk_log(2, 11, 21, 1, 0, 1)],
        ))
        .await
        .expect("ingest block 1");
        svc.ingest_finalized_block(mk_block(
            2,
            [1; 32],
            vec![mk_log(1, 10, 22, 2, 0, 0), mk_log(1, 12, 23, 2, 0, 1)],
        ))
        .await
        .expect("ingest block 2");

        let first = query_page(&svc, 1, 2, indexed_address_filter(1), 2, None)
            .await
            .expect("first page");
        assert_eq!(first.items.len(), 2);
        assert!(first.meta.has_more);
        assert_eq!(first.meta.next_resume_log_id, Some(2));

        let second = query_page(
            &svc,
            1,
            2,
            indexed_address_filter(1),
            2,
            first.meta.next_resume_log_id,
        )
        .await
        .expect("second page");
        assert_eq!(second.items.len(), 1);
        assert!(!second.meta.has_more);
        assert_eq!(second.items[0].block_num, 2);
    });
}

#[test]
fn query_range_clips_to_finalized_head() {
    block_on(async {
        let svc = FinalizedHistoryService::new(
            Config::default(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        svc.ingest_finalized_block(mk_block(1, [0; 32], vec![mk_log(1, 10, 20, 1, 0, 0)]))
            .await
            .expect("ingest");

        let page = query_page(&svc, 1, 99, indexed_address_filter(1), 10, None)
            .await
            .expect("query");
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.meta.resolved_to_block.number, 1);
    });
}

#[test]
fn query_without_indexed_clause_is_invalid() {
    block_on(async {
        let svc = FinalizedHistoryService::new(
            Config::default(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        let err = query_page(
            &svc,
            0,
            0,
            LogFilter {
                address: None,
                topic0: None,
                topic1: None,
                topic2: None,
                topic3: None,
            },
            10,
            None,
        )
        .await
        .expect_err("query should fail");
        assert!(matches!(err, Error::InvalidParams(_)));
    });
}

#[test]
fn ingest_and_query_across_24_bit_log_shard_boundary() {
    block_on(async {
        let meta = InMemoryMetaStore::default();
        let blob = InMemoryBlobStore::default();
        meta.put(
            INDEXED_HEAD_KEY,
            encode_indexed_head(&IndexedHead {
                indexed_finalized_head: 1,
            }),
            PutCond::Any,
            FenceToken(1),
        )
        .await
        .expect("seed indexed head");
        meta.put(
            &block_meta_key(1),
            encode_block_meta(&BlockMeta {
                block_hash: [1; 32],
                parent_hash: [0; 32],
                first_log_id: u64::from(MAX_LOCAL_ID),
                count: 0,
            }),
            PutCond::Any,
            FenceToken(1),
        )
        .await
        .expect("seed block meta");

        let svc = FinalizedHistoryService::new(Config::default(), meta, blob, 1);
        svc.ingest_finalized_block(mk_block(
            2,
            [1; 32],
            vec![mk_log(7, 10, 20, 2, 0, 0), mk_log(7, 10, 21, 2, 0, 1)],
        ))
        .await
        .expect("ingest block 2");

        let page = query_page(&svc, 2, 2, indexed_address_filter(7), 10, None)
            .await
            .expect("query");
        assert_eq!(page.items.len(), 2);

        let shard0 = stream_id(
            "addr",
            &[7; 20],
            finalized_history_query::core::ids::LogShard::new(0).unwrap(),
        );
        let shard1 = stream_id(
            "addr",
            &[7; 20],
            finalized_history_query::core::ids::LogShard::new(1).unwrap(),
        );
        assert!(
            svc.ingest
                .meta_store
                .get(&stream_fragment_meta_key(
                    &shard0,
                    stream_page_start_local(MAX_LOCAL_ID),
                    2
                ))
                .await
                .expect("shard0 fragment")
                .is_some()
        );
        assert!(
            svc.ingest
                .meta_store
                .get(&stream_fragment_meta_key(&shard1, 0, 2))
                .await
                .expect("shard1 fragment")
                .is_some()
        );
    });
}

#[test]
fn sealed_sub_bucket_and_page_compaction_are_written_when_boundaries_close() {
    block_on(async {
        let svc = FinalizedHistoryService::new(
            Config::default(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        let first_log_id = u64::from(STREAM_PAGE_LOCAL_ID_SPAN - 1);
        svc.ingest
            .meta_store
            .put(
                INDEXED_HEAD_KEY,
                encode_indexed_head(&IndexedHead {
                    indexed_finalized_head: 1,
                }),
                PutCond::Any,
                FenceToken(1),
            )
            .await
            .expect("seed indexed head");
        svc.ingest
            .meta_store
            .put(
                &block_meta_key(1),
                encode_block_meta(&BlockMeta {
                    block_hash: [1; 32],
                    parent_hash: [0; 32],
                    first_log_id,
                    count: 0,
                }),
                PutCond::Any,
                FenceToken(1),
            )
            .await
            .expect("seed block meta");

        let block = mk_block(
            2,
            [1; 32],
            vec![mk_log(5, 10, 20, 2, 0, 0), mk_log(5, 10, 21, 2, 0, 1)],
        );
        svc.ingest_finalized_block(block).await.expect("ingest");

        let sid = stream_id(
            "addr",
            &[5; 20],
            finalized_history_query::core::ids::LogShard::new(0).unwrap(),
        );
        let page_start = stream_page_start_local(STREAM_PAGE_LOCAL_ID_SPAN - 1);
        assert!(
            svc.ingest
                .meta_store
                .get(&stream_page_meta_key(&sid, page_start))
                .await
                .expect("stream page meta")
                .is_some()
        );
        assert!(
            svc.ingest
                .blob_store
                .get_blob(&stream_page_blob_key(&sid, page_start))
                .await
                .expect("stream page blob")
                .is_some()
        );
    });
}

#[test]
fn strict_mode_uses_cas_for_indexed_head_publication() {
    block_on(async {
        let meta = RecordingMetaStore::default();
        let blob = InMemoryBlobStore::default();
        let svc = FinalizedHistoryService::new(
            Config {
                ingest_mode: IngestMode::StrictCas,
                ..Config::default()
            },
            meta,
            blob,
            1,
        );

        svc.ingest_finalized_block(mk_block(1, [0; 32], vec![mk_log(1, 10, 20, 1, 0, 0)]))
            .await
            .expect("ingest");

        assert_eq!(
            svc.ingest
                .meta_store
                .count_puts_with_prefix(INDEXED_HEAD_KEY, |cond| {
                    matches!(cond, PutCond::IfAbsent | PutCond::IfVersion(_))
                }),
            1
        );
    });
}

#[test]
fn fast_mode_uses_unconditional_indexed_head_publication() {
    block_on(async {
        let meta = RecordingMetaStore::default();
        let blob = InMemoryBlobStore::default();
        let svc = FinalizedHistoryService::new(
            Config {
                ingest_mode: IngestMode::SingleWriterFast,
                ..Config::default()
            },
            meta,
            blob,
            1,
        );

        svc.ingest_finalized_block(mk_block(1, [0; 32], vec![mk_log(1, 10, 20, 1, 0, 0)]))
            .await
            .expect("ingest");

        assert_eq!(
            svc.ingest
                .meta_store
                .count_puts_with_prefix(INDEXED_HEAD_KEY, |cond| matches!(cond, PutCond::Any)),
            1
        );
    });
}

#[test]
fn directory_sub_bucket_fragment_and_summary_exist_for_crossing_block() {
    block_on(async {
        let meta = InMemoryMetaStore::default();
        let blob = InMemoryBlobStore::default();
        meta.put(
            INDEXED_HEAD_KEY,
            encode_indexed_head(&IndexedHead {
                indexed_finalized_head: 1,
            }),
            PutCond::Any,
            FenceToken(1),
        )
        .await
        .expect("seed indexed head");
        meta.put(
            &block_meta_key(1),
            encode_block_meta(&BlockMeta {
                block_hash: [1; 32],
                parent_hash: [0; 32],
                first_log_id: LOG_DIRECTORY_SUB_BUCKET_SIZE - 2,
                count: 0,
            }),
            PutCond::Any,
            FenceToken(1),
        )
        .await
        .expect("seed block meta");

        let svc = FinalizedHistoryService::new(Config::default(), meta, blob, 1);
        svc.ingest_finalized_block(mk_block(
            2,
            [1; 32],
            vec![
                mk_log(9, 10, 20, 2, 0, 0),
                mk_log(9, 10, 21, 2, 0, 1),
                mk_log(9, 10, 22, 2, 0, 2),
            ],
        ))
        .await
        .expect("ingest");

        assert!(
            svc.ingest
                .meta_store
                .get(&log_directory_fragment_key(0, 2))
                .await
                .expect("directory fragment")
                .is_some()
        );
        assert!(
            svc.ingest
                .meta_store
                .get(&log_directory_sub_bucket_key(0))
                .await
                .expect("directory sub bucket")
                .is_some()
        );
    });
}
