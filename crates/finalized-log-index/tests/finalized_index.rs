use std::fs;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use finalized_log_index::api::{
    ExecutionBudget, FinalizedIndexService, QueryLogsRequest, QueryOrder,
};
use finalized_log_index::codec::chunk::{ChunkBlob, encode_chunk};
use finalized_log_index::codec::log::{encode_block_meta, encode_meta_state};
use finalized_log_index::codec::manifest::{Manifest, decode_manifest, encode_manifest};
use finalized_log_index::config::{BroadQueryPolicy, Config, GuardrailAction, IngestMode};
use finalized_log_index::domain::filter::{Clause, LogFilter};
use finalized_log_index::domain::keys::{
    MAX_LOCAL_ID, META_STATE_KEY, block_hash_to_num_key, compose_global_log_id, log_shard,
    manifest_key, stream_id,
};
use finalized_log_index::domain::types::{Block, BlockMeta, Log, MetaState};
use finalized_log_index::error::Error;
use finalized_log_index::store::blob::InMemoryBlobStore;
use finalized_log_index::store::fs::{FsBlobStore, FsMetaStore};
use finalized_log_index::store::meta::InMemoryMetaStore;
use finalized_log_index::store::traits::{
    BlobStore, DelCond, FenceToken, MetaStore, Page, PutCond, PutResult, Record,
};
use futures::executor::block_on;
use roaring::RoaringBitmap;

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
    svc: &FinalizedIndexService<M, B>,
    from_block: u64,
    to_block: u64,
    filter: LogFilter,
    limit: Option<usize>,
    resume_log_id: Option<u64>,
) -> finalized_log_index::Result<finalized_log_index::QueryPage<Log>> {
    query_page_with_budget(
        svc,
        from_block,
        to_block,
        filter,
        limit,
        resume_log_id,
        ExecutionBudget::default(),
    )
    .await
}

async fn query_page_with_budget<M: MetaStore, B: BlobStore>(
    svc: &FinalizedIndexService<M, B>,
    from_block: u64,
    to_block: u64,
    filter: LogFilter,
    limit: Option<usize>,
    resume_log_id: Option<u64>,
    budget: ExecutionBudget,
) -> finalized_log_index::Result<finalized_log_index::QueryPage<Log>> {
    svc.query_logs(
        query_request(
            from_block,
            to_block,
            filter,
            limit.unwrap_or(usize::MAX),
            resume_log_id,
        ),
        budget,
    )
    .await
}

struct FlakyMetaStore {
    inner: InMemoryMetaStore,
    fail_get_remaining: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl MetaStore for FlakyMetaStore {
    async fn get(
        &self,
        key: &[u8],
    ) -> finalized_log_index::error::Result<Option<finalized_log_index::store::traits::Record>>
    {
        if self.fail_get_remaining.load(Ordering::Relaxed) > 0 {
            self.fail_get_remaining.fetch_sub(1, Ordering::Relaxed);
            return Err(Error::Backend("injected backend get failure".to_string()));
        }
        self.inner.get(key).await
    }

    async fn put(
        &self,
        key: &[u8],
        value: bytes::Bytes,
        cond: finalized_log_index::store::traits::PutCond,
        fence: FenceToken,
    ) -> finalized_log_index::error::Result<finalized_log_index::store::traits::PutResult> {
        self.inner.put(key, value, cond, fence).await
    }

    async fn delete(
        &self,
        key: &[u8],
        cond: finalized_log_index::store::traits::DelCond,
        fence: FenceToken,
    ) -> finalized_log_index::error::Result<()> {
        self.inner.delete(key, cond, fence).await
    }

    async fn list_prefix(
        &self,
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> finalized_log_index::error::Result<finalized_log_index::store::traits::Page> {
        self.inner.list_prefix(prefix, cursor, limit).await
    }
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
                    .filter(|(k, cond)| k.starts_with(prefix) && pred(*cond))
                    .count()
            })
            .unwrap_or(0)
    }
}

#[derive(Default)]
struct RecordingBlobStore {
    inner: InMemoryBlobStore,
    gets: Mutex<Vec<Vec<u8>>>,
}

impl RecordingBlobStore {
    fn count_gets_with_prefix(&self, prefix: &[u8]) -> usize {
        self.gets
            .lock()
            .map(|gets| gets.iter().filter(|key| key.starts_with(prefix)).count())
            .unwrap_or(0)
    }
}

#[async_trait::async_trait]
impl BlobStore for RecordingBlobStore {
    async fn put_blob(
        &self,
        key: &[u8],
        value: bytes::Bytes,
    ) -> finalized_log_index::error::Result<()> {
        self.inner.put_blob(key, value).await
    }

    async fn get_blob(
        &self,
        key: &[u8],
    ) -> finalized_log_index::error::Result<Option<bytes::Bytes>> {
        if let Ok(mut gets) = self.gets.lock() {
            gets.push(key.to_vec());
        }
        self.inner.get_blob(key).await
    }

    async fn delete_blob(&self, key: &[u8]) -> finalized_log_index::error::Result<()> {
        self.inner.delete_blob(key).await
    }

    async fn list_prefix(
        &self,
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> finalized_log_index::error::Result<Page> {
        self.inner.list_prefix(prefix, cursor, limit).await
    }
}

#[async_trait::async_trait]
impl MetaStore for RecordingMetaStore {
    async fn get(&self, key: &[u8]) -> finalized_log_index::error::Result<Option<Record>> {
        self.inner.get(key).await
    }

    async fn put(
        &self,
        key: &[u8],
        value: bytes::Bytes,
        cond: PutCond,
        fence: FenceToken,
    ) -> finalized_log_index::error::Result<PutResult> {
        if let Ok(mut puts) = self.puts.lock() {
            puts.push((key.to_vec(), cond));
        }
        self.inner.put(key, value, cond, fence).await
    }

    async fn delete(
        &self,
        key: &[u8],
        cond: DelCond,
        fence: FenceToken,
    ) -> finalized_log_index::error::Result<()> {
        self.inner.delete(key, cond, fence).await
    }

    async fn list_prefix(
        &self,
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> finalized_log_index::error::Result<Page> {
        self.inner.list_prefix(prefix, cursor, limit).await
    }
}

#[test]
fn ingest_and_query_with_limits() {
    block_on(async {
        let config = Config {
            target_entries_per_chunk: 2,
            planner_max_or_terms: 8,
            ..Config::default()
        };
        let svc = FinalizedIndexService::new(
            config,
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        let b1 = mk_block(
            1,
            [0; 32],
            vec![mk_log(1, 10, 20, 1, 0, 0), mk_log(2, 11, 21, 1, 0, 1)],
        );
        let b2 = mk_block(
            2,
            b1.block_hash,
            vec![mk_log(1, 10, 22, 2, 0, 0), mk_log(3, 12, 23, 2, 0, 1)],
        );

        svc.ingest_finalized_block(b1).await.expect("ingest b1");
        svc.ingest_finalized_block(b2).await.expect("ingest b2");

        let filter = LogFilter {
            address: Some(Clause::One([1; 20])),
            topic0: Some(Clause::One([10; 32])),
            topic1: None,
            topic2: None,
            topic3: None,
        };

        let all = query_page(&svc, 1, 2, filter.clone(), None, None)
            .await
            .expect("query all");
        assert_eq!(all.items.len(), 2);

        let limited = query_page(&svc, 1, 2, filter, Some(1), None)
            .await
            .expect("query limited");
        assert_eq!(limited.items.len(), 1);
    });
}

#[test]
fn invalid_request_validation() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config::default(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        let b1 = mk_block(1, [0; 32], vec![mk_log(1, 10, 20, 1, 0, 0)]);
        svc.ingest_finalized_block(b1.clone())
            .await
            .expect("ingest b1");

        let err = svc
            .query_logs(
                query_request(
                    1,
                    1,
                    LogFilter {
                        address: None,
                        topic0: Some(Clause::One([10; 32])),
                        topic1: None,
                        topic2: None,
                        topic3: None,
                    },
                    0,
                    None,
                ),
                ExecutionBudget::default(),
            )
            .await
            .expect_err("limit validation");
        assert!(matches!(err, Error::InvalidParams(_)));

        let err = svc
            .query_logs(
                query_request(
                    1,
                    1,
                    LogFilter {
                        address: None,
                        topic0: None,
                        topic1: None,
                        topic2: None,
                        topic3: None,
                    },
                    10,
                    Some(99),
                ),
                ExecutionBudget::default(),
            )
            .await
            .expect_err("invalid params expected");
        assert!(matches!(err, Error::InvalidParams(_)));
    });
}

#[test]
fn indexed_same_block_resume_pagination_tracks_page_meta() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config::default(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        let block = mk_block(
            1,
            [0; 32],
            vec![
                mk_log(1, 10, 20, 1, 0, 0),
                mk_log(1, 10, 21, 1, 0, 1),
                mk_log(1, 10, 22, 1, 0, 2),
            ],
        );
        svc.ingest_finalized_block(block).await.expect("ingest");

        let filter = LogFilter {
            address: Some(Clause::One([1; 20])),
            topic0: Some(Clause::One([10; 32])),
            topic1: None,
            topic2: None,
            topic3: None,
        };

        let first = query_page(&svc, 1, 1, filter.clone(), Some(1), None)
            .await
            .expect("first page");
        assert_eq!(first.items.len(), 1);
        assert!(first.meta.has_more);
        assert_eq!(first.meta.next_resume_log_id, Some(0));
        assert_eq!(first.meta.resolved_from_block.number, 1);
        assert_eq!(first.meta.resolved_to_block.number, 1);
        assert_eq!(first.meta.cursor_block.number, 1);

        let second = query_page(
            &svc,
            1,
            1,
            filter.clone(),
            Some(1),
            first.meta.next_resume_log_id,
        )
        .await
        .expect("second page");
        assert_eq!(second.items.len(), 1);
        assert!(second.meta.has_more);
        assert_eq!(second.meta.next_resume_log_id, Some(1));
        assert_eq!(second.meta.cursor_block.number, 1);

        let third = query_page(&svc, 1, 1, filter, Some(1), second.meta.next_resume_log_id)
            .await
            .expect("third page");
        assert_eq!(third.items.len(), 1);
        assert!(!third.meta.has_more);
        assert_eq!(third.meta.next_resume_log_id, None);
        assert_eq!(third.meta.cursor_block.number, 1);
    });
}

#[test]
fn block_scan_same_block_resume_pagination_tracks_page_meta() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config {
                planner_max_or_terms: 1,
                planner_broad_query_policy: BroadQueryPolicy::BlockScan,
                ..Config::default()
            },
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        let block = mk_block(
            1,
            [0; 32],
            vec![
                mk_log(1, 10, 20, 1, 0, 0),
                mk_log(2, 10, 21, 1, 0, 1),
                mk_log(1, 10, 22, 1, 0, 2),
            ],
        );
        svc.ingest_finalized_block(block).await.expect("ingest");

        let filter = LogFilter {
            address: Some(Clause::Or(vec![[1; 20], [2; 20]])),
            topic0: Some(Clause::One([10; 32])),
            topic1: None,
            topic2: None,
            topic3: None,
        };

        let first = query_page(&svc, 1, 1, filter.clone(), Some(1), None)
            .await
            .expect("first page");
        assert_eq!(first.items.len(), 1);
        assert!(first.meta.has_more);
        assert_eq!(first.meta.next_resume_log_id, Some(0));

        let second = query_page(
            &svc,
            1,
            1,
            filter.clone(),
            Some(1),
            first.meta.next_resume_log_id,
        )
        .await
        .expect("second page");
        assert_eq!(second.items.len(), 1);
        assert!(second.meta.has_more);
        assert_eq!(second.meta.next_resume_log_id, Some(1));

        let third = query_page(&svc, 1, 1, filter, Some(1), second.meta.next_resume_log_id)
            .await
            .expect("third page");
        assert_eq!(third.items.len(), 1);
        assert!(!third.meta.has_more);
        assert_eq!(third.meta.next_resume_log_id, None);
        assert_eq!(third.meta.cursor_block.number, 1);
    });
}

#[test]
fn empty_page_metadata_uses_resolved_range_endpoint() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config::default(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        let b1 = mk_block(1, [0; 32], vec![mk_log(1, 10, 20, 1, 0, 0)]);
        let b2 = mk_block(2, b1.block_hash, vec![mk_log(2, 11, 21, 2, 0, 0)]);
        svc.ingest_finalized_block(b1).await.expect("ingest b1");
        svc.ingest_finalized_block(b2).await.expect("ingest b2");

        let page = query_page(
            &svc,
            1,
            2,
            LogFilter {
                address: Some(Clause::One([9; 20])),
                topic0: None,
                topic1: None,
                topic2: None,
                topic3: None,
            },
            Some(10),
            None,
        )
        .await
        .expect("empty page");

        assert!(page.items.is_empty());
        assert!(!page.meta.has_more);
        assert_eq!(page.meta.resolved_from_block.number, 1);
        assert_eq!(page.meta.resolved_to_block.number, 2);
        assert_eq!(page.meta.cursor_block.number, 2);
        assert_eq!(page.meta.next_resume_log_id, None);
    });
}

#[test]
fn exact_limit_without_additional_match_has_no_resume_metadata() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config::default(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        let block = mk_block(1, [0; 32], vec![mk_log(1, 10, 20, 1, 0, 0)]);
        svc.ingest_finalized_block(block).await.expect("ingest");

        let page = query_page(&svc, 1, 1, LogFilter::default(), Some(1), None)
            .await
            .expect("query");

        assert_eq!(page.items.len(), 1);
        assert!(!page.meta.has_more);
        assert_eq!(page.meta.next_resume_log_id, None);
        assert_eq!(page.meta.cursor_block.number, 1);
    });
}

#[test]
fn cross_block_resume_pagination_advances_without_duplicates() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config::default(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        let b1 = mk_block(1, [0; 32], vec![mk_log(1, 10, 20, 1, 0, 0)]);
        let b2 = mk_block(2, b1.block_hash, vec![mk_log(1, 10, 21, 2, 0, 0)]);
        svc.ingest_finalized_block(b1).await.expect("ingest b1");
        svc.ingest_finalized_block(b2).await.expect("ingest b2");

        let first = query_page(&svc, 1, 2, LogFilter::default(), Some(1), None)
            .await
            .expect("first page");
        assert_eq!(first.items.len(), 1);
        assert_eq!(first.items[0].block_num, 1);
        assert!(first.meta.has_more);
        assert_eq!(first.meta.next_resume_log_id, Some(0));
        assert_eq!(first.meta.cursor_block.number, 1);

        let second = query_page(
            &svc,
            1,
            2,
            LogFilter::default(),
            Some(1),
            first.meta.next_resume_log_id,
        )
        .await
        .expect("second page");
        assert_eq!(second.items.len(), 1);
        assert_eq!(second.items[0].block_num, 2);
        assert!(!second.meta.has_more);
        assert_eq!(second.meta.next_resume_log_id, None);
        assert_eq!(second.meta.cursor_block.number, 2);
    });
}

#[test]
fn indexed_query_does_not_leak_logs_past_empty_range_end() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config::default(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        let b1 = mk_block(1, [0; 32], Vec::new());
        let b2 = mk_block(2, b1.block_hash, vec![mk_log(1, 10, 20, 2, 0, 0)]);
        svc.ingest_finalized_block(b1).await.expect("ingest b1");
        svc.ingest_finalized_block(b2).await.expect("ingest b2");

        let page = query_page(&svc, 1, 1, LogFilter::default(), Some(10), None)
            .await
            .expect("query");

        assert!(page.items.is_empty());
        assert_eq!(page.meta.resolved_from_block.number, 1);
        assert_eq!(page.meta.resolved_to_block.number, 1);
        assert_eq!(page.meta.cursor_block.number, 1);
    });
}

#[test]
fn block_scan_query_does_not_leak_logs_past_empty_range_end() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config {
                planner_max_or_terms: 0,
                planner_broad_query_policy: BroadQueryPolicy::BlockScan,
                ..Config::default()
            },
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        let b1 = mk_block(1, [0; 32], Vec::new());
        let b2 = mk_block(2, b1.block_hash, vec![mk_log(1, 10, 20, 2, 0, 0)]);
        svc.ingest_finalized_block(b1).await.expect("ingest b1");
        svc.ingest_finalized_block(b2).await.expect("ingest b2");

        let page = query_page(
            &svc,
            1,
            1,
            LogFilter {
                address: Some(Clause::One([1; 20])),
                topic0: None,
                topic1: None,
                topic2: None,
                topic3: None,
            },
            Some(10),
            None,
        )
        .await
        .expect("query");

        assert!(page.items.is_empty());
        assert_eq!(page.meta.resolved_from_block.number, 1);
        assert_eq!(page.meta.resolved_to_block.number, 1);
        assert_eq!(page.meta.cursor_block.number, 1);
    });
}

#[test]
fn indexed_query_skips_empty_leading_blocks_within_range() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config::default(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        let b1 = mk_block(1, [0; 32], Vec::new());
        let b2 = mk_block(2, b1.block_hash, vec![mk_log(1, 10, 20, 2, 0, 0)]);
        svc.ingest_finalized_block(b1).await.expect("ingest b1");
        svc.ingest_finalized_block(b2).await.expect("ingest b2");

        let page = query_page(&svc, 1, 2, LogFilter::default(), Some(10), None)
            .await
            .expect("query");

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].block_num, 2);
        assert_eq!(page.meta.resolved_from_block.number, 1);
        assert_eq!(page.meta.resolved_to_block.number, 2);
        assert_eq!(page.meta.cursor_block.number, 2);
    });
}

#[test]
fn block_scan_query_skips_empty_leading_blocks_within_range() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config {
                planner_max_or_terms: 0,
                planner_broad_query_policy: BroadQueryPolicy::BlockScan,
                ..Config::default()
            },
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        let b1 = mk_block(1, [0; 32], Vec::new());
        let b2 = mk_block(2, b1.block_hash, vec![mk_log(1, 10, 20, 2, 0, 0)]);
        svc.ingest_finalized_block(b1).await.expect("ingest b1");
        svc.ingest_finalized_block(b2).await.expect("ingest b2");

        let page = query_page(
            &svc,
            1,
            2,
            LogFilter {
                address: Some(Clause::One([1; 20])),
                topic0: None,
                topic1: None,
                topic2: None,
                topic3: None,
            },
            Some(10),
            None,
        )
        .await
        .expect("query");

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].block_num, 2);
        assert_eq!(page.meta.resolved_from_block.number, 1);
        assert_eq!(page.meta.resolved_to_block.number, 2);
        assert_eq!(page.meta.cursor_block.number, 2);
    });
}

#[test]
fn all_empty_blocks_range_returns_empty_page() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config::default(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        let b1 = mk_block(1, [0; 32], Vec::new());
        let b2 = mk_block(2, b1.block_hash, Vec::new());
        svc.ingest_finalized_block(b1).await.expect("ingest b1");
        svc.ingest_finalized_block(b2).await.expect("ingest b2");

        let page = query_page(&svc, 1, 2, LogFilter::default(), Some(10), None)
            .await
            .expect("query");

        assert!(page.items.is_empty());
        assert!(!page.meta.has_more);
        assert_eq!(page.meta.resolved_from_block.number, 1);
        assert_eq!(page.meta.resolved_to_block.number, 2);
        assert_eq!(page.meta.cursor_block.number, 2);
    });
}

#[test]
fn query_range_clips_to_finalized_head() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config::default(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        let b1 = mk_block(1, [0; 32], vec![mk_log(1, 10, 20, 1, 0, 0)]);
        let b2 = mk_block(2, b1.block_hash, vec![mk_log(2, 11, 21, 2, 0, 0)]);
        svc.ingest_finalized_block(b1).await.expect("ingest b1");
        svc.ingest_finalized_block(b2).await.expect("ingest b2");

        let page = query_page(&svc, 1, 10, LogFilter::default(), Some(10), None)
            .await
            .expect("query");

        assert_eq!(
            page.items
                .iter()
                .map(|log| log.block_num)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(page.meta.resolved_from_block.number, 1);
        assert_eq!(page.meta.resolved_to_block.number, 2);
        assert_eq!(page.meta.cursor_block.number, 2);
    });
}

#[test]
fn query_above_finalized_head_returns_empty_page_anchored_to_head() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config::default(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        let b1 = mk_block(1, [0; 32], vec![mk_log(1, 10, 20, 1, 0, 0)]);
        let b2 = mk_block(2, b1.block_hash, vec![mk_log(2, 11, 21, 2, 0, 0)]);
        svc.ingest_finalized_block(b1).await.expect("ingest b1");
        svc.ingest_finalized_block(b2).await.expect("ingest b2");

        let page = query_page(&svc, 10, 20, LogFilter::default(), Some(10), None)
            .await
            .expect("query");

        assert!(page.items.is_empty());
        assert_eq!(page.meta.resolved_from_block.number, 2);
        assert_eq!(page.meta.resolved_to_block.number, 2);
        assert_eq!(page.meta.cursor_block.number, 2);
    });
}

#[test]
fn empty_store_returns_zero_anchored_empty_page() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config::default(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        let page = query_page(&svc, 1, 10, LogFilter::default(), Some(10), None)
            .await
            .expect("query");

        assert!(page.items.is_empty());
        assert_eq!(page.meta.resolved_from_block.number, 0);
        assert_eq!(page.meta.resolved_to_block.number, 0);
        assert_eq!(page.meta.cursor_block.number, 0);
    });
}

#[test]
fn inverted_block_range_is_invalid() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config::default(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        let err = query_page(&svc, 2, 1, LogFilter::default(), Some(10), None)
            .await
            .expect_err("expected invalid block range");

        assert!(matches!(err, Error::InvalidParams(_)));
    });
}

#[test]
fn resume_log_id_below_window_is_invalid() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config::default(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        let b1 = mk_block(1, [0; 32], vec![mk_log(1, 10, 20, 1, 0, 0)]);
        let b2 = mk_block(2, b1.block_hash, vec![mk_log(2, 11, 21, 2, 0, 0)]);
        svc.ingest_finalized_block(b1).await.expect("ingest b1");
        svc.ingest_finalized_block(b2).await.expect("ingest b2");

        let err = query_page(&svc, 2, 2, LogFilter::default(), Some(10), Some(0))
            .await
            .expect_err("expected invalid resume_log_id");

        assert!(matches!(err, Error::InvalidParams(_)));
    });
}

#[test]
fn resume_log_id_above_window_is_invalid() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config::default(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        let b1 = mk_block(1, [0; 32], vec![mk_log(1, 10, 20, 1, 0, 0)]);
        let b2 = mk_block(2, b1.block_hash, vec![mk_log(2, 11, 21, 2, 0, 0)]);
        svc.ingest_finalized_block(b1).await.expect("ingest b1");
        svc.ingest_finalized_block(b2).await.expect("ingest b2");

        let err = query_page(&svc, 1, 1, LogFilter::default(), Some(10), Some(1))
            .await
            .expect_err("expected invalid resume_log_id");

        assert!(matches!(err, Error::InvalidParams(_)));
    });
}

#[test]
fn budget_max_results_caps_page_size_and_sets_resume_metadata() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config::default(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        let block = mk_block(
            1,
            [0; 32],
            vec![mk_log(1, 10, 20, 1, 0, 0), mk_log(2, 11, 21, 1, 0, 1)],
        );
        svc.ingest_finalized_block(block).await.expect("ingest");

        let page = query_page_with_budget(
            &svc,
            1,
            1,
            LogFilter::default(),
            Some(10),
            None,
            ExecutionBudget {
                max_results: Some(1),
            },
        )
        .await
        .expect("query");

        assert_eq!(page.items.len(), 1);
        assert!(page.meta.has_more);
        assert_eq!(page.meta.next_resume_log_id, Some(0));
        assert_eq!(page.meta.cursor_block.number, 1);
    });
}

#[test]
fn zero_budget_max_results_is_invalid() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config::default(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        let err = query_page_with_budget(
            &svc,
            1,
            1,
            LogFilter::default(),
            Some(10),
            None,
            ExecutionBudget {
                max_results: Some(0),
            },
        )
        .await
        .expect_err("expected invalid budget");

        assert!(matches!(err, Error::InvalidParams(_)));
    });
}

#[test]
fn resuming_after_last_log_in_range_with_empty_tail_returns_empty_page_at_range_end() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config::default(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        let b1 = mk_block(1, [0; 32], vec![mk_log(1, 10, 20, 1, 0, 0)]);
        let b2 = mk_block(2, b1.block_hash, Vec::new());
        svc.ingest_finalized_block(b1).await.expect("ingest b1");
        svc.ingest_finalized_block(b2).await.expect("ingest b2");

        let page = query_page(&svc, 1, 2, LogFilter::default(), Some(10), Some(0))
            .await
            .expect("query");

        assert!(page.items.is_empty());
        assert!(!page.meta.has_more);
        assert_eq!(page.meta.resolved_from_block.number, 1);
        assert_eq!(page.meta.resolved_to_block.number, 2);
        assert_eq!(page.meta.cursor_block.number, 2);
        assert_eq!(page.meta.next_resume_log_id, None);
    });
}

#[test]
fn resume_past_last_match_inside_window_returns_empty_final_page() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config::default(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        let block = mk_block(
            1,
            [0; 32],
            vec![
                mk_log(1, 10, 20, 1, 0, 0),
                mk_log(2, 10, 21, 1, 0, 1),
                mk_log(3, 10, 22, 1, 0, 2),
            ],
        );
        svc.ingest_finalized_block(block).await.expect("ingest");

        let page = query_page(
            &svc,
            1,
            1,
            LogFilter {
                address: Some(Clause::One([1; 20])),
                topic0: Some(Clause::One([10; 32])),
                topic1: None,
                topic2: None,
                topic3: None,
            },
            Some(10),
            Some(2),
        )
        .await
        .expect("empty resume page");

        assert!(page.items.is_empty());
        assert!(!page.meta.has_more);
        assert_eq!(page.meta.cursor_block.number, 1);
        assert_eq!(page.meta.next_resume_log_id, None);
    });
}

#[test]
fn or_guardrail_is_enforced() {
    block_on(async {
        let config = Config {
            planner_max_or_terms: 2,
            ..Config::default()
        };
        let svc = FinalizedIndexService::new(
            config,
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        let b1 = mk_block(1, [0; 32], vec![mk_log(1, 10, 20, 1, 0, 0)]);
        svc.ingest_finalized_block(b1).await.expect("ingest b1");

        let err = query_page(
            &svc,
            1,
            1,
            LogFilter {
                address: Some(Clause::Or(vec![[1; 20], [2; 20], [3; 20]])),
                topic0: None,
                topic1: None,
                topic2: None,
                topic3: None,
            },
            None,
            None,
        )
        .await
        .expect_err("too broad");

        assert!(matches!(err, Error::QueryTooBroad { actual: 3, max: 2 }));
    });
}

#[test]
fn broad_query_can_fallback_to_block_scan() {
    block_on(async {
        let config = Config {
            planner_max_or_terms: 1,
            planner_broad_query_policy: BroadQueryPolicy::BlockScan,
            ..Config::default()
        };
        let svc = FinalizedIndexService::new(
            config,
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        let b1 = mk_block(
            1,
            [0; 32],
            vec![mk_log(1, 10, 20, 1, 0, 0), mk_log(2, 10, 21, 1, 0, 1)],
        );
        svc.ingest_finalized_block(b1).await.expect("ingest b1");

        // Two OR terms exceed max_or_terms=1, but policy says block-scan fallback.
        let got = query_page(
            &svc,
            1,
            1,
            LogFilter {
                address: Some(Clause::Or(vec![[1; 20], [2; 20]])),
                topic0: Some(Clause::One([10; 32])),
                topic1: None,
                topic2: None,
                topic3: None,
            },
            None,
            None,
        )
        .await
        .expect("fallback block scan query");

        assert_eq!(got.items.len(), 2);
    });
}

#[test]
fn fs_store_adapter_roundtrip() {
    block_on(async {
        let root = std::env::temp_dir().join(format!(
            "finalized-index-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        fs::create_dir_all(&root).expect("mkdir");

        let meta = FsMetaStore::new(&root, 1).expect("meta store");
        let blob = FsBlobStore::new(&root).expect("blob store");
        let config = Config {
            target_entries_per_chunk: 1,
            ..Config::default()
        };
        let svc = FinalizedIndexService::new(config, meta, blob, 1);

        let b1 = mk_block(1, [0; 32], vec![mk_log(9, 5, 4, 1, 0, 0)]);
        svc.ingest_finalized_block(b1).await.expect("ingest");

        let got = query_page(
            &svc,
            1,
            1,
            LogFilter {
                address: Some(Clause::One([9; 20])),
                topic0: Some(Clause::One([5; 32])),
                topic1: None,
                topic2: None,
                topic3: None,
            },
            None,
            None,
        )
        .await
        .expect("query");
        assert_eq!(got.items.len(), 1);

        let _ = fs::remove_dir_all(root);
    });
}

#[test]
fn ingest_always_writes_topic0_for_cold_signature() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config {
                target_entries_per_chunk: 1,
                ..Config::default()
            },
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        let sig = [0x55; 32];
        let mut parent = [0u8; 32];
        for b in 1..=3001u64 {
            let logs = if b == 1 || b == 3001 {
                vec![mk_log(1, sig[0], 20, b, 0, 0)]
            } else {
                Vec::new()
            };
            let block = mk_block(b, parent, logs);
            parent = block.block_hash;
            svc.ingest_finalized_block(block).await.expect("ingest");
        }

        let sid = stream_id("topic0", &sig, log_shard(0));
        let rec = svc
            .ingest
            .meta_store
            .get(&manifest_key(&sid))
            .await
            .expect("manifest get")
            .expect("manifest exists");
        let manifest = decode_manifest(&rec.value).expect("decode manifest");
        assert_eq!(manifest.approx_count, 2);
    });
}

#[test]
fn seals_by_chunk_bytes_threshold() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config {
                target_entries_per_chunk: 10_000,
                target_chunk_bytes: 1,
                maintenance_seal_seconds: 60_000,
                ..Config::default()
            },
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        let b1 = mk_block(1, [0; 32], vec![mk_log(9, 9, 9, 1, 0, 0)]);
        svc.ingest_finalized_block(b1).await.expect("ingest");

        let sid = stream_id("addr", &[9; 20], 0);
        let rec = svc
            .ingest
            .meta_store
            .get(&manifest_key(&sid))
            .await
            .expect("manifest get")
            .expect("manifest");
        let manifest = decode_manifest(&rec.value).expect("decode manifest");
        assert_eq!(manifest.chunk_refs.len(), 1);
    });
}

#[test]
fn periodic_maintenance_seals_old_tails() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config {
                target_entries_per_chunk: 10_000,
                target_chunk_bytes: usize::MAX / 2,
                maintenance_seal_seconds: 1,
                ..Config::default()
            },
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        let b1 = mk_block(1, [0; 32], vec![mk_log(7, 7, 7, 1, 0, 0)]);
        svc.ingest_finalized_block(b1).await.expect("ingest");

        let sid = stream_id("addr", &[7; 20], 0);
        let mkey = manifest_key(&sid);
        let mut manifest = svc
            .ingest
            .meta_store
            .get(&mkey)
            .await
            .expect("manifest get")
            .map(|rec| decode_manifest(&rec.value).expect("decode manifest"))
            .unwrap_or_else(Manifest::default);
        assert_eq!(manifest.chunk_refs.len(), 0);

        manifest.last_seal_unix_sec = 1;
        let _ = svc
            .ingest
            .meta_store
            .put(
                &mkey,
                encode_manifest(&manifest),
                PutCond::Any,
                FenceToken(1),
            )
            .await
            .expect("seed old seal time");

        let stats = svc
            .ingest
            .run_periodic_maintenance(1)
            .await
            .expect("maintenance");
        assert!(stats.flushed_streams >= 1);
        assert!(stats.sealed_streams >= 1);

        let rec = svc
            .ingest
            .meta_store
            .get(&mkey)
            .await
            .expect("manifest get")
            .expect("manifest");
        let manifest = decode_manifest(&rec.value).expect("decode manifest");
        assert_eq!(manifest.chunk_refs.len(), 1);
    });
}

#[test]
fn query_only_loads_overlapping_address_chunks() {
    block_on(async {
        let blob = RecordingBlobStore::default();
        let svc = FinalizedIndexService::new(
            Config {
                target_entries_per_chunk: 1,
                planner_max_or_terms: 8,
                ..Config::default()
            },
            InMemoryMetaStore::default(),
            blob,
            1,
        );

        let b1 = mk_block(1, [0; 32], vec![mk_log(9, 1, 1, 1, 0, 0)]);
        let b2 = mk_block(2, b1.block_hash, vec![mk_log(9, 2, 2, 2, 0, 0)]);
        let b3 = mk_block(3, b2.block_hash, vec![mk_log(9, 3, 3, 3, 0, 0)]);
        svc.ingest_finalized_block(b1).await.expect("ingest b1");
        svc.ingest_finalized_block(b2).await.expect("ingest b2");
        svc.ingest_finalized_block(b3).await.expect("ingest b3");

        let got = query_page(
            &svc,
            2,
            2,
            LogFilter {
                address: Some(Clause::One([9; 20])),
                topic0: None,
                topic1: None,
                topic2: None,
                topic3: None,
            },
            None,
            None,
        )
        .await
        .expect("query");
        assert_eq!(got.items.len(), 1);
        assert_eq!(got.items[0].block_num, 2);

        let addr_prefix = b"chunks/addr/";
        let log_pack_prefix = b"log_packs/";
        assert_eq!(svc.ingest.blob_store.count_gets_with_prefix(addr_prefix), 1);
        assert_eq!(
            svc.ingest
                .blob_store
                .count_gets_with_prefix(log_pack_prefix),
            1
        );
    });
}

#[test]
fn topic0_queries_use_only_topic0_chunks() {
    block_on(async {
        let blob = RecordingBlobStore::default();
        let svc = FinalizedIndexService::new(
            Config {
                target_entries_per_chunk: 1,
                planner_max_or_terms: 8,
                ..Config::default()
            },
            InMemoryMetaStore::default(),
            blob,
            1,
        );

        let b1 = mk_block(1, [0; 32], vec![mk_log(1, 7, 1, 1, 0, 0)]);
        let b2 = mk_block(2, b1.block_hash, vec![mk_log(2, 7, 2, 2, 0, 0)]);
        let b3 = mk_block(3, b2.block_hash, vec![mk_log(3, 7, 3, 3, 0, 0)]);
        svc.ingest_finalized_block(b1).await.expect("ingest b1");
        svc.ingest_finalized_block(b2).await.expect("ingest b2");
        svc.ingest_finalized_block(b3).await.expect("ingest b3");

        let got = query_page(
            &svc,
            2,
            2,
            LogFilter {
                address: None,
                topic0: Some(Clause::One([7; 32])),
                topic1: None,
                topic2: None,
                topic3: None,
            },
            None,
            None,
        )
        .await
        .expect("query");
        assert_eq!(got.items.len(), 1);
        assert_eq!(got.items[0].block_num, 2);

        let topic0_prefix = b"chunks/topic0/";
        let log_pack_prefix = b"log_packs/";
        assert_eq!(
            svc.ingest.blob_store.count_gets_with_prefix(topic0_prefix),
            1
        );
        assert_eq!(
            svc.ingest
                .blob_store
                .count_gets_with_prefix(log_pack_prefix),
            1
        );
    });
}

#[test]
fn ingest_and_query_across_24_bit_log_shard_boundary() {
    block_on(async {
        let meta = InMemoryMetaStore::default();
        let blob = InMemoryBlobStore::default();
        let svc = FinalizedIndexService::new(
            Config {
                target_entries_per_chunk: 1,
                planner_max_or_terms: 8,
                ..Config::default()
            },
            meta,
            blob,
            1,
        );

        let previous = BlockMeta {
            block_hash: [1; 32],
            parent_hash: [0; 32],
            first_log_id: 0,
            count: 0,
        };
        let _ = svc
            .ingest
            .meta_store
            .put(
                META_STATE_KEY,
                encode_meta_state(&MetaState {
                    indexed_finalized_head: 1,
                    next_log_id: u64::from(MAX_LOCAL_ID),
                    writer_epoch: 1,
                }),
                PutCond::Any,
                FenceToken(1),
            )
            .await
            .expect("seed meta state");
        let _ = svc
            .ingest
            .meta_store
            .put(
                &finalized_log_index::domain::keys::block_meta_key(1),
                encode_block_meta(&previous),
                PutCond::Any,
                FenceToken(1),
            )
            .await
            .expect("seed previous block meta");

        let b2 = mk_block(2, previous.block_hash, vec![mk_log(9, 1, 1, 2, 0, 0)]);
        let b3 = mk_block(3, b2.block_hash, vec![mk_log(9, 2, 2, 3, 0, 0)]);
        svc.ingest_finalized_block(b2).await.expect("ingest b2");
        svc.ingest_finalized_block(b3).await.expect("ingest b3");

        let first_id = u64::from(MAX_LOCAL_ID);
        let second_id = compose_global_log_id(log_shard(first_id) + 1, 0);
        let first_sid = stream_id("addr", &[9; 20], log_shard(first_id));
        let second_sid = stream_id("addr", &[9; 20], log_shard(second_id));

        let first_manifest = svc
            .ingest
            .meta_store
            .get(&manifest_key(&first_sid))
            .await
            .expect("first manifest get")
            .expect("first manifest");
        let second_manifest = svc
            .ingest
            .meta_store
            .get(&manifest_key(&second_sid))
            .await
            .expect("second manifest get")
            .expect("second manifest");
        assert_eq!(
            decode_manifest(&first_manifest.value)
                .expect("decode first manifest")
                .chunk_refs
                .len(),
            1
        );
        assert_eq!(
            decode_manifest(&second_manifest.value)
                .expect("decode second manifest")
                .chunk_refs
                .len(),
            1
        );

        let got = query_page(
            &svc,
            2,
            3,
            LogFilter {
                address: Some(Clause::One([9; 20])),
                topic0: None,
                topic1: None,
                topic2: None,
                topic3: None,
            },
            None,
            None,
        )
        .await
        .expect("query");
        assert_eq!(got.items.len(), 2);
        assert_eq!(got.items[0].block_num, 2);
        assert_eq!(got.items[1].block_num, 3);
    });
}

#[test]
fn invalid_parent_puts_service_in_degraded_mode() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config::default(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        let b1 = mk_block(1, [0; 32], vec![mk_log(1, 1, 1, 1, 0, 0)]);
        svc.ingest_finalized_block(b1).await.expect("ingest b1");

        let bad_b2 = mk_block(2, [9; 32], vec![mk_log(1, 1, 1, 2, 0, 0)]);
        let err = svc
            .ingest_finalized_block(bad_b2)
            .await
            .expect_err("invalid parent");
        assert!(matches!(err, Error::InvalidParent));

        let health = svc.health().await;
        assert!(health.degraded);
        assert!(health.message.contains("degraded"));

        let qerr = query_page(
            &svc,
            1,
            1,
            LogFilter {
                address: None,
                topic0: None,
                topic1: None,
                topic2: None,
                topic3: None,
            },
            None,
            None,
        )
        .await
        .expect_err("degraded query");
        assert!(matches!(qerr, Error::Degraded(_)));
    });
}

#[test]
fn gc_guardrail_fail_closed_sets_degraded() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config {
                max_orphan_chunk_bytes: 1,
                gc_guardrail_action: GuardrailAction::FailClosed,
                ..Config::default()
            },
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        let mut bm = RoaringBitmap::new();
        bm.insert(1);
        let orphan = ChunkBlob {
            min_local: 1,
            max_local: 1,
            count: 1,
            crc32: 0,
            bitmap: bm,
        };
        svc.ingest
            .blob_store
            .put_blob(
                b"chunks/orphan/0000000000000001",
                encode_chunk(&orphan).expect("encode chunk"),
            )
            .await
            .expect("put orphan");

        let stats = svc.run_gc_once().await.expect("gc");
        assert!(stats.exceeded_guardrail);
        assert!(svc.health().await.degraded);
    });
}

#[test]
fn gc_guardrail_throttle_blocks_ingest() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config {
                max_orphan_chunk_bytes: 1,
                gc_guardrail_action: GuardrailAction::Throttle,
                ..Config::default()
            },
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        let mut bm = RoaringBitmap::new();
        bm.insert(2);
        let orphan = ChunkBlob {
            min_local: 2,
            max_local: 2,
            count: 1,
            crc32: 0,
            bitmap: bm,
        };
        svc.ingest
            .blob_store
            .put_blob(
                b"chunks/orphan/0000000000000002",
                encode_chunk(&orphan).expect("encode chunk"),
            )
            .await
            .expect("put orphan");

        let _ = svc.run_gc_once().await.expect("gc");
        let err = svc
            .ingest_finalized_block(mk_block(1, [0; 32], vec![mk_log(1, 1, 1, 1, 0, 0)]))
            .await
            .expect_err("throttled ingest");
        assert!(matches!(err, Error::Throttled(_)));
        assert!(!svc.health().await.degraded);
    });
}

#[test]
fn prune_block_hash_index_hook() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config::default(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        let b1 = mk_block(1, [0; 32], vec![mk_log(1, 1, 1, 1, 0, 0)]);
        let b2 = mk_block(2, b1.block_hash, vec![mk_log(2, 2, 2, 2, 0, 0)]);
        svc.ingest_finalized_block(b1.clone())
            .await
            .expect("ingest b1");
        svc.ingest_finalized_block(b2.clone())
            .await
            .expect("ingest b2");

        let removed = svc.prune_block_hash_index_below(2).await.expect("prune");
        assert!(removed >= 1);
        assert!(
            svc.ingest
                .meta_store
                .get(&block_hash_to_num_key(&b1.block_hash))
                .await
                .expect("get b1 map")
                .is_none()
        );
        assert!(
            svc.ingest
                .meta_store
                .get(&block_hash_to_num_key(&b2.block_hash))
                .await
                .expect("get b2 map")
                .is_some()
        );
    });
}

#[test]
fn backend_failures_throttle_then_degrade() {
    block_on(async {
        let fail_counter = Arc::new(AtomicUsize::new(3));
        let svc = FinalizedIndexService::new(
            Config {
                backend_error_throttle_after: 2,
                backend_error_degraded_after: 3,
                ..Config::default()
            },
            FlakyMetaStore {
                inner: InMemoryMetaStore::default(),
                fail_get_remaining: fail_counter,
            },
            InMemoryBlobStore::default(),
            1,
        );
        let b1 = mk_block(1, [0; 32], vec![mk_log(1, 1, 1, 1, 0, 0)]);

        let e1 = svc
            .ingest_finalized_block(b1.clone())
            .await
            .expect_err("err1");
        assert!(matches!(e1, Error::Backend(_)));
        assert!(!svc.health().await.degraded);

        let e2 = svc
            .ingest_finalized_block(b1.clone())
            .await
            .expect_err("err2");
        assert!(matches!(e2, Error::Backend(_)));
        let h2 = svc.health().await;
        assert!(!h2.degraded);
        assert!(h2.message.contains("throttled"));

        let e3 = svc.ingest_finalized_block(b1).await.expect_err("err3");
        assert!(matches!(e3, Error::Throttled(_)));

        let qerr = query_page(
            &svc,
            1,
            1,
            LogFilter {
                address: None,
                topic0: None,
                topic1: None,
                topic2: None,
                topic3: None,
            },
            None,
            None,
        )
        .await
        .expect_err("backend query error");
        assert!(matches!(qerr, Error::Backend(_)));
        assert!(svc.health().await.degraded);
    });
}

#[test]
fn backend_success_clears_throttle() {
    block_on(async {
        let fail_counter = Arc::new(AtomicUsize::new(2));
        let svc = FinalizedIndexService::new(
            Config {
                backend_error_throttle_after: 2,
                backend_error_degraded_after: 10,
                ..Config::default()
            },
            FlakyMetaStore {
                inner: InMemoryMetaStore::default(),
                fail_get_remaining: fail_counter,
            },
            InMemoryBlobStore::default(),
            1,
        );
        let b1 = mk_block(1, [0; 32], vec![mk_log(1, 1, 1, 1, 0, 0)]);

        let _ = svc
            .ingest_finalized_block(b1.clone())
            .await
            .expect_err("err1");
        let _ = svc
            .ingest_finalized_block(b1.clone())
            .await
            .expect_err("err2");
        assert!(svc.health().await.message.contains("throttled"));

        let _ = svc.indexed_finalized_head().await.expect("head probe");
        svc.ingest_finalized_block(b1)
            .await
            .expect("eventual success");
        let health = svc.health().await;
        assert!(!health.degraded);
        assert_eq!(health.message, "ok");
    });
}

#[test]
fn strict_mode_keeps_cas_for_state_and_manifest_writes() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config {
                ingest_mode: IngestMode::StrictCas,
                target_entries_per_chunk: 1,
                ..Config::default()
            },
            RecordingMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        let b1 = mk_block(1, [0; 32], vec![mk_log(9, 5, 4, 1, 0, 0)]);
        svc.ingest_finalized_block(b1).await.expect("ingest b1");

        assert!(
            svc.ingest
                .meta_store
                .count_puts_with_prefix(META_STATE_KEY, |c| {
                    matches!(c, PutCond::IfAbsent | PutCond::IfVersion(_))
                })
                >= 1
        );
        assert_eq!(
            svc.ingest
                .meta_store
                .count_puts_with_prefix(META_STATE_KEY, |c| matches!(c, PutCond::Any)),
            0
        );

        assert!(
            svc.ingest
                .meta_store
                .count_puts_with_prefix(b"manifests/", |c| {
                    matches!(c, PutCond::IfAbsent | PutCond::IfVersion(_))
                })
                >= 1
        );
        assert_eq!(
            svc.ingest
                .meta_store
                .count_puts_with_prefix(b"manifests/", |c| matches!(c, PutCond::Any)),
            0
        );
    });
}

#[test]
fn fast_mode_uses_unconditional_writes_for_state_and_manifest() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config {
                ingest_mode: IngestMode::SingleWriterFast,
                target_entries_per_chunk: 1,
                ..Config::default()
            },
            RecordingMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        let b1 = mk_block(1, [0; 32], vec![mk_log(9, 5, 4, 1, 0, 0)]);
        svc.ingest_finalized_block(b1).await.expect("ingest b1");

        assert!(
            svc.ingest
                .meta_store
                .count_puts_with_prefix(META_STATE_KEY, |c| matches!(c, PutCond::Any))
                >= 1
        );
        assert_eq!(
            svc.ingest
                .meta_store
                .count_puts_with_prefix(META_STATE_KEY, |c| {
                    matches!(c, PutCond::IfAbsent | PutCond::IfVersion(_))
                }),
            0
        );

        assert!(
            svc.ingest
                .meta_store
                .count_puts_with_prefix(b"manifests/", |c| matches!(c, PutCond::Any))
                >= 1
        );
        assert_eq!(
            svc.ingest
                .meta_store
                .count_puts_with_prefix(b"manifests/", |c| {
                    matches!(c, PutCond::IfAbsent | PutCond::IfVersion(_))
                }),
            0
        );
    });
}
