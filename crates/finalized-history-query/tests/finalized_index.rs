use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use bytes::Bytes;
use finalized_history_query::api::{
    ExecutionBudget, FinalizedHistoryService, QueryLogsRequest, QueryOrder,
};
use finalized_history_query::codec::finalized_state::{
    decode_publication_state, encode_block_meta, encode_publication_state,
};
use finalized_history_query::config::Config;
use finalized_history_query::core::state::load_finalized_head_state;
use finalized_history_query::domain::keys::{
    LOG_DIRECTORY_SUB_BUCKET_SIZE, MAX_LOCAL_ID, PUBLICATION_STATE_KEY, STREAM_PAGE_LOCAL_ID_SPAN,
    block_logs_blob_key, block_meta_key, log_directory_fragment_key, stream_fragment_blob_key,
    stream_fragment_meta_key, stream_id, stream_page_blob_key, stream_page_meta_key,
    stream_page_start_local,
};
use finalized_history_query::domain::types::{Block, BlockMeta, Log, PublicationState};
use finalized_history_query::ingest::authority::lease::{LeaseAuthority, current_time_ms};
use finalized_history_query::ingest::engine::IngestEngine;
use finalized_history_query::recovery::startup::startup_plan;
use finalized_history_query::store::blob::InMemoryBlobStore;
use finalized_history_query::store::meta::InMemoryMetaStore;
use finalized_history_query::store::publication::{CasOutcome, FenceStore, PublicationStore};
use finalized_history_query::store::traits::{
    BlobStore, DelCond, FenceToken, MetaStore, Page, PutCond, PutResult, Record,
};
use finalized_history_query::{Clause, Error, LogFilter, WriteAuthority};
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

fn seeded_publication_state(
    owner_id: u64,
    session_id: [u8; 16],
    epoch: u64,
    indexed_finalized_head: u64,
) -> PublicationState {
    seeded_publication_state_with_expiry(
        owner_id,
        session_id,
        epoch,
        indexed_finalized_head,
        u64::MAX,
    )
}

fn seeded_publication_state_with_expiry(
    owner_id: u64,
    session_id: [u8; 16],
    epoch: u64,
    indexed_finalized_head: u64,
    lease_expires_at_ms: u64,
) -> PublicationState {
    PublicationState {
        owner_id,
        session_id,
        epoch,
        indexed_finalized_head,
        lease_expires_at_ms,
    }
}

async fn query_page<A, M, B>(
    svc: &FinalizedHistoryService<A, M, B>,
    from_block: u64,
    to_block: u64,
    filter: LogFilter,
    limit: usize,
    resume_log_id: Option<u64>,
) -> finalized_history_query::Result<finalized_history_query::QueryPage<Log>>
where
    A: WriteAuthority,
    M: MetaStore + PublicationStore + FenceStore,
    B: BlobStore,
{
    svc.query_logs(
        QueryLogsRequest {
            from_block,
            to_block,
            order: QueryOrder::Ascending,
            resume_log_id,
            limit,
            filter,
        },
        ExecutionBudget::default(),
    )
    .await
}

async fn acquire_lease_token<P: PublicationStore + FenceStore + Clone>(
    store: P,
    owner_id: u64,
    now_ms: u64,
    lease_duration_ms: u64,
) -> finalized_history_query::Result<finalized_history_query::WriteToken> {
    LeaseAuthority::new(store, owner_id, lease_duration_ms, 0)
        .acquire(now_ms)
        .await
}

static CONTROLLED_NOW_MS: AtomicU64 = AtomicU64::new(0);
fn controlled_now_ms() -> u64 {
    CONTROLLED_NOW_MS.load(Ordering::Relaxed)
}

#[derive(Clone)]
struct FailDeleteOnceMetaStore {
    inner: Arc<InMemoryMetaStore>,
    failed: Arc<AtomicBool>,
}

impl MetaStore for FailDeleteOnceMetaStore {
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
        self.inner.put(key, value, cond, fence).await
    }

    async fn delete(
        &self,
        key: &[u8],
        cond: DelCond,
        fence: FenceToken,
    ) -> finalized_history_query::Result<()> {
        if key == block_meta_key(1).as_slice() && !self.failed.swap(true, Ordering::Relaxed) {
            return Err(Error::Backend("injected startup retry failure".to_string()));
        }
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

impl PublicationStore for FailDeleteOnceMetaStore {
    async fn load(&self) -> finalized_history_query::Result<Option<PublicationState>> {
        self.inner.load().await
    }

    async fn create_if_absent(
        &self,
        initial: &PublicationState,
    ) -> finalized_history_query::Result<CasOutcome<PublicationState>> {
        self.inner.create_if_absent(initial).await
    }

    async fn compare_and_set(
        &self,
        expected: &PublicationState,
        next: &PublicationState,
    ) -> finalized_history_query::Result<CasOutcome<PublicationState>> {
        self.inner.compare_and_set(expected, next).await
    }
}

impl FenceStore for FailDeleteOnceMetaStore {
    async fn advance_fence(&self, min_epoch: u64) -> finalized_history_query::Result<()> {
        self.inner.advance_fence(min_epoch).await
    }

    async fn current_fence(&self) -> finalized_history_query::Result<u64> {
        self.inner.current_fence().await
    }
}

#[derive(Clone)]
struct ExpireBeforePublishMetaStore {
    inner: Arc<InMemoryMetaStore>,
    advanced: Arc<AtomicBool>,
}

impl MetaStore for ExpireBeforePublishMetaStore {
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
        let result = self.inner.put(key, value, cond, fence).await?;
        if key != PUBLICATION_STATE_KEY && !self.advanced.swap(true, Ordering::Relaxed) {
            let publication_state = self
                .inner
                .load()
                .await?
                .expect("publication state should exist before artifact writes");
            CONTROLLED_NOW_MS.store(
                publication_state.lease_expires_at_ms.saturating_add(1),
                Ordering::Relaxed,
            );
        }
        Ok(result)
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

impl PublicationStore for ExpireBeforePublishMetaStore {
    async fn load(&self) -> finalized_history_query::Result<Option<PublicationState>> {
        self.inner.load().await
    }

    async fn create_if_absent(
        &self,
        initial: &PublicationState,
    ) -> finalized_history_query::Result<CasOutcome<PublicationState>> {
        self.inner.create_if_absent(initial).await
    }

    async fn compare_and_set(
        &self,
        expected: &PublicationState,
        next: &PublicationState,
    ) -> finalized_history_query::Result<CasOutcome<PublicationState>> {
        if next.indexed_finalized_head > expected.indexed_finalized_head
            && expected.lease_expires_at_ms < CONTROLLED_NOW_MS.load(Ordering::Relaxed)
        {
            return Ok(CasOutcome::Failed {
                current: Some(expected.clone()),
            });
        }
        self.inner.compare_and_set(expected, next).await
    }
}

impl FenceStore for ExpireBeforePublishMetaStore {
    async fn advance_fence(&self, min_epoch: u64) -> finalized_history_query::Result<()> {
        self.inner.advance_fence(min_epoch).await
    }

    async fn current_fence(&self) -> finalized_history_query::Result<u64> {
        self.inner.current_fence().await
    }
}

#[test]
fn ingest_publishes_publication_state_and_immutable_frontier_artifacts() {
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
        let publication_state = svc
            .ingest
            .meta_store
            .get(PUBLICATION_STATE_KEY)
            .await
            .expect("publication state get")
            .expect("publication state");
        let publication_state =
            decode_publication_state(&publication_state.value).expect("decode publication state");
        assert_eq!(publication_state.owner_id, 1);
        assert_eq!(publication_state.epoch, 1);
        assert_eq!(publication_state.indexed_finalized_head, 1);
        assert!(publication_state.lease_expires_at_ms > 0);
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
fn query_range_clips_to_published_head() {
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
fn acquire_publication_bootstraps_and_takeover_increments_epoch() {
    block_on(async {
        let meta = InMemoryMetaStore::default();

        let first = acquire_lease_token(meta.clone(), 7, 100, 50)
            .await
            .expect("bootstrap");
        let first_state = meta
            .load()
            .await
            .expect("load publication state")
            .expect("publication state");
        assert_eq!(first_state.owner_id, 7);
        assert_eq!(first.epoch, 1);
        assert_eq!(first.indexed_finalized_head, 0);

        let second = acquire_lease_token(meta.clone(), 9, 151, 50)
            .await
            .expect("takeover after expiry");
        let second_state = meta
            .load()
            .await
            .expect("load publication state")
            .expect("publication state");
        assert_eq!(second_state.owner_id, 9);
        assert_eq!(second.epoch, 2);
        assert_eq!(second.indexed_finalized_head, 0);
    });
}

#[test]
fn standby_writer_does_not_take_over_while_primary_lease_is_fresh() {
    block_on(async {
        let meta = InMemoryMetaStore::default();

        let _first = acquire_lease_token(meta.clone(), 7, 100, 50)
            .await
            .expect("bootstrap");
        let first_state = meta
            .load()
            .await
            .expect("load publication state")
            .expect("publication state");
        let err = acquire_lease_token(meta.clone(), 9, 120, 50)
            .await
            .expect_err("fresh lease should reject standby takeover");
        assert!(matches!(err, Error::LeaseStillFresh));

        let publication_state = meta.load().await.expect("load publication state");
        let publication_state = publication_state.expect("publication state");
        assert_eq!(publication_state.owner_id, first_state.owner_id);
        assert_eq!(publication_state.session_id, first_state.session_id);
        assert_eq!(publication_state.epoch, first_state.epoch);
    });
}

#[test]
fn readers_use_only_publication_state() {
    block_on(async {
        let meta = InMemoryMetaStore::default();
        let blob = InMemoryBlobStore::default();
        assert!(matches!(
            meta.create_if_absent(&seeded_publication_state(11, [11u8; 16], 4, 3))
                .await
                .expect("create publication state"),
            finalized_history_query::store::publication::CasOutcome::Applied(_)
        ));
        meta.put(
            &block_meta_key(3),
            encode_block_meta(&BlockMeta {
                block_hash: [3; 32],
                parent_hash: [2; 32],
                first_log_id: 9,
                count: 1,
            }),
            PutCond::Any,
            FenceToken(0),
        )
        .await
        .expect("seed block meta");

        let svc = FinalizedHistoryService::new(Config::default(), meta, blob, 11);
        assert_eq!(svc.indexed_finalized_head().await.expect("head"), 3);
        let state = load_finalized_head_state(&svc.ingest.meta_store)
            .await
            .expect("load finalized head state");
        assert_eq!(state.indexed_finalized_head, 3);
        assert_eq!(state.publication_epoch, 4);
    });
}

#[test]
fn ingest_and_query_across_24_bit_log_shard_boundary() {
    block_on(async {
        let meta = InMemoryMetaStore::default();
        let blob = InMemoryBlobStore::default();
        assert!(matches!(
            meta.create_if_absent(&seeded_publication_state_with_expiry(1, [1u8; 16], 1, 1, 0))
                .await
                .expect("seed publication state"),
            finalized_history_query::store::publication::CasOutcome::Applied(_)
        ));
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
    });
}

#[test]
fn sealed_sub_bucket_and_page_compaction_are_written_when_boundaries_close() {
    block_on(async {
        let meta = InMemoryMetaStore::default();
        let blob = InMemoryBlobStore::default();
        let first_log_id = u64::from(STREAM_PAGE_LOCAL_ID_SPAN - 1);
        assert!(matches!(
            meta.create_if_absent(&seeded_publication_state_with_expiry(1, [1u8; 16], 1, 1, 0))
                .await
                .expect("seed publication state"),
            finalized_history_query::store::publication::CasOutcome::Applied(_)
        ));
        meta.put(
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

        let svc = FinalizedHistoryService::new(Config::default(), meta, blob, 1);
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
fn publication_state_key_is_encoded_at_the_canonical_location() {
    block_on(async {
        let meta = InMemoryMetaStore::default();
        meta.put(
            PUBLICATION_STATE_KEY,
            encode_publication_state(&seeded_publication_state(1, [1u8; 16], 2, 3)),
            PutCond::Any,
            FenceToken(0),
        )
        .await
        .expect("write publication state");

        let record = meta
            .get(PUBLICATION_STATE_KEY)
            .await
            .expect("get publication state")
            .expect("publication state");
        let decoded = decode_publication_state(&record.value).expect("decode");
        assert_eq!(decoded.indexed_finalized_head, 3);
    });
}

#[test]
fn directory_fragments_exist_for_blocks_crossing_sub_bucket_boundaries() {
    block_on(async {
        let meta = InMemoryMetaStore::default();
        let blob = InMemoryBlobStore::default();
        assert!(matches!(
            meta.create_if_absent(&seeded_publication_state_with_expiry(1, [1u8; 16], 1, 1, 0))
                .await
                .expect("seed publication state"),
            finalized_history_query::store::publication::CasOutcome::Applied(_)
        ));
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
    });
}

#[test]
fn service_startup_bootstraps_publication_ownership() {
    block_on(async {
        let svc = FinalizedHistoryService::new(
            Config::default(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            5,
        );

        let plan = svc.startup().await.expect("startup");
        assert_eq!(plan.head_state.indexed_finalized_head, 0);
        assert_eq!(plan.head_state.publication_epoch, 1);
        assert_eq!(svc.indexed_finalized_head().await.expect("head"), 0);
    });
}

#[test]
fn single_writer_service_ingests_and_publishes_reader_visible_head() {
    block_on(async {
        let meta = InMemoryMetaStore::default();
        let blob = InMemoryBlobStore::default();
        let svc = FinalizedHistoryService::new_single_writer(Config::default(), meta.clone(), blob);

        svc.ingest_finalized_block(mk_block(1, [0; 32], vec![mk_log(3, 10, 20, 1, 0, 0)]))
            .await
            .expect("single-writer ingest");

        let state = meta
            .load()
            .await
            .expect("load publication state")
            .expect("publication state");
        let head = load_finalized_head_state(&meta)
            .await
            .expect("load finalized head state");

        assert_eq!(state.owner_id, 0);
        assert_eq!(state.indexed_finalized_head, 1);
        assert_eq!(head.indexed_finalized_head, 1);
    });
}

#[test]
fn service_startup_uses_configured_lease_duration() {
    block_on(async {
        let config = Config {
            publication_lease_duration_ms: 1_000,
            ..Config::default()
        };
        let before = current_time_ms();
        let svc = FinalizedHistoryService::new(
            config,
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            5,
        );

        svc.startup().await.expect("startup");
        let after = current_time_ms();
        let publication_state = svc
            .ingest
            .meta_store
            .load()
            .await
            .expect("load publication state")
            .expect("publication state");

        assert!(publication_state.lease_expires_at_ms >= before + 1_000);
        assert!(publication_state.lease_expires_at_ms <= after + 1_000);
    });
}

#[test]
fn service_can_publish_a_contiguous_batch() {
    block_on(async {
        let svc = FinalizedHistoryService::new(
            Config::default(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        svc.startup().await.expect("startup");

        let blocks = vec![
            mk_block(1, [0; 32], vec![mk_log(1, 10, 20, 1, 0, 0)]),
            mk_block(2, [1; 32], vec![mk_log(1, 10, 21, 2, 0, 0)]),
        ];
        let (outcome, head_state) = (
            svc.ingest_finalized_blocks(blocks)
                .await
                .expect("batched ingest"),
            load_finalized_head_state(&svc.ingest.meta_store)
                .await
                .expect("head state"),
        );

        assert_eq!(outcome.indexed_finalized_head, 2);
        assert_eq!(head_state.indexed_finalized_head, 2);
    });
}

#[test]
fn ingest_uses_publication_epoch_for_fenced_writes() {
    block_on(async {
        let meta = InMemoryMetaStore::with_min_epoch(5);
        let blob = InMemoryBlobStore::default();
        assert!(matches!(
            meta.create_if_absent(&seeded_publication_state_with_expiry(1, [1u8; 16], 5, 0, 0))
                .await
                .expect("seed publication state"),
            finalized_history_query::store::publication::CasOutcome::Applied(_)
        ));

        let svc = FinalizedHistoryService::new(Config::default(), meta, blob, 1);
        svc.startup().await.expect("startup");
        svc.ingest_finalized_block(mk_block(1, [0; 32], vec![mk_log(1, 10, 20, 1, 0, 0)]))
            .await
            .expect("ingest should use publication epoch as fence token");
    });
}

#[test]
fn startup_cleanup_uses_publication_epoch_for_fenced_deletes() {
    block_on(async {
        let meta = InMemoryMetaStore::with_min_epoch(5);
        let blob = InMemoryBlobStore::default();
        assert!(matches!(
            meta.create_if_absent(&seeded_publication_state_with_expiry(1, [1u8; 16], 5, 0, 0))
                .await
                .expect("seed publication state"),
            finalized_history_query::store::publication::CasOutcome::Applied(_)
        ));
        meta.put(
            &block_meta_key(1),
            encode_block_meta(&BlockMeta {
                block_hash: [1; 32],
                parent_hash: [0; 32],
                first_log_id: 0,
                count: 0,
            }),
            PutCond::Any,
            FenceToken(5),
        )
        .await
        .expect("seed unpublished block meta");

        let svc = FinalizedHistoryService::new(Config::default(), meta, blob, 1);
        svc.startup()
            .await
            .expect("startup cleanup should use publication epoch as fence token");
        assert!(
            svc.ingest
                .meta_store
                .get(&block_meta_key(1))
                .await
                .expect("read block meta after startup")
                .is_none()
        );
    });
}

#[test]
fn takeover_should_fence_stale_writer_before_artifact_writes() {
    block_on(async {
        let meta = InMemoryMetaStore::default();
        let blob = InMemoryBlobStore::default();
        let authority = LeaseAuthority::new(meta.clone(), 1, 50, 0);
        let engine = IngestEngine::new(Config::default(), authority, meta.clone(), blob);

        let stale_lease = engine
            .authority
            .acquire(100)
            .await
            .expect("writer 1 acquires publication");
        let takeover = LeaseAuthority::new(meta.clone(), 2, 50, 0);
        let _takeover_lease = takeover
            .acquire(151)
            .await
            .expect("writer 2 takes over publication");

        let err = engine
            .ingest_finalized_block(
                &mk_block(1, [0; 32], vec![mk_log(1, 10, 20, 1, 0, 0)]),
                stale_lease,
            )
            .await
            .expect_err("stale writer ingest should fail");
        assert!(matches!(err, Error::LeaseLost));

        assert!(
            engine
                .meta_store
                .get(&block_meta_key(1))
                .await
                .expect("read block meta")
                .is_none(),
            "stale writer should be fenced before writing block metadata"
        );
        assert!(
            engine
                .blob_store
                .get_blob(&block_logs_blob_key(1))
                .await
                .expect("read block blob")
                .is_none(),
            "stale writer should be fenced before writing block blobs"
        );
    });
}

#[test]
fn startup_plan_should_not_take_publication_ownership() {
    block_on(async {
        let meta = InMemoryMetaStore::default();
        let blob = InMemoryBlobStore::default();
        assert!(matches!(
            meta.create_if_absent(&seeded_publication_state(7, [7u8; 16], 3, 0))
                .await
                .expect("seed publication state"),
            finalized_history_query::store::publication::CasOutcome::Applied(_)
        ));

        let _ = startup_plan(&meta, &blob, 0)
            .await
            .expect("startup plan should succeed");

        let publication_state = meta
            .get(PUBLICATION_STATE_KEY)
            .await
            .expect("read publication state")
            .expect("publication state present");
        let publication_state =
            decode_publication_state(&publication_state.value).expect("decode publication state");
        assert_eq!(publication_state.owner_id, 7);
        assert_eq!(publication_state.epoch, 3);
    });
}

#[test]
fn startup_retry_reuses_the_same_session_after_ownership_is_acquired() {
    block_on(async {
        let inner = Arc::new(InMemoryMetaStore::default());
        inner
            .put(
                &block_meta_key(1),
                encode_block_meta(&BlockMeta {
                    block_hash: [1; 32],
                    parent_hash: [0; 32],
                    first_log_id: 0,
                    count: 0,
                }),
                PutCond::Any,
                FenceToken(0),
            )
            .await
            .expect("seed unpublished block meta");

        let svc = FinalizedHistoryService::new(
            Config::default(),
            FailDeleteOnceMetaStore {
                inner,
                failed: Arc::new(AtomicBool::new(false)),
            },
            InMemoryBlobStore::default(),
            7,
        );

        let err = svc
            .startup()
            .await
            .expect_err("first startup should fail after ownership is acquired");
        assert!(matches!(err, Error::Backend(_)));

        svc.startup()
            .await
            .expect("retry startup should reuse the same session");
    });
}

#[test]
fn ingest_renews_again_before_final_publish_when_lease_expires_mid_batch() {
    block_on(async {
        CONTROLLED_NOW_MS.store(1_000, Ordering::Relaxed);

        let config = Config {
            now_ms: controlled_now_ms,
            publication_lease_duration_ms: 50,
            publication_lease_renew_skew_ms: 0,
            ..Config::default()
        };
        let meta = ExpireBeforePublishMetaStore {
            inner: Arc::new(InMemoryMetaStore::default()),
            advanced: Arc::new(AtomicBool::new(false)),
        };
        let blob = InMemoryBlobStore::default();
        let authority = LeaseAuthority::new(meta.clone(), 1, 50, 0);
        let engine = IngestEngine::new(config, authority, meta, blob);

        let lease = engine
            .authority
            .acquire(controlled_now_ms())
            .await
            .expect("bootstrap");

        engine
            .ingest_finalized_block(
                &mk_block(1, [0; 32], vec![mk_log(1, 10, 20, 1, 0, 0)]),
                lease,
            )
            .await
            .expect("ingest should renew before final publish");
    });
}
