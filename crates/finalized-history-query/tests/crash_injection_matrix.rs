use std::sync::{Arc, Mutex};

use bytes::Bytes;
use finalized_history_query::Clause;
use finalized_history_query::LeaseAuthority;
use finalized_history_query::LogFilter;
use finalized_history_query::api::{
    ExecutionBudget, FinalizedHistoryService, QueryLogsRequest, QueryOrder,
};
use finalized_history_query::codec::finalized_state::encode_block_record;
use finalized_history_query::config::Config;
use finalized_history_query::domain::keys::{
    LOG_DIRECTORY_SUB_BUCKET_SIZE, PUBLICATION_STATE_KEY, bitmap_page_meta_key, block_record_key,
    log_dir_sub_bucket_key, stream_id, stream_page_start_local,
};
use finalized_history_query::domain::types::{Block, BlockRecord, Log, PublicationState};
use finalized_history_query::error::{Error, Result};
use finalized_history_query::startup::startup_plan;
use finalized_history_query::store::blob::InMemoryBlobStore;
use finalized_history_query::store::meta::InMemoryMetaStore;
use finalized_history_query::store::publication::{CasOutcome, PublicationStore};
use finalized_history_query::store::traits::{
    BlobStore, DelCond, MetaStore, Page, PutCond, PutResult, Record,
};
use futures::executor::block_on;

#[derive(Clone, Copy)]
enum FaultOp {
    MetaPut,
    BlobPut,
    PublicationCas,
}

#[derive(Clone)]
struct FaultPlan {
    op: FaultOp,
    prefix: Vec<u8>,
    fail_on_match: usize,
    seen_matches: usize,
    armed: bool,
}

#[derive(Default)]
struct FaultInjector {
    plan: Mutex<Option<FaultPlan>>,
}

impl FaultInjector {
    fn arm(&self, op: FaultOp, prefix: &[u8], fail_on_match: usize) {
        let mut guard = self.plan.lock().expect("injector lock");
        *guard = Some(FaultPlan {
            op,
            prefix: prefix.to_vec(),
            fail_on_match,
            seen_matches: 0,
            armed: true,
        });
    }

    fn clear(&self) {
        let mut guard = self.plan.lock().expect("injector lock");
        *guard = None;
    }

    fn maybe_fail(&self, op: FaultOp, key: &[u8]) -> Result<()> {
        let mut guard = self.plan.lock().expect("injector lock");
        let Some(plan) = guard.as_mut() else {
            return Ok(());
        };
        if !plan.armed || !matches_op(plan.op, op) || !key.starts_with(&plan.prefix) {
            return Ok(());
        }

        plan.seen_matches = plan.seen_matches.saturating_add(1);
        if plan.seen_matches == plan.fail_on_match {
            plan.armed = false;
            return Err(Error::Backend("injected crash fault".to_string()));
        }
        Ok(())
    }
}

fn matches_op(a: FaultOp, b: FaultOp) -> bool {
    matches!(
        (a, b),
        (FaultOp::MetaPut, FaultOp::MetaPut)
            | (FaultOp::BlobPut, FaultOp::BlobPut)
            | (FaultOp::PublicationCas, FaultOp::PublicationCas)
    )
}

#[derive(Clone)]
struct FaultyMetaStore {
    inner: Arc<InMemoryMetaStore>,
    injector: Arc<FaultInjector>,
}

impl MetaStore for FaultyMetaStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Record>> {
        self.inner.get(key).await
    }

    async fn put(&self, key: &[u8], value: Bytes, cond: PutCond) -> Result<PutResult> {
        self.injector.maybe_fail(FaultOp::MetaPut, key)?;
        self.inner.put(key, value, cond).await
    }

    async fn delete(&self, key: &[u8], cond: DelCond) -> Result<()> {
        self.inner.delete(key, cond).await
    }

    async fn list_prefix(
        &self,
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page> {
        self.inner.list_prefix(prefix, cursor, limit).await
    }
}

impl PublicationStore for FaultyMetaStore {
    async fn load(&self) -> Result<Option<PublicationState>> {
        self.inner.load().await
    }

    async fn create_if_absent(
        &self,
        initial: &PublicationState,
    ) -> Result<CasOutcome<PublicationState>> {
        self.injector
            .maybe_fail(FaultOp::PublicationCas, PUBLICATION_STATE_KEY)?;
        self.inner.create_if_absent(initial).await
    }

    async fn compare_and_set(
        &self,
        expected: &PublicationState,
        next: &PublicationState,
    ) -> Result<CasOutcome<PublicationState>> {
        self.injector
            .maybe_fail(FaultOp::PublicationCas, PUBLICATION_STATE_KEY)?;
        self.inner.compare_and_set(expected, next).await
    }
}

#[derive(Clone)]
struct FaultyBlobStore {
    inner: Arc<InMemoryBlobStore>,
    injector: Arc<FaultInjector>,
}

impl BlobStore for FaultyBlobStore {
    async fn put_blob(&self, key: &[u8], value: Bytes) -> Result<()> {
        self.injector.maybe_fail(FaultOp::BlobPut, key)?;
        self.inner.put_blob(key, value).await
    }

    async fn get_blob(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get_blob(key).await
    }

    async fn delete_blob(&self, key: &[u8]) -> Result<()> {
        self.inner.delete_blob(key).await
    }

    async fn list_prefix(
        &self,
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page> {
        self.inner.list_prefix(prefix, cursor, limit).await
    }
}

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

fn mk_service(
    meta: Arc<InMemoryMetaStore>,
    blob: Arc<InMemoryBlobStore>,
    injector: Arc<FaultInjector>,
) -> FinalizedHistoryService<LeaseAuthority<FaultyMetaStore>, FaultyMetaStore, FaultyBlobStore> {
    mk_service_with_writer(meta, blob, injector, 1)
}

fn mk_service_with_writer(
    meta: Arc<InMemoryMetaStore>,
    blob: Arc<InMemoryBlobStore>,
    injector: Arc<FaultInjector>,
    writer_id: u64,
) -> FinalizedHistoryService<LeaseAuthority<FaultyMetaStore>, FaultyMetaStore, FaultyBlobStore> {
    FinalizedHistoryService::new_reader_writer(
        Config {
            observe_upstream_finalized_block: Arc::new(|| Some(u64::MAX / 4)),
            ..Config::default()
        },
        FaultyMetaStore {
            inner: meta,
            injector: injector.clone(),
        },
        FaultyBlobStore {
            inner: blob,
            injector,
        },
        writer_id,
    )
}

fn indexed_address_or_filter(addresses: &[u8]) -> LogFilter {
    LogFilter {
        address: Some(Clause::Or(
            addresses.iter().map(|address| [*address; 20]).collect(),
        )),
        topic0: None,
        topic1: None,
        topic2: None,
        topic3: None,
    }
}

async fn query_range(
    svc: &FinalizedHistoryService<
        LeaseAuthority<FaultyMetaStore>,
        FaultyMetaStore,
        FaultyBlobStore,
    >,
    from_block: u64,
    to_block: u64,
) -> Vec<Log> {
    let page = svc
        .query_logs(
            QueryLogsRequest {
                from_block,
                to_block,
                order: QueryOrder::Ascending,
                resume_log_id: None,
                limit: usize::MAX,
                filter: indexed_address_or_filter(&[1, 2, 3, 7, 8]),
            },
            ExecutionBudget::default(),
        )
        .await
        .expect("query logs");
    page.items
}

#[test]
fn ingest_retry_survives_faults_at_immutable_publication_boundaries() {
    block_on(async {
        let cases = vec![
            (
                "block_log_blob_put",
                FaultOp::BlobPut,
                b"block_log_blob/".as_slice(),
            ),
            (
                "block_log_header_put",
                FaultOp::MetaPut,
                b"block_log_header/".as_slice(),
            ),
            (
                "block_record_put",
                FaultOp::MetaPut,
                b"block_record/".as_slice(),
            ),
            (
                "block_hash_index_put",
                FaultOp::MetaPut,
                b"block_hash_index/".as_slice(),
            ),
            (
                "log_dir_by_block_put",
                FaultOp::MetaPut,
                b"log_dir_by_block/".as_slice(),
            ),
            (
                "bitmap_by_block_put",
                FaultOp::MetaPut,
                b"bitmap_by_block/".as_slice(),
            ),
            (
                "publication_state_cas",
                FaultOp::PublicationCas,
                PUBLICATION_STATE_KEY,
            ),
        ];

        for (label, op, prefix) in cases {
            let injector = Arc::new(FaultInjector::default());
            let meta = Arc::new(InMemoryMetaStore::default());
            let blob = Arc::new(InMemoryBlobStore::default());
            let svc = mk_service(meta.clone(), blob.clone(), injector.clone());
            let block = mk_block(
                1,
                [0; 32],
                vec![mk_log(1, 10, 20, 1, 0, 0), mk_log(2, 11, 21, 1, 0, 1)],
            );

            injector.arm(op, prefix, 1);
            let err = svc
                .ingest_finalized_block(block.clone())
                .await
                .expect_err(label);
            assert!(matches!(err, Error::Backend(_)));
            assert_eq!(svc.indexed_finalized_head().await.expect("head"), 0);

            injector.clear();
            svc.ingest_finalized_block(block)
                .await
                .expect("retry ingest");
            let items = query_range(&svc, 1, 1).await;
            assert_eq!(items.len(), 2);
        }
    });
}

#[test]
fn failed_publication_cas_keeps_partial_artifacts_invisible_until_retry() {
    block_on(async {
        let injector = Arc::new(FaultInjector::default());
        let meta = Arc::new(InMemoryMetaStore::default());
        let blob = Arc::new(InMemoryBlobStore::default());
        let svc = mk_service(meta.clone(), blob.clone(), injector.clone());
        let block = mk_block(
            1,
            [0; 32],
            vec![mk_log(7, 10, 20, 1, 0, 0), mk_log(8, 11, 21, 1, 0, 1)],
        );

        injector.arm(FaultOp::PublicationCas, PUBLICATION_STATE_KEY, 1);
        let err = svc
            .ingest_finalized_block(block.clone())
            .await
            .expect_err("publication CAS should fail");
        assert!(matches!(err, Error::Backend(_)));
        assert_eq!(svc.indexed_finalized_head().await.expect("head"), 0);
        assert!(query_range(&svc, 1, 1).await.is_empty());

        injector.clear();
        svc.ingest_finalized_block(block)
            .await
            .expect("retry ingest");
        let items = query_range(&svc, 1, 1).await;
        assert_eq!(items.len(), 2);
    });
}

#[test]
fn takeover_without_cleanup_overwrites_different_retry_payload_for_same_block() {
    block_on(async {
        let injector = Arc::new(FaultInjector::default());
        let meta = Arc::new(InMemoryMetaStore::default());
        let blob = Arc::new(InMemoryBlobStore::default());
        let seed_first_log_id = 2_560_000u64 - 2;

        assert!(matches!(
            meta.create_if_absent(&PublicationState {
                owner_id: 1,
                session_id: [1u8; 16],
                epoch: 1,
                indexed_finalized_head: 1,
                lease_valid_through_block: 0,
            })
            .await
            .expect("seed publication state"),
            CasOutcome::Applied(_)
        ));
        meta.put(
            &block_record_key(1),
            encode_block_record(&BlockRecord {
                block_hash: [1; 32],
                parent_hash: [0; 32],
                first_log_id: seed_first_log_id,
                count: 0,
            }),
            PutCond::Any,
        )
        .await
        .expect("seed block 1 meta");

        let crashing_writer =
            mk_service_with_writer(meta.clone(), blob.clone(), injector.clone(), 1);
        crashing_writer.startup().await.expect("writer startup");
        let first_attempt = mk_block(
            2,
            [1; 32],
            vec![
                mk_log(7, 10, 20, 2, 0, 0),
                mk_log(7, 10, 21, 2, 0, 1),
                mk_log(7, 10, 22, 2, 0, 2),
            ],
        );

        injector.arm(FaultOp::PublicationCas, PUBLICATION_STATE_KEY, 1);
        let err = crashing_writer
            .ingest_finalized_block(first_attempt)
            .await
            .expect_err("publish CAS should fail after summary creation");
        assert!(matches!(err, Error::Backend(_)));

        assert!(
            meta.get(&log_dir_sub_bucket_key(
                2_560_000 - LOG_DIRECTORY_SUB_BUCKET_SIZE
            ))
            .await
            .expect("dir sub bucket")
            .is_some()
        );
        let sid = stream_id(
            "addr",
            &[7; 20],
            finalized_history_query::core::ids::LogShard::new(0).unwrap(),
        );
        let page_start = stream_page_start_local((seed_first_log_id as u32).saturating_sub(0));
        assert!(
            meta.get(&bitmap_page_meta_key(&sid, page_start))
                .await
                .expect("stream page meta")
                .is_some()
        );

        injector.clear();
        let current_state = meta
            .load()
            .await
            .expect("load publication state")
            .expect("publication state present");
        let expired_state = PublicationState {
            lease_valid_through_block: 0,
            ..current_state.clone()
        };
        assert!(matches!(
            meta.compare_and_set(&current_state, &expired_state)
                .await
                .expect("expire publication lease"),
            CasOutcome::Applied(_)
        ));
        let takeover_writer =
            mk_service_with_writer(meta.clone(), blob.clone(), injector.clone(), 2);
        takeover_writer
            .startup()
            .await
            .expect("startup without cleanup");

        let retry_block = mk_block(
            2,
            [1; 32],
            vec![
                mk_log(7, 10, 20, 2, 0, 0),
                mk_log(7, 10, 21, 2, 0, 1),
                mk_log(7, 10, 22, 2, 0, 2),
                mk_log(7, 10, 23, 2, 0, 3),
            ],
        );
        takeover_writer
            .ingest_finalized_block(retry_block)
            .await
            .expect("retry should overwrite unpublished artifacts");

        let plan = startup_plan(
            &takeover_writer.ingest.meta_store,
            &takeover_writer.ingest.blob_store,
            0,
        )
        .await
        .expect("post conflict startup plan");
        assert_eq!(plan.head_state.indexed_finalized_head, 2);
        let items = query_range(&takeover_writer, 2, 2).await;
        assert_eq!(items.len(), 4);
    });
}
