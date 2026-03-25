use std::sync::{Arc, Mutex};

use alloy_rlp::Encodable;
use bytes::Bytes;
use finalized_history_query::Clause;
use finalized_history_query::LeaseAuthority;
use finalized_history_query::LogFilter;
use finalized_history_query::api::{
    ExecutionBudget, FinalizedHistoryService, QueryLogsRequest, QueryOrder,
};
use finalized_history_query::config::Config;
use finalized_history_query::core::state::{
    BLOCK_RECORD_TABLE, BlockRecord, BlockRecordSpec, PrimaryWindowRecord,
};
use finalized_history_query::error::{Error, Result};
use finalized_history_query::family::Families;
use finalized_history_query::kernel::codec::StorageCodec;
use finalized_history_query::kernel::sharded_streams::page_start_local;
use finalized_history_query::kernel::table_specs::{PointTableSpec, ScannableTableSpec};
use finalized_history_query::logs::table_specs::{BitmapByBlockSpec, BitmapPageMetaSpec};
use finalized_history_query::logs::types::Log;
use finalized_history_query::status::service_status;
use finalized_history_query::store::blob::InMemoryBlobStore;
use finalized_history_query::store::meta::InMemoryMetaStore;
use finalized_history_query::store::publication::PublicationState;
use finalized_history_query::store::publication::{
    CasOutcome, MetaPublicationStore, PublicationStore,
};
use finalized_history_query::store::publication::{
    PUBLICATION_STATE_SUFFIX, PUBLICATION_STATE_TABLE,
};
use finalized_history_query::store::traits::{
    BlobStore, BlobTableId, DelCond, MetaStore, Page, PutCond, PutResult, Record, ScannableTableId,
    TableId,
};
use finalized_history_query::{EvmBlockHeader, FinalizedBlock};
use futures::executor::block_on;

const STREAM_PAGE_LOCAL_ID_SPAN: u32 = 4_096;

#[derive(Clone, Copy)]
enum FailurePhase {
    ArtifactMetaWrite,
    ArtifactBlobWrite,
    PublicationStateCas,
    PublishHeadAdvance,
}

#[derive(Clone)]
struct FaultPlan {
    phase: FailurePhase,
    prefix: Vec<u8>,
    fail_on_match: usize,
    seen_matches: usize,
    armed: bool,
}

#[derive(Default)]
struct FaultInjector {
    plan: Mutex<Option<FaultPlan>>,
}

fn shared_block_record(
    block_hash: [u8; 32],
    parent_hash: [u8; 32],
    logs: Option<(u64, u32)>,
    traces: Option<(u64, u32)>,
) -> BlockRecord {
    BlockRecord {
        block_hash,
        parent_hash,
        logs: logs.map(|(first_primary_id, count)| PrimaryWindowRecord {
            first_primary_id,
            count,
        }),
        txs: Some(PrimaryWindowRecord {
            first_primary_id: 0,
            count: 0,
        }),
        traces: traces.map(|(first_primary_id, count)| PrimaryWindowRecord {
            first_primary_id,
            count,
        }),
    }
}

impl FaultInjector {
    fn arm(&self, phase: FailurePhase, prefix: &[u8], fail_on_match: usize) {
        let mut guard = self.plan.lock().expect("injector lock");
        *guard = Some(FaultPlan {
            phase,
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

    fn maybe_fail(&self, phase: FailurePhase, key: &[u8]) -> Result<()> {
        let mut guard = self.plan.lock().expect("injector lock");
        let Some(plan) = guard.as_mut() else {
            return Ok(());
        };
        if !plan.armed || !matches_phase(plan.phase, phase) || !key.starts_with(&plan.prefix) {
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

fn matches_phase(a: FailurePhase, b: FailurePhase) -> bool {
    matches!(
        (a, b),
        (
            FailurePhase::ArtifactMetaWrite,
            FailurePhase::ArtifactMetaWrite
        ) | (
            FailurePhase::ArtifactBlobWrite,
            FailurePhase::ArtifactBlobWrite
        ) | (
            FailurePhase::PublicationStateCas,
            FailurePhase::PublicationStateCas
        ) | (
            FailurePhase::PublishHeadAdvance,
            FailurePhase::PublishHeadAdvance
        )
    )
}

#[derive(Clone)]
struct FaultyMetaStore {
    inner: Arc<InMemoryMetaStore>,
    injector: Arc<FaultInjector>,
}

impl FaultyMetaStore {
    fn logical_key(family: TableId, key: &[u8]) -> Vec<u8> {
        let mut out = family.as_str().as_bytes().to_vec();
        if !key.is_empty() {
            out.push(b'/');
            out.extend_from_slice(key);
        }
        out
    }

    fn scan_logical_key(family: ScannableTableId, partition: &[u8], clustering: &[u8]) -> Vec<u8> {
        let mut out = family.as_str().as_bytes().to_vec();
        if !partition.is_empty() {
            out.push(b'/');
            out.extend_from_slice(partition);
        }
        if !clustering.is_empty() {
            out.push(b'/');
            out.extend_from_slice(clustering);
        }
        out
    }

    fn publication_state_logical_key() -> Vec<u8> {
        Self::logical_key(PUBLICATION_STATE_TABLE, PUBLICATION_STATE_SUFFIX)
    }
}

impl MetaStore for FaultyMetaStore {
    async fn get(&self, family: TableId, key: &[u8]) -> Result<Option<Record>> {
        self.inner.get(family, key).await
    }

    async fn put(
        &self,
        family: TableId,
        key: &[u8],
        value: Bytes,
        cond: PutCond,
    ) -> Result<PutResult> {
        let logical_key = Self::logical_key(family, key);
        if family == PUBLICATION_STATE_TABLE {
            self.injector
                .maybe_fail(FailurePhase::PublicationStateCas, &logical_key)?;
            if matches!(cond, PutCond::IfVersion(_))
                && let Some(current) = self.inner.get(family, key).await?
            {
                let current_state = PublicationState::decode(&current.value)?;
                let next_state = PublicationState::decode(&value)?;
                if next_state.indexed_finalized_head > current_state.indexed_finalized_head {
                    self.injector
                        .maybe_fail(FailurePhase::PublishHeadAdvance, &logical_key)?;
                }
            }
        } else {
            self.injector
                .maybe_fail(FailurePhase::ArtifactMetaWrite, &logical_key)?;
        }
        self.inner.put(family, key, value, cond).await
    }

    async fn delete(&self, family: TableId, key: &[u8], cond: DelCond) -> Result<()> {
        self.inner.delete(family, key, cond).await
    }

    async fn scan_get(
        &self,
        family: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
    ) -> Result<Option<Record>> {
        self.inner.scan_get(family, partition, clustering).await
    }

    async fn scan_put(
        &self,
        family: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
        value: Bytes,
        cond: PutCond,
    ) -> Result<PutResult> {
        let logical_key = Self::scan_logical_key(family, partition, clustering);
        self.injector
            .maybe_fail(FailurePhase::ArtifactMetaWrite, &logical_key)?;
        self.inner
            .scan_put(family, partition, clustering, value, cond)
            .await
    }

    async fn scan_delete(
        &self,
        family: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
        cond: DelCond,
    ) -> Result<()> {
        self.inner
            .scan_delete(family, partition, clustering, cond)
            .await
    }

    async fn scan_list(
        &self,
        family: ScannableTableId,
        partition: &[u8],
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page> {
        self.inner
            .scan_list(family, partition, prefix, cursor, limit)
            .await
    }
}

#[derive(Clone)]
struct FaultyBlobStore {
    inner: Arc<InMemoryBlobStore>,
    injector: Arc<FaultInjector>,
}

impl FaultyBlobStore {
    fn logical_key(table: BlobTableId, key: &[u8]) -> Vec<u8> {
        let mut out = table.as_str().as_bytes().to_vec();
        if !key.is_empty() {
            out.push(b'/');
            out.extend_from_slice(key);
        }
        out
    }
}

impl BlobStore for FaultyBlobStore {
    async fn put_blob(&self, table: BlobTableId, key: &[u8], value: Bytes) -> Result<()> {
        self.injector.maybe_fail(
            FailurePhase::ArtifactBlobWrite,
            &Self::logical_key(table, key),
        )?;
        self.inner.put_blob(table, key, value).await
    }

    async fn get_blob(&self, table: BlobTableId, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get_blob(table, key).await
    }

    async fn delete_blob(&self, table: BlobTableId, key: &[u8]) -> Result<()> {
        self.inner.delete_blob(table, key).await
    }

    async fn list_prefix(
        &self,
        table: BlobTableId,
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page> {
        self.inner.list_prefix(table, prefix, cursor, limit).await
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

fn mk_block(block_num: u64, parent_hash: [u8; 32], logs: Vec<Log>) -> FinalizedBlock {
    FinalizedBlock {
        block_num,
        block_hash: [block_num as u8; 32],
        parent_hash,
        header: EvmBlockHeader::minimal(block_num, [block_num as u8; 32], parent_hash),
        logs,
        txs: Vec::new(),
        trace_rlp: Vec::new(),
    }
}

fn encode_trace_field<T: alloy_rlp::Encodable>(value: T) -> Vec<u8> {
    let mut out = Vec::new();
    value.encode(&mut out);
    out
}

fn encode_trace_bytes(value: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    value.encode(&mut out);
    out
}

fn encode_trace_frame(from: [u8; 20], to: Option<[u8; 20]>, value: &[u8], input: &[u8]) -> Vec<u8> {
    let fields = vec![
        encode_trace_field(0u8),
        encode_trace_field(0u64),
        encode_trace_bytes(&from),
        encode_trace_bytes(to.as_ref().map(<[u8; 20]>::as_slice).unwrap_or(&[])),
        encode_trace_bytes(value),
        encode_trace_field(100u64),
        encode_trace_field(90u64),
        encode_trace_bytes(input),
        encode_trace_bytes(&[]),
        encode_trace_field(1u8),
        encode_trace_field(0u64),
    ];
    let mut out = Vec::new();
    alloy_rlp::Header {
        list: true,
        payload_length: fields.iter().map(Vec::len).sum(),
    }
    .encode(&mut out);
    for field in fields {
        out.extend_from_slice(&field);
    }
    out
}

fn encode_trace_block(txs: Vec<Vec<Vec<u8>>>) -> Vec<u8> {
    let txs = txs
        .into_iter()
        .map(|frames| {
            let mut tx = Vec::new();
            alloy_rlp::Header {
                list: true,
                payload_length: frames.iter().map(Vec::len).sum(),
            }
            .encode(&mut tx);
            for frame in frames {
                tx.extend_from_slice(&frame);
            }
            tx
        })
        .collect::<Vec<_>>();
    let mut out = Vec::new();
    alloy_rlp::Header {
        list: true,
        payload_length: txs.iter().map(Vec::len).sum(),
    }
    .encode(&mut out);
    for tx in txs {
        out.extend_from_slice(&tx);
    }
    out
}

fn mk_trace_block(block_num: u64, parent_hash: [u8; 32], trace_rlp: Vec<u8>) -> FinalizedBlock {
    FinalizedBlock {
        block_num,
        block_hash: [block_num as u8; 32],
        parent_hash,
        header: EvmBlockHeader::minimal(block_num, [block_num as u8; 32], parent_hash),
        logs: Vec::new(),
        txs: Vec::new(),
        trace_rlp,
    }
}

fn mk_service(
    meta: Arc<InMemoryMetaStore>,
    blob: Arc<InMemoryBlobStore>,
    injector: Arc<FaultInjector>,
) -> FinalizedHistoryService<
    LeaseAuthority<MetaPublicationStore<FaultyMetaStore>>,
    FaultyMetaStore,
    FaultyBlobStore,
> {
    mk_service_with_writer(meta, blob, injector, 1)
}

fn mk_service_with_writer(
    meta: Arc<InMemoryMetaStore>,
    blob: Arc<InMemoryBlobStore>,
    injector: Arc<FaultInjector>,
    writer_id: u64,
) -> FinalizedHistoryService<
    LeaseAuthority<MetaPublicationStore<FaultyMetaStore>>,
    FaultyMetaStore,
    FaultyBlobStore,
> {
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
        LeaseAuthority<MetaPublicationStore<FaultyMetaStore>>,
        FaultyMetaStore,
        FaultyBlobStore,
    >,
    from_block: u64,
    to_block: u64,
) -> Vec<Log> {
    let page = svc
        .query_logs(
            QueryLogsRequest {
                from_block: Some(from_block),
                to_block: Some(to_block),
                from_block_hash: None,
                to_block_hash: None,
                order: QueryOrder::Ascending,
                resume_id: None,
                limit: usize::MAX,
                filter: indexed_address_or_filter(&[1, 2, 3, 7, 8]),
            },
            ExecutionBudget::default(),
        )
        .await
        .expect("query logs");
    page.items
        .into_iter()
        .map(|log| log.to_owned_log())
        .collect()
}

#[test]
fn ingest_retry_survives_faults_at_immutable_publication_boundaries() {
    block_on(async {
        let cases = vec![
            (
                "block_log_blob_put",
                FailurePhase::ArtifactBlobWrite,
                b"block_log_blob/".to_vec(),
            ),
            (
                "block_log_header_put",
                FailurePhase::ArtifactMetaWrite,
                b"block_log_header/".to_vec(),
            ),
            (
                "block_record_put",
                FailurePhase::ArtifactMetaWrite,
                b"block_record/".to_vec(),
            ),
            (
                "block_hash_index_put",
                FailurePhase::ArtifactMetaWrite,
                b"block_hash_index/".to_vec(),
            ),
            (
                "log_dir_by_block_put",
                FailurePhase::ArtifactMetaWrite,
                b"log_dir_by_block/".to_vec(),
            ),
            (
                "bitmap_by_block_put",
                FailurePhase::ArtifactMetaWrite,
                b"bitmap_by_block/".to_vec(),
            ),
            (
                "publication_head_advance_cas",
                FailurePhase::PublishHeadAdvance,
                FaultyMetaStore::publication_state_logical_key(),
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

            injector.arm(op, &prefix, 1);
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

        let publication_state_key = FaultyMetaStore::publication_state_logical_key();
        injector.arm(FailurePhase::PublishHeadAdvance, &publication_state_key, 1);
        let err = svc
            .ingest_finalized_block(block.clone())
            .await
            .expect_err("publication CAS should fail");
        assert!(matches!(err, Error::Backend(_)));
        let sid = finalized_history_query::kernel::sharded_streams::sharded_stream_id(
            "addr",
            &[7; 20],
            finalized_history_query::core::ids::LogShard::new(0)
                .unwrap()
                .get(),
        );
        assert!(
            meta.get(BLOCK_RECORD_TABLE, &BlockRecordSpec::key(1))
                .await
                .expect("block record after failed publish")
                .is_some()
        );
        assert!(
            meta.scan_get(
                BitmapByBlockSpec::TABLE,
                &BitmapByBlockSpec::partition(&sid, page_start_local(0, STREAM_PAGE_LOCAL_ID_SPAN)),
                &BitmapByBlockSpec::clustering(1),
            )
            .await
            .expect("bitmap fragment after failed publish")
            .is_some()
        );
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
fn trace_publication_failure_keeps_partial_trace_artifacts_invisible_until_retry() {
    block_on(async {
        let injector = Arc::new(FaultInjector::default());
        let meta = Arc::new(InMemoryMetaStore::default());
        let blob = Arc::new(InMemoryBlobStore::default());
        let svc = mk_service(meta.clone(), blob.clone(), injector.clone());
        let block = mk_trace_block(
            1,
            [0; 32],
            encode_trace_block(vec![vec![
                encode_trace_frame([7; 20], Some([8; 20]), &[1], &[0xaa, 0, 0, 1]),
                encode_trace_frame([7; 20], Some([9; 20]), &[2], &[0xbb, 0, 0, 2]),
            ]]),
        );

        let publication_state_key = FaultyMetaStore::publication_state_logical_key();
        injector.arm(FailurePhase::PublishHeadAdvance, &publication_state_key, 1);
        let err = svc
            .ingest_finalized_block(block.clone())
            .await
            .expect_err("publication CAS should fail for trace ingest");
        assert!(matches!(err, Error::Backend(_)));
        assert_eq!(svc.indexed_finalized_head().await.expect("head"), 0);

        let writer_runtime = finalized_history_query::runtime::Runtime::new(
            svc.meta_store().clone(),
            svc.blob_store().clone(),
            finalized_history_query::tables::BytesCacheConfig::default(),
        );
        let status = service_status(
            &writer_runtime,
            &MetaPublicationStore::new(meta.clone()),
            &Families::default(),
        )
        .await
        .expect("status after failed trace publication");
        assert_eq!(status.head_state.indexed_finalized_head, 0);
        assert_eq!(status.trace_state.next_trace_id.get(), 0);

        injector.clear();
        svc.ingest_finalized_block(block)
            .await
            .expect("retry trace ingest");

        let query = svc
            .query_traces(
                finalized_history_query::QueryTracesRequest {
                    from_block: Some(1),
                    to_block: Some(1),
                    from_block_hash: None,
                    to_block_hash: None,
                    order: QueryOrder::Ascending,
                    resume_id: None,
                    limit: 10,
                    filter: finalized_history_query::TraceFilter {
                        from: Some(Clause::One([7; 20])),
                        ..Default::default()
                    },
                },
                ExecutionBudget::default(),
            )
            .await
            .expect("query traces after retry");
        assert_eq!(query.items.len(), 2);
    });
}

#[test]
fn takeover_without_cleanup_overwrites_different_retry_payload_for_same_block() {
    block_on(async {
        let injector = Arc::new(FaultInjector::default());
        let meta = Arc::new(InMemoryMetaStore::default());
        let blob = Arc::new(InMemoryBlobStore::default());
        let seed_first_log_id = 2_560_000u64 - 2;
        let publication_store = MetaPublicationStore::new(Arc::clone(&meta));

        assert!(matches!(
            publication_store
                .create_if_absent(&PublicationState {
                    owner_id: 1,
                    session_id: [1u8; 16],
                    indexed_finalized_head: 1,
                    lease_valid_through_block: 0,
                })
                .await
                .expect("seed publication state"),
            CasOutcome::Applied(_)
        ));
        meta.put(
            BLOCK_RECORD_TABLE,
            &BlockRecordSpec::key(1),
            shared_block_record([1; 32], [0; 32], Some((seed_first_log_id, 0)), Some((0, 0)))
                .encode(),
            PutCond::Any,
        )
        .await
        .expect("seed block 1 meta");

        let crashing_writer =
            mk_service_with_writer(meta.clone(), blob.clone(), injector.clone(), 1);
        let first_attempt = mk_block(
            2,
            [1; 32],
            vec![
                mk_log(7, 10, 20, 2, 0, 0),
                mk_log(7, 10, 21, 2, 0, 1),
                mk_log(7, 10, 22, 2, 0, 2),
            ],
        );

        let publication_state_key = FaultyMetaStore::publication_state_logical_key();
        injector.arm(FailurePhase::PublishHeadAdvance, &publication_state_key, 1);
        let err = crashing_writer
            .ingest_finalized_block(first_attempt)
            .await
            .expect_err("publish CAS should fail after summary creation");
        assert!(matches!(err, Error::Backend(_)));

        let sid = finalized_history_query::kernel::sharded_streams::sharded_stream_id(
            "addr",
            &[7; 20],
            finalized_history_query::core::ids::LogShard::new(0)
                .unwrap()
                .get(),
        );
        let page_start = page_start_local(
            (seed_first_log_id as u32).saturating_sub(0),
            STREAM_PAGE_LOCAL_ID_SPAN,
        );
        injector.clear();
        let current_state = publication_store
            .load()
            .await
            .expect("load publication state")
            .expect("publication state present");
        let expired_state = PublicationState {
            lease_valid_through_block: 0,
            ..current_state.clone()
        };
        assert!(matches!(
            publication_store
                .compare_and_set(&current_state, &expired_state)
                .await
                .expect("expire publication lease"),
            CasOutcome::Applied(_)
        ));
        let takeover_writer =
            mk_service_with_writer(meta.clone(), blob.clone(), injector.clone(), 2);

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

        let runtime = finalized_history_query::runtime::Runtime::new(
            takeover_writer.meta_store().clone(),
            takeover_writer.blob_store().clone(),
            finalized_history_query::tables::BytesCacheConfig::default(),
        );
        let status = service_status(
            &runtime,
            &MetaPublicationStore::new(Arc::clone(&meta)),
            &Families::default(),
        )
        .await
        .expect("post conflict status");
        assert_eq!(status.head_state.indexed_finalized_head, 2);
        assert!(
            meta.get(
                BitmapPageMetaSpec::TABLE,
                &BitmapPageMetaSpec::key(&sid, page_start)
            )
            .await
            .expect("stream page meta after retry")
            .is_some()
        );
        let items = query_range(&takeover_writer, 2, 2).await;
        assert_eq!(items.len(), 4);
    });
}
