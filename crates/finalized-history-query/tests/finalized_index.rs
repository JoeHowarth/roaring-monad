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
    block_meta_key, log_directory_fragment_key, stream_fragment_blob_key, stream_fragment_meta_key,
    stream_id, stream_page_blob_key, stream_page_meta_key, stream_page_start_local,
};
use finalized_history_query::domain::types::{Block, BlockMeta, Log, PublicationState};
use finalized_history_query::ingest::publication::acquire_publication;
use finalized_history_query::store::blob::InMemoryBlobStore;
use finalized_history_query::store::meta::InMemoryMetaStore;
use finalized_history_query::store::publication::PublicationStore;
use finalized_history_query::store::traits::{BlobStore, FenceToken, MetaStore, PutCond};
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

async fn query_page<M, B>(
    svc: &FinalizedHistoryService<M, B>,
    from_block: u64,
    to_block: u64,
    filter: LogFilter,
    limit: usize,
    resume_log_id: Option<u64>,
) -> finalized_history_query::Result<finalized_history_query::QueryPage<Log>>
where
    M: MetaStore + PublicationStore,
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
        assert_eq!(
            publication_state,
            PublicationState {
                owner_id: 1,
                epoch: 1,
                indexed_finalized_head: 1,
            }
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

        let first = acquire_publication(&meta, 7).await.expect("bootstrap");
        assert_eq!(first.owner_id, 7);
        assert_eq!(first.epoch, 1);
        assert_eq!(first.indexed_finalized_head, 0);

        let second = acquire_publication(&meta, 9).await.expect("takeover");
        assert_eq!(second.owner_id, 9);
        assert_eq!(second.epoch, 2);
        assert_eq!(second.indexed_finalized_head, 0);
    });
}

#[test]
fn readers_use_only_publication_state() {
    block_on(async {
        let meta = InMemoryMetaStore::default();
        let blob = InMemoryBlobStore::default();
        assert!(matches!(
            meta.create_if_absent(&PublicationState {
                owner_id: 11,
                epoch: 4,
                indexed_finalized_head: 3,
            })
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
            meta.create_if_absent(&PublicationState {
                owner_id: 1,
                epoch: 1,
                indexed_finalized_head: 1,
            })
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
            meta.create_if_absent(&PublicationState {
                owner_id: 1,
                epoch: 1,
                indexed_finalized_head: 1,
            })
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
            encode_publication_state(&PublicationState {
                owner_id: 1,
                epoch: 2,
                indexed_finalized_head: 3,
            }),
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
            meta.create_if_absent(&PublicationState {
                owner_id: 1,
                epoch: 1,
                indexed_finalized_head: 1,
            })
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
