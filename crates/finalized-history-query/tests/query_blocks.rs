#[allow(dead_code, unused_imports)]
mod helpers;

use finalized_history_query::Error;
use finalized_history_query::api::{
    ExecutionBudget, FinalizedHistoryService, QueryBlocksRequest, QueryOrder,
};
use finalized_history_query::core::state::{
    BLOCK_RECORD_TABLE, BlockRecord, BlockRecordSpec, PrimaryWindowRecord,
};
use finalized_history_query::kernel::codec::StorageCodec;
use finalized_history_query::store::blob::InMemoryBlobStore;
use finalized_history_query::store::meta::InMemoryMetaStore;
use finalized_history_query::store::publication::{MetaPublicationStore, PublicationStore};
use finalized_history_query::store::traits::{MetaStore, PutCond};
use futures::executor::block_on;

use helpers::*;

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

#[test]
fn query_blocks_returns_empty_when_range_is_beyond_published_head() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        svc.ingest_finalized_block(mk_block(1, [0; 32], Vec::new()))
            .await
            .expect("ingest block");

        let page = query_block_page(&svc, 10, 20, 5)
            .await
            .expect("query blocks beyond head");

        assert!(page.items.is_empty());
        assert!(!page.meta.has_more);
        assert_eq!(page.meta.cursor_block.number, 1);
    });
}

#[test]
fn query_blocks_clips_to_published_head() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        svc.ingest_finalized_block(mk_block(1, [0; 32], Vec::new()))
            .await
            .expect("ingest block 1");
        svc.ingest_finalized_block(mk_block(2, [1; 32], Vec::new()))
            .await
            .expect("ingest block 2");

        let page = query_block_page(&svc, 1, 99, 10)
            .await
            .expect("query blocks");

        assert_eq!(page.items.len(), 2);
        assert_eq!(page.meta.resolved_to_block.number, 2);
        assert_eq!(page.meta.cursor_block.number, 2);
    });
}

#[test]
fn query_blocks_paginates_with_exact_cursor_block() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        svc.ingest_finalized_block(mk_block(1, [0; 32], Vec::new()))
            .await
            .expect("ingest block 1");
        svc.ingest_finalized_block(mk_block(2, [1; 32], Vec::new()))
            .await
            .expect("ingest block 2");
        svc.ingest_finalized_block(mk_block(3, [2; 32], Vec::new()))
            .await
            .expect("ingest block 3");

        let page = query_block_page(&svc, 1, 3, 2)
            .await
            .expect("query first block page");

        assert_eq!(
            page.items
                .iter()
                .map(|block| block.number)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert!(page.meta.has_more);
        assert_eq!(page.meta.cursor_block.number, 2);
        assert_eq!(page.meta.next_resume_id, None);
    });
}

#[test]
fn query_blocks_budget_clamps_effective_limit() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        svc.ingest_finalized_block(mk_block(1, [0; 32], Vec::new()))
            .await
            .expect("ingest block 1");
        svc.ingest_finalized_block(mk_block(2, [1; 32], Vec::new()))
            .await
            .expect("ingest block 2");
        svc.ingest_finalized_block(mk_block(3, [2; 32], Vec::new()))
            .await
            .expect("ingest block 3");

        let page = svc
            .query_blocks(
                QueryBlocksRequest {
                    from_block: Some(1),
                    to_block: Some(3),
                    from_block_hash: None,
                    to_block_hash: None,
                    order: QueryOrder::Ascending,
                    limit: 3,
                },
                ExecutionBudget {
                    max_results: Some(2),
                },
            )
            .await
            .expect("query blocks with budget");

        assert_eq!(page.items.len(), 2);
        assert!(page.meta.has_more);
        assert_eq!(page.items[0].number, 1);
        assert_eq!(page.items[1].number, 2);
    });
}

#[test]
fn query_blocks_rejects_zero_limit_even_when_range_is_empty() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        svc.ingest_finalized_block(mk_block(1, [0; 32], Vec::new()))
            .await
            .expect("ingest block 1");

        let err = query_block_page(&svc, 10, 20, 0)
            .await
            .expect_err("zero limit should be rejected");

        assert!(matches!(
            err,
            Error::InvalidParams("limit must be at least 1")
        ));
    });
}

#[test]
fn query_blocks_rejects_zero_budget_even_when_range_is_empty() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        svc.ingest_finalized_block(mk_block(1, [0; 32], Vec::new()))
            .await
            .expect("ingest block 1");

        let err = svc
            .query_blocks(
                QueryBlocksRequest {
                    from_block: Some(10),
                    to_block: Some(20),
                    from_block_hash: None,
                    to_block_hash: None,
                    order: QueryOrder::Ascending,
                    limit: 5,
                },
                ExecutionBudget {
                    max_results: Some(0),
                },
            )
            .await
            .expect_err("zero budget should be rejected");

        assert!(matches!(
            err,
            Error::InvalidParams("budget.max_results must be at least 1 when set")
        ));
    });
}

#[test]
fn query_blocks_resolves_block_hash_bounds() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        svc.ingest_finalized_block(mk_block(1, [0; 32], Vec::new()))
            .await
            .expect("ingest block 1");
        svc.ingest_finalized_block(mk_block(2, [1; 32], Vec::new()))
            .await
            .expect("ingest block 2");

        let page = svc
            .query_blocks(
                QueryBlocksRequest {
                    from_block: None,
                    to_block: None,
                    from_block_hash: Some([2; 32]),
                    to_block_hash: Some([2; 32]),
                    order: QueryOrder::Ascending,
                    limit: 10,
                },
                ExecutionBudget::default(),
            )
            .await
            .expect("query blocks by block hash");

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].number, 2);
        assert_eq!(page.meta.resolved_from_block.hash, [2; 32]);
    });
}

#[test]
fn query_blocks_returns_empty_when_resolved_boundary_metadata_is_missing() {
    block_on(async {
        let meta = InMemoryMetaStore::default();
        let blob = InMemoryBlobStore::default();
        let publication_store = MetaPublicationStore::new(std::sync::Arc::new(meta.clone()));
        assert!(matches!(
            publication_store
                .create_if_absent(&seeded_publication_state(7, [7u8; 16], 3))
                .await
                .expect("seed publication state"),
            finalized_history_query::store::publication::CasOutcome::Applied(_)
        ));

        meta.put(
            BLOCK_RECORD_TABLE,
            &BlockRecordSpec::key(3),
            shared_block_record([3; 32], [2; 32], Some((0, 0)), Some((0, 0))).encode(),
            PutCond::Any,
        )
        .await
        .expect("seed only head block");

        let svc = FinalizedHistoryService::new_reader_only(lease_writer_config(), meta, blob);
        let page = query_block_page(&svc, 1, 3, 10)
            .await
            .expect("query blocks with missing boundary metadata");

        assert!(page.items.is_empty());
        assert_eq!(page.meta.cursor_block.number, 3);
    });
}

#[test]
fn query_blocks_fails_closed_when_interior_block_record_is_missing() {
    block_on(async {
        let meta = InMemoryMetaStore::default();
        let blob = InMemoryBlobStore::default();
        let publication_store = MetaPublicationStore::new(std::sync::Arc::new(meta.clone()));
        assert!(matches!(
            publication_store
                .create_if_absent(&seeded_publication_state(9, [9u8; 16], 3))
                .await
                .expect("seed publication state"),
            finalized_history_query::store::publication::CasOutcome::Applied(_)
        ));

        meta.put(
            BLOCK_RECORD_TABLE,
            &BlockRecordSpec::key(1),
            shared_block_record([1; 32], [0; 32], Some((0, 0)), Some((0, 0))).encode(),
            PutCond::Any,
        )
        .await
        .expect("seed block 1");
        meta.put(
            BLOCK_RECORD_TABLE,
            &BlockRecordSpec::key(3),
            shared_block_record([3; 32], [2; 32], Some((0, 0)), Some((0, 0))).encode(),
            PutCond::Any,
        )
        .await
        .expect("seed block 3");

        let svc = FinalizedHistoryService::new_reader_only(lease_writer_config(), meta, blob);
        let err = query_block_page(&svc, 1, 3, 10)
            .await
            .expect_err("interior gap should fail closed");

        assert!(matches!(err, Error::NotFound));
    });
}
