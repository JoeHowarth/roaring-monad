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
fn query_blocks_materializes_full_stored_header() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        let mut block = mk_block(1, [0; 32], Vec::new());
        block.header.ommers_hash = [1; 32];
        block.header.beneficiary = [2; 20];
        block.header.state_root = [3; 32];
        block.header.transactions_root = [4; 32];
        block.header.receipts_root = [5; 32];
        block.header.logs_bloom = [[6; 64]; 4];
        block.header.difficulty = [7; 32];
        block.header.gas_limit = 30_000_000;
        block.header.gas_used = 21_000;
        block.header.timestamp = 1_700_000_001;
        block.header.extra_data = vec![9, 8, 7];
        block.header.mix_hash = [10; 32];
        block.header.nonce = [11; 8];
        block.header.base_fee_per_gas = Some([12; 32]);
        block.header.withdrawals_root = Some([13; 32]);
        block.header.blob_gas_used = Some(14);
        block.header.excess_blob_gas = Some(15);
        block.header.parent_beacon_block_root = Some([16; 32]);
        block.header.requests_hash = Some([17; 32]);
        svc.ingest_finalized_block(block)
            .await
            .expect("ingest block");

        let page = query_block_page(&svc, 1, 1, 1).await.expect("query blocks");

        assert_eq!(page.items.len(), 1);
        let item = &page.items[0];
        assert_eq!(item.number, 1);
        assert_eq!(item.hash, [1; 32]);
        assert_eq!(item.parent_hash, [0; 32]);
        assert_eq!(item.ommers_hash, [1; 32]);
        assert_eq!(item.beneficiary, [2; 20]);
        assert_eq!(item.state_root, [3; 32]);
        assert_eq!(item.transactions_root, [4; 32]);
        assert_eq!(item.receipts_root, [5; 32]);
        assert_eq!(item.logs_bloom, [[6; 64]; 4]);
        assert_eq!(item.difficulty, [7; 32]);
        assert_eq!(item.gas_limit, 30_000_000);
        assert_eq!(item.gas_used, 21_000);
        assert_eq!(item.timestamp, 1_700_000_001);
        assert_eq!(item.extra_data, vec![9, 8, 7]);
        assert_eq!(item.mix_hash, [10; 32]);
        assert_eq!(item.nonce, [11; 8]);
        assert_eq!(item.base_fee_per_gas, Some([12; 32]));
        assert_eq!(item.withdrawals_root, Some([13; 32]));
        assert_eq!(item.blob_gas_used, Some(14));
        assert_eq!(item.excess_blob_gas, Some(15));
        assert_eq!(item.parent_beacon_block_root, Some([16; 32]));
        assert_eq!(item.requests_hash, Some([17; 32]));
    });
}

#[test]
fn get_block_header_by_returns_ingested_header() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        let mut block = mk_block(1, [0; 32], Vec::new());
        block.header.timestamp = 42;
        block.header.gas_limit = 99;
        svc.ingest_finalized_block(block)
            .await
            .expect("ingest block");

        let header = svc
            .get_block_header_by(1)
            .await
            .expect("get header")
            .expect("header");
        assert_eq!(header.number, 1);
        assert_eq!(header.timestamp, 42);
        assert_eq!(header.gas_limit, 99);
    });
}

#[test]
fn get_block_and_query_blocks_hydrate_block_transactions() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        let txs = vec![
            mk_ingest_tx(
                0,
                [1; 32],
                [2; 20],
                encode_legacy_tx(Some([3; 20]), &[0xaa, 0xbb, 0xcc, 0xdd, 1]),
            ),
            mk_ingest_tx(
                1,
                [4; 32],
                [5; 20],
                encode_legacy_tx(None, &[0x11, 0x22, 0x33]),
            ),
        ];
        svc.ingest_finalized_block(mk_tx_block(1, [0; 32], txs))
            .await
            .expect("ingest tx block");

        let block = svc.get_block(1).await.expect("get block").expect("block");
        assert_eq!(block.number, 1);
        assert_eq!(block.txs.len(), 2);
        assert_eq!(block.txs[0].tx_idx(), 0);
        assert_eq!(block.txs[1].tx_idx(), 1);

        let page = query_block_page(&svc, 1, 1, 1).await.expect("query blocks");
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].txs.len(), 2);
        assert_eq!(
            page.items[0].txs[0].tx_hash().expect("first hash"),
            &[1; 32]
        );
        assert_eq!(
            page.items[0].txs[1].tx_hash().expect("second hash"),
            &[4; 32]
        );
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
