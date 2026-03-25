#[allow(dead_code, unused_imports)]
mod helpers;

use finalized_history_query::api::{
    ExecutionBudget, FinalizedHistoryService, QueryOrder, QueryTransactionsRequest,
};
use finalized_history_query::store::blob::InMemoryBlobStore;
use finalized_history_query::store::meta::InMemoryMetaStore;
use finalized_history_query::{Clause, Error, TxFilter};
use futures::executor::block_on;

use helpers::*;

#[test]
fn ingest_and_query_transactions_with_resume() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        svc.ingest_finalized_block(mk_tx_block(
            1,
            [0; 32],
            vec![
                mk_ingest_tx(
                    0,
                    [1; 32],
                    [7; 20],
                    encode_legacy_tx(Some([9; 20]), &[0xaa, 0xbb, 0xcc, 0xdd, 1]),
                ),
                mk_ingest_tx(
                    1,
                    [2; 32],
                    [8; 20],
                    encode_legacy_tx(Some([9; 20]), &[0x10, 0x20, 0x30, 0x40, 2]),
                ),
            ],
        ))
        .await
        .expect("ingest block 1");
        svc.ingest_finalized_block(mk_tx_block(
            2,
            [1; 32],
            vec![
                mk_ingest_tx(
                    0,
                    [3; 32],
                    [7; 20],
                    encode_eip1559_tx(Some([8; 20]), &[0xaa, 0xbb, 0xcc, 0xdd, 3]),
                ),
                mk_ingest_tx(1, [4; 32], [5; 20], encode_eip1559_tx(None, &[0x99, 0x88])),
            ],
        ))
        .await
        .expect("ingest block 2");

        let filter = TxFilter {
            from: Some(Clause::One([7; 20])),
            selector: Some(Clause::One([0xaa, 0xbb, 0xcc, 0xdd])),
            ..Default::default()
        };

        let first = query_tx_page(&svc, 1, 2, filter.clone(), 1, None)
            .await
            .expect("first page");
        assert_eq!(first.items.len(), 1);
        assert!(first.meta.has_more);
        assert_eq!(first.items[0].block_num(), 1);
        assert_eq!(first.items[0].tx_idx(), 0);

        let second = query_tx_page(&svc, 1, 2, filter, 1, first.meta.next_resume_id)
            .await
            .expect("second page");
        assert_eq!(second.items.len(), 1);
        assert!(!second.meta.has_more);
        assert_eq!(second.items[0].block_num(), 2);
        assert_eq!(second.items[0].to_addr().expect("to"), Some([8; 20]));
    });
}

#[test]
fn query_transactions_support_to_only_and_selector_only_filters() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        svc.ingest_finalized_block(mk_tx_block(
            1,
            [0; 32],
            vec![
                mk_ingest_tx(
                    0,
                    [1; 32],
                    [7; 20],
                    encode_legacy_tx(Some([9; 20]), &[0xaa, 0xbb, 0xcc, 0xdd, 1]),
                ),
                mk_ingest_tx(
                    1,
                    [2; 32],
                    [8; 20],
                    encode_legacy_tx(Some([9; 20]), &[0x10, 0x20, 0x30, 0x40, 2]),
                ),
                mk_ingest_tx(
                    2,
                    [3; 32],
                    [9; 20],
                    encode_eip1559_tx(None, &[0xaa, 0xbb, 0xcc, 0xdd]),
                ),
            ],
        ))
        .await
        .expect("ingest");

        let to_only = query_tx_page(
            &svc,
            1,
            1,
            TxFilter {
                to: Some(Clause::One([9; 20])),
                ..Default::default()
            },
            10,
            None,
        )
        .await
        .expect("to-only");
        assert_eq!(to_only.items.len(), 2);

        let selector_only = query_tx_page(
            &svc,
            1,
            1,
            TxFilter {
                selector: Some(Clause::One([0xaa, 0xbb, 0xcc, 0xdd])),
                ..Default::default()
            },
            10,
            None,
        )
        .await
        .expect("selector-only");
        assert_eq!(selector_only.items.len(), 1);
        assert_eq!(selector_only.items[0].tx_hash().expect("hash"), &[1; 32]);
    });
}

#[test]
fn query_transactions_rejects_filter_without_indexed_clause() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        svc.ingest_finalized_block(mk_tx_block(1, [0; 32], Vec::new()))
            .await
            .expect("ingest");

        let err = svc
            .query_transactions(
                QueryTransactionsRequest {
                    from_block: Some(1),
                    to_block: Some(1),
                    from_block_hash: None,
                    to_block_hash: None,
                    order: QueryOrder::Ascending,
                    resume_id: None,
                    limit: 10,
                    filter: TxFilter::default(),
                },
                ExecutionBudget::default(),
            )
            .await
            .expect_err("unindexed tx query should fail");
        assert!(matches!(err, Error::InvalidParams(_)));
    });
}

#[test]
fn get_tx_uses_tx_hash_index() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        svc.ingest_finalized_block(mk_tx_block(
            1,
            [0; 32],
            vec![
                mk_ingest_tx(
                    0,
                    [1; 32],
                    [7; 20],
                    encode_legacy_tx(Some([9; 20]), &[0xaa, 0xbb, 0xcc, 0xdd, 1]),
                ),
                mk_ingest_tx(
                    1,
                    [2; 32],
                    [8; 20],
                    encode_legacy_tx(Some([9; 20]), &[0x10, 0x20, 0x30, 0x40, 2]),
                ),
            ],
        ))
        .await
        .expect("ingest");

        let tx = svc.get_tx([2; 32]).await.expect("get tx").expect("tx");
        assert_eq!(tx.block_num(), 1);
        assert_eq!(tx.tx_idx(), 1);
        assert_eq!(tx.sender().expect("sender"), &[8; 20]);

        let missing = svc.get_tx([9; 32]).await.expect("missing tx");
        assert!(missing.is_none());
    });
}
