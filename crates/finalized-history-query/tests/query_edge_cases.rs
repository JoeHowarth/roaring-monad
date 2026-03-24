#[allow(dead_code, unused_imports)]
mod helpers;

use finalized_history_query::api::{
    ExecutionBudget, FinalizedHistoryService, QueryLogsRequest, QueryOrder,
};
use finalized_history_query::store::blob::InMemoryBlobStore;
use finalized_history_query::store::meta::InMemoryMetaStore;
use finalized_history_query::{Clause, Error, LogFilter};
use futures::executor::block_on;

use helpers::*;

// --- RangeResolver edge cases ---

#[test]
fn query_returns_error_when_from_block_exceeds_to_block() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        svc.ingest_finalized_block(mk_block(1, [0; 32], vec![mk_log(1, 10, 20, 1, 0, 0)]))
            .await
            .expect("ingest");

        let err = svc
            .query_logs(
                QueryLogsRequest {
                    from_block: Some(5),
                    to_block: Some(2),
                    from_block_hash: None,
                    to_block_hash: None,
                    order: QueryOrder::Ascending,
                    resume_log_id: None,
                    limit: 10,
                    filter: indexed_address_filter(1),
                },
                ExecutionBudget::default(),
            )
            .await
            .expect_err("from > to should fail");
        assert!(matches!(err, Error::InvalidParams(_)));
    });
}

#[test]
fn query_returns_empty_when_from_block_exceeds_finalized_head() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        svc.ingest_finalized_block(mk_block(1, [0; 32], vec![mk_log(1, 10, 20, 1, 0, 0)]))
            .await
            .expect("ingest");

        let page = query_page(&svc, 100, 200, indexed_address_filter(1), 10, None)
            .await
            .expect("query beyond head");
        assert!(page.items.is_empty());
        assert!(!page.meta.has_more);
    });
}

#[test]
fn query_logs_resolves_block_hash_bounds() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        svc.ingest_finalized_block(mk_block(1, [0; 32], vec![mk_log(1, 10, 20, 1, 0, 0)]))
            .await
            .expect("ingest");

        let page = svc
            .query_logs(
                QueryLogsRequest {
                    from_block: None,
                    to_block: None,
                    from_block_hash: Some([1; 32]),
                    to_block_hash: Some([1; 32]),
                    order: QueryOrder::Ascending,
                    resume_log_id: None,
                    limit: 10,
                    filter: indexed_address_filter(1),
                },
                ExecutionBudget::default(),
            )
            .await
            .expect("query logs by block hash");

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].block_num, 1);
    });
}

#[test]
fn query_returns_empty_when_no_blocks_indexed() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_only(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
        );

        let page = query_page(&svc, 1, 10, indexed_address_filter(1), 10, None)
            .await
            .expect("query on empty store");
        assert!(page.items.is_empty());
    });
}

// --- Resume log_id edge cases ---

#[test]
fn query_rejects_resume_log_id_outside_window() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        svc.ingest_finalized_block(mk_block(1, [0; 32], vec![mk_log(1, 10, 20, 1, 0, 0)]))
            .await
            .expect("ingest");

        let err = svc
            .query_logs(
                QueryLogsRequest {
                    from_block: Some(1),
                    to_block: Some(1),
                    from_block_hash: None,
                    to_block_hash: None,
                    order: QueryOrder::Ascending,
                    resume_log_id: Some(999_999),
                    limit: 10,
                    filter: indexed_address_filter(1),
                },
                ExecutionBudget::default(),
            )
            .await
            .expect_err("resume outside window");
        assert!(matches!(err, Error::InvalidParams(_)));
    });
}

#[test]
fn query_resume_at_last_log_returns_empty() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        // Block 1: one log at log_id=0
        svc.ingest_finalized_block(mk_block(1, [0; 32], vec![mk_log(1, 10, 20, 1, 0, 0)]))
            .await
            .expect("ingest");

        // Resume after the only log — window is exhausted.
        let page = query_page(&svc, 1, 1, indexed_address_filter(1), 10, Some(0))
            .await
            .expect("resume at last log");
        assert!(page.items.is_empty());
        assert!(!page.meta.has_more);
    });
}

// --- ExecutionBudget.max_results ---

#[test]
fn execution_budget_clamps_effective_limit() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        svc.ingest_finalized_block(mk_block(
            1,
            [0; 32],
            vec![
                mk_log(1, 10, 20, 1, 0, 0),
                mk_log(1, 10, 21, 1, 0, 1),
                mk_log(1, 10, 22, 1, 0, 2),
            ],
        ))
        .await
        .expect("ingest");

        // Request limit=10 but budget caps at 1.
        let page = svc
            .query_logs(
                QueryLogsRequest {
                    from_block: Some(1),
                    to_block: Some(1),
                    from_block_hash: None,
                    to_block_hash: None,
                    order: QueryOrder::Ascending,
                    resume_log_id: None,
                    limit: 10,
                    filter: indexed_address_filter(1),
                },
                ExecutionBudget {
                    max_results: Some(1),
                },
            )
            .await
            .expect("budget-capped query");
        assert_eq!(page.items.len(), 1);
        assert!(page.meta.has_more);
    });
}

#[test]
fn execution_budget_zero_is_rejected() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        svc.ingest_finalized_block(mk_block(1, [0; 32], vec![mk_log(1, 10, 20, 1, 0, 0)]))
            .await
            .expect("ingest");

        let err = svc
            .query_logs(
                QueryLogsRequest {
                    from_block: Some(1),
                    to_block: Some(1),
                    from_block_hash: None,
                    to_block_hash: None,
                    order: QueryOrder::Ascending,
                    resume_log_id: None,
                    limit: 10,
                    filter: indexed_address_filter(1),
                },
                ExecutionBudget {
                    max_results: Some(0),
                },
            )
            .await
            .expect_err("zero budget");
        assert!(matches!(err, Error::InvalidParams(_)));
    });
}

// --- Filter rejects at query level ---

#[test]
fn query_rejects_filter_without_indexed_clause() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        svc.ingest_finalized_block(mk_block(1, [0; 32], vec![mk_log(1, 10, 20, 1, 0, 0)]))
            .await
            .expect("ingest");

        let err = svc
            .query_logs(
                QueryLogsRequest {
                    from_block: Some(1),
                    to_block: Some(1),
                    from_block_hash: None,
                    to_block_hash: None,
                    order: QueryOrder::Ascending,
                    resume_log_id: None,
                    limit: 10,
                    filter: LogFilter {
                        address: Some(Clause::Any),
                        ..Default::default()
                    },
                },
                ExecutionBudget::default(),
            )
            .await
            .expect_err("no indexed clause");
        assert!(matches!(err, Error::InvalidParams(_)));
    });
}

// --- Filter match via indexed query: bitmap matches but exact_match rejects ---

#[test]
fn indexed_query_filters_out_non_matching_logs_from_same_stream() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        // Two logs share address but have different topic0.
        svc.ingest_finalized_block(mk_block(
            1,
            [0; 32],
            vec![mk_log(1, 10, 20, 1, 0, 0), mk_log(1, 99, 21, 1, 0, 1)],
        ))
        .await
        .expect("ingest");

        // Filter by address + topic0: only the first log should match.
        let filter = LogFilter {
            address: Some(Clause::One([1; 20])),
            topic0: Some(Clause::One([10; 32])),
            ..Default::default()
        };
        let page = query_page(&svc, 1, 1, filter, 10, None)
            .await
            .expect("filtered query");
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].topics[0], [10; 32]);
    });
}
