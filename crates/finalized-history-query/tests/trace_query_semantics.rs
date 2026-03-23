#[allow(dead_code, unused_imports)]
mod helpers;

use finalized_history_query::api::{ExecutionBudget, FinalizedHistoryService, QueryTracesRequest};
use finalized_history_query::store::blob::InMemoryBlobStore;
use finalized_history_query::store::meta::InMemoryMetaStore;
use finalized_history_query::{Clause, Error, TraceFilter};
use futures::executor::block_on;

use helpers::*;

#[test]
fn ingest_and_query_traces_with_resume_and_post_filters() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        let block1 = finalized_history_query::FinalizedBlock {
            block_num: 1,
            block_hash: [1; 32],
            parent_hash: [0; 32],
            logs: Vec::new(),
            txs: Vec::new(),
            trace_rlp: encode_trace_block(vec![vec![
                encode_trace_frame(
                    0,
                    0,
                    [7; 20],
                    Some([9; 20]),
                    &[0, 1],
                    100,
                    90,
                    &[0xaa, 0xbb, 0xcc, 0xdd, 1],
                    &[],
                    1,
                    0,
                ),
                encode_trace_frame(
                    0,
                    0,
                    [7; 20],
                    Some([8; 20]),
                    &[],
                    100,
                    80,
                    &[0xaa, 0xbb, 0xcc, 0xdd, 2],
                    &[],
                    1,
                    1,
                ),
            ]]),
        };
        let block2 = finalized_history_query::FinalizedBlock {
            block_num: 2,
            block_hash: [2; 32],
            parent_hash: [1; 32],
            logs: Vec::new(),
            txs: Vec::new(),
            trace_rlp: encode_trace_block(vec![vec![encode_trace_frame(
                0,
                0,
                [7; 20],
                Some([9; 20]),
                &[5],
                120,
                110,
                &[0xaa, 0xbb, 0xcc, 0xdd, 3],
                &[],
                1,
                0,
            )]]),
        };

        svc.ingest_finalized_blocks(vec![block1, block2])
            .await
            .expect("ingest traces");

        let first = query_trace_page(
            &svc,
            1,
            2,
            TraceFilter {
                from: Some(Clause::One([7; 20])),
                selector: Some(Clause::One([0xaa, 0xbb, 0xcc, 0xdd])),
                has_value: Some(true),
                is_top_level: Some(true),
                ..Default::default()
            },
            1,
            None,
        )
        .await
        .expect("first trace page");
        assert_eq!(first.items.len(), 1);
        assert!(first.meta.has_more);
        assert_eq!(first.meta.next_resume_id, Some(0));
        assert_eq!(first.items[0].block_num, 1);
        assert_eq!(first.items[0].tx_idx, 0);
        assert_eq!(first.items[0].trace_idx, 0);

        let second = query_trace_page(
            &svc,
            1,
            2,
            TraceFilter {
                from: Some(Clause::One([7; 20])),
                selector: Some(Clause::One([0xaa, 0xbb, 0xcc, 0xdd])),
                has_value: Some(true),
                is_top_level: Some(true),
                ..Default::default()
            },
            1,
            first.meta.next_resume_id,
        )
        .await
        .expect("second trace page");
        assert_eq!(second.items.len(), 1);
        assert!(!second.meta.has_more);
        assert_eq!(second.items[0].block_num, 2);
        assert_eq!(second.items[0].to, Some([9; 20]));
    });
}

#[test]
fn query_traces_rejects_is_top_level_only_filter() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        svc.ingest_finalized_block(finalized_history_query::FinalizedBlock {
            block_num: 1,
            block_hash: [1; 32],
            parent_hash: [0; 32],
            logs: Vec::new(),
            txs: Vec::new(),
            trace_rlp: encode_trace_block(vec![vec![encode_trace_frame(
                0,
                0,
                [7; 20],
                Some([9; 20]),
                &[],
                100,
                90,
                &[1, 2, 3, 4],
                &[],
                1,
                0,
            )]]),
        })
        .await
        .expect("ingest trace block");

        let err = svc
            .query_traces(
                QueryTracesRequest {
                    from_block: Some(1),
                    to_block: Some(1),
                    from_block_hash: None,
                    to_block_hash: None,
                    order: finalized_history_query::QueryOrder::Ascending,
                    resume_trace_id: None,
                    limit: 10,
                    filter: TraceFilter {
                        is_top_level: Some(true),
                        ..Default::default()
                    },
                },
                ExecutionBudget::default(),
            )
            .await
            .expect_err("is_top_level-only query should fail");
        assert!(matches!(err, Error::InvalidParams(_)));
    });
}

#[test]
fn query_traces_resolves_block_hash_bounds() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        svc.ingest_finalized_block(finalized_history_query::FinalizedBlock {
            block_num: 1,
            block_hash: [1; 32],
            parent_hash: [0; 32],
            logs: Vec::new(),
            txs: Vec::new(),
            trace_rlp: encode_trace_block(vec![vec![encode_trace_frame(
                0,
                0,
                [5; 20],
                Some([6; 20]),
                &[9],
                100,
                90,
                &[0, 1, 2, 3, 4],
                &[],
                1,
                0,
            )]]),
        })
        .await
        .expect("ingest trace block");

        let page = svc
            .query_traces(
                QueryTracesRequest {
                    from_block: None,
                    to_block: None,
                    from_block_hash: Some([1; 32]),
                    to_block_hash: Some([1; 32]),
                    order: finalized_history_query::QueryOrder::Ascending,
                    resume_trace_id: None,
                    limit: 10,
                    filter: TraceFilter {
                        from: Some(Clause::One([5; 20])),
                        has_value: Some(true),
                        ..Default::default()
                    },
                },
                ExecutionBudget::default(),
            )
            .await
            .expect("query traces by block hash");

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].block_num, 1);
        assert_eq!(page.meta.resolved_from_block.hash, [1; 32]);
        assert_eq!(page.meta.resolved_to_block.hash, [1; 32]);
    });
}
