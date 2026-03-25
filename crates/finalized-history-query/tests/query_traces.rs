#[allow(dead_code, unused_imports)]
mod helpers;

use finalized_history_query::api::{ExecutionBudget, FinalizedHistoryService, QueryTracesRequest};
use finalized_history_query::store::blob::InMemoryBlobStore;
use finalized_history_query::store::meta::InMemoryMetaStore;
use finalized_history_query::{Clause, Error, TraceFilter};
use futures::executor::block_on;

use helpers::*;

fn complex_trace_blocks() -> Vec<finalized_history_query::FinalizedBlock> {
    vec![
        mk_trace_block(
            1,
            [0; 32],
            encode_trace_block(vec![vec![
                encode_trace_frame(TraceFrameParts {
                    typ: 0,
                    flags: 0,
                    from: [7; 20],
                    to: Some([9; 20]),
                    value: &[0, 1],
                    gas: 100,
                    gas_used: 90,
                    input: &[0xaa, 0xbb, 0xcc, 0xdd, 1],
                    output: &[],
                    status: 1,
                    depth: 0,
                }),
                encode_trace_frame(TraceFrameParts {
                    typ: 0,
                    flags: 0,
                    from: [8; 20],
                    to: Some([9; 20]),
                    value: &[],
                    gas: 100,
                    gas_used: 80,
                    input: &[0x10, 0x20, 0x30, 0x40, 2],
                    output: &[],
                    status: 1,
                    depth: 1,
                }),
            ]]),
        ),
        mk_trace_block(
            2,
            [1; 32],
            encode_trace_block(vec![vec![
                encode_trace_frame(TraceFrameParts {
                    typ: 0,
                    flags: 0,
                    from: [7; 20],
                    to: Some([8; 20]),
                    value: &[5],
                    gas: 120,
                    gas_used: 110,
                    input: &[0xaa, 0xbb, 0xcc, 0xdd, 3],
                    output: &[],
                    status: 1,
                    depth: 0,
                }),
                encode_trace_frame(TraceFrameParts {
                    typ: 3,
                    flags: 0,
                    from: [5; 20],
                    to: None,
                    value: &[9],
                    gas: 150,
                    gas_used: 130,
                    input: &[0x99, 0x88],
                    output: &[],
                    status: 1,
                    depth: 0,
                }),
            ]]),
        ),
        mk_trace_block(
            3,
            [2; 32],
            encode_trace_block(vec![vec![encode_trace_frame(TraceFrameParts {
                typ: 0,
                flags: 0,
                from: [4; 20],
                to: Some([6; 20]),
                value: &[1],
                gas: 130,
                gas_used: 120,
                input: &[0xde, 0xad, 0xbe, 0xef, 0],
                output: &[],
                status: 0,
                depth: 0,
            })]]),
        ),
    ]
}

fn shifted_complex_trace_blocks(start_block: u64) -> Vec<finalized_history_query::FinalizedBlock> {
    complex_trace_blocks()
        .into_iter()
        .enumerate()
        .map(|(index, mut block)| {
            let block_num = start_block + index as u64;
            block.block_num = block_num;
            block.block_hash = [block_num as u8; 32];
            block.parent_hash = if index == 0 {
                [start_block.saturating_sub(1) as u8; 32]
            } else {
                [(block_num - 1) as u8; 32]
            };
            block.header.number = block.block_num;
            block.header.hash = block.block_hash;
            block.header.parent_hash = block.parent_hash;
            block
        })
        .collect()
}

#[test]
fn ingest_and_query_traces_with_resume_and_post_filters() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        for block in complex_trace_blocks() {
            svc.ingest_finalized_block(block)
                .await
                .expect("ingest traces");
        }

        let first = query_trace_page(
            &svc,
            1,
            3,
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
        assert_eq!(first.items[0].block_num(), 1);
        assert_eq!(first.items[0].tx_idx(), 0);
        assert_eq!(first.items[0].trace_idx(), 0);

        let second = query_trace_page(
            &svc,
            1,
            3,
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
        assert_eq!(second.items[0].block_num(), 2);
        assert_eq!(
            second.items[0].call_frame().to_addr().expect("to").copied(),
            Some([8; 20])
        );
    });
}

#[test]
fn query_traces_supports_to_only_selector_only_and_has_value_only_filters() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        for block in complex_trace_blocks() {
            svc.ingest_finalized_block(block)
                .await
                .expect("ingest traces");
        }

        let to_only = query_trace_page(
            &svc,
            1,
            3,
            TraceFilter {
                to: Some(Clause::One([9; 20])),
                ..Default::default()
            },
            10,
            None,
        )
        .await
        .expect("to-only query");
        assert_eq!(to_only.items.len(), 2);
        assert_eq!(to_only.items[0].block_num(), 1);
        assert_eq!(to_only.items[1].block_num(), 1);

        let selector_only = query_trace_page(
            &svc,
            1,
            3,
            TraceFilter {
                selector: Some(Clause::One([0xaa, 0xbb, 0xcc, 0xdd])),
                ..Default::default()
            },
            10,
            None,
        )
        .await
        .expect("selector-only query");
        assert_eq!(selector_only.items.len(), 2);
        assert_eq!(selector_only.items[0].block_num(), 1);
        assert_eq!(selector_only.items[1].block_num(), 2);

        let has_value_only = query_trace_page(
            &svc,
            1,
            3,
            TraceFilter {
                has_value: Some(true),
                ..Default::default()
            },
            10,
            None,
        )
        .await
        .expect("has-value-only query");
        assert_eq!(has_value_only.items.len(), 4);
        assert_eq!(has_value_only.items[0].block_num(), 1);
        assert_eq!(has_value_only.items[1].block_num(), 2);
        assert_eq!(has_value_only.items[2].block_num(), 2);
        assert_eq!(has_value_only.items[3].block_num(), 3);
    });
}

#[test]
fn query_traces_supports_compound_filters_and_blocks_without_traces() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        svc.ingest_finalized_block(mk_trace_block(1, [0; 32], Vec::new()))
            .await
            .expect("ingest empty trace block");
        for block in shifted_complex_trace_blocks(2) {
            svc.ingest_finalized_block(block)
                .await
                .expect("ingest traces");
        }

        let page = query_trace_page(
            &svc,
            1,
            4,
            TraceFilter {
                from: Some(Clause::One([7; 20])),
                to: Some(Clause::One([8; 20])),
                has_value: Some(true),
                ..Default::default()
            },
            10,
            None,
        )
        .await
        .expect("compound trace query");

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].block_num(), 3);
        let frame = page.items[0].call_frame();
        assert_eq!(*frame.from_addr().expect("from"), [7; 20]);
        assert_eq!(frame.to_addr().expect("to").copied(), Some([8; 20]));
    });
}

#[test]
fn query_traces_support_is_top_level_only_filter() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        svc.ingest_finalized_block(mk_trace_block(
            1,
            [0; 32],
            encode_trace_block(vec![vec![encode_trace_frame(TraceFrameParts {
                typ: 0,
                flags: 0,
                from: [7; 20],
                to: Some([9; 20]),
                value: &[],
                gas: 100,
                gas_used: 90,
                input: &[1, 2, 3, 4],
                output: &[],
                status: 1,
                depth: 0,
            })]]),
        ))
        .await
        .expect("ingest trace block");

        let page = svc
            .query_traces(
                QueryTracesRequest {
                    from_block: Some(1),
                    to_block: Some(1),
                    from_block_hash: None,
                    to_block_hash: None,
                    order: finalized_history_query::QueryOrder::Ascending,
                    resume_id: None,
                    limit: 10,
                    filter: TraceFilter {
                        is_top_level: Some(true),
                        ..Default::default()
                    },
                },
                ExecutionBudget::default(),
            )
            .await
            .expect("is_top_level-only query");
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].block_num(), 1);
        assert_eq!(page.items[0].trace_idx(), 0);
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
        svc.ingest_finalized_block(mk_trace_block(
            1,
            [0; 32],
            encode_trace_block(vec![vec![encode_trace_frame(TraceFrameParts {
                typ: 0,
                flags: 0,
                from: [5; 20],
                to: Some([6; 20]),
                value: &[9],
                gas: 100,
                gas_used: 90,
                input: &[0, 1, 2, 3, 4],
                output: &[],
                status: 1,
                depth: 0,
            })]]),
        ))
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
                    resume_id: None,
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
        assert_eq!(page.items[0].block_num(), 1);
        assert_eq!(page.meta.resolved_from_block.hash, [1; 32]);
        assert_eq!(page.meta.resolved_to_block.hash, [1; 32]);
    });
}

#[test]
fn query_traces_rejects_resume_trace_id_outside_window() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        for block in complex_trace_blocks() {
            svc.ingest_finalized_block(block)
                .await
                .expect("ingest traces");
        }

        let err = query_trace_page(
            &svc,
            1,
            3,
            TraceFilter {
                from: Some(Clause::One([7; 20])),
                ..Default::default()
            },
            10,
            Some(999_999),
        )
        .await
        .expect_err("resume trace id outside window");
        assert!(matches!(err, Error::InvalidParams(_)));
    });
}
