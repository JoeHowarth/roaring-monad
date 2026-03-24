use std::sync::Arc;

use alloy_rlp::Encodable;
use finalized_history_query::api::{
    ExecutionBudget, FinalizedHistoryService, QueryLogsRequest, QueryOrder, QueryTracesRequest,
};
use finalized_history_query::config::Config;
use finalized_history_query::core::ids::LogId;
use finalized_history_query::family::Families;
use finalized_history_query::logs::types::Log;
use finalized_history_query::store::blob::InMemoryBlobStore;
use finalized_history_query::store::meta::InMemoryMetaStore;
use finalized_history_query::store::publication::MetaPublicationStore;
use finalized_history_query::{Clause, FinalizedBlock, LeaseAuthority, LogFilter, TraceFilter};
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

fn mk_block(block_num: u64, parent_hash: [u8; 32], logs: Vec<Log>) -> FinalizedBlock {
    FinalizedBlock {
        block_num,
        block_hash: [block_num as u8; 32],
        parent_hash,
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

fn encode_trace_frame(
    from: [u8; 20],
    to: Option<[u8; 20]>,
    value: &[u8],
    input: &[u8],
    status: u8,
    depth: u64,
) -> Vec<u8> {
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
        encode_trace_field(status),
        encode_trace_field(depth),
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
        logs: Vec::new(),
        txs: Vec::new(),
        trace_rlp,
    }
}

fn naive_trace_query(
    blocks: &[FinalizedBlock],
    from_block: u64,
    to_block: u64,
    filter: &TraceFilter,
    max_results: Option<usize>,
) -> Vec<(u64, u32, u32, [u8; 20], Option<[u8; 20]>)> {
    let mut out = Vec::new();

    for block in blocks {
        if block.block_num < from_block || block.block_num > to_block {
            continue;
        }

        let iter = finalized_history_query::traces::view::BlockTraceIter::new(&block.trace_rlp)
            .expect("trace iter");
        for frame in iter {
            let frame = frame.expect("frame");
            let view = frame.view;
            let from = *view.from_addr().expect("from");
            let to = view.to_addr().expect("to").copied();
            let selector = view.selector().expect("selector").copied();
            let has_value = view.has_value().expect("has_value");
            let depth = view.depth().expect("depth");

            let matches_from = match &filter.from {
                None | Some(Clause::Any) => true,
                Some(Clause::One(value)) => value == &from,
                Some(Clause::Or(values)) => values.iter().any(|value| value == &from),
            };
            let matches_to = match &filter.to {
                None | Some(Clause::Any) => true,
                Some(Clause::One(value)) => to.as_ref() == Some(value),
                Some(Clause::Or(values)) => to
                    .as_ref()
                    .map(|actual| values.iter().any(|value| value == actual))
                    .unwrap_or(false),
            };
            let matches_selector = match &filter.selector {
                None | Some(Clause::Any) => true,
                Some(Clause::One(value)) => selector.as_ref() == Some(value),
                Some(Clause::Or(values)) => selector
                    .as_ref()
                    .map(|actual| values.iter().any(|value| value == actual))
                    .unwrap_or(false),
            };
            let matches_top_level = match filter.is_top_level {
                None => true,
                Some(expected) => (depth == 0) == expected,
            };
            let matches_has_value = match filter.has_value {
                None => true,
                Some(expected) => has_value == expected,
            };

            if matches_from
                && matches_to
                && matches_selector
                && matches_top_level
                && matches_has_value
            {
                out.push((block.block_num, frame.tx_idx, frame.trace_idx, from, to));
                if let Some(limit) = max_results
                    && out.len() >= limit
                {
                    return out;
                }
            }
        }
    }

    out
}

async fn query_trace_range(
    svc: &FinalizedHistoryService<
        LeaseAuthority<MetaPublicationStore<InMemoryMetaStore>>,
        InMemoryMetaStore,
        InMemoryBlobStore,
    >,
    from_block: u64,
    to_block: u64,
    filter: TraceFilter,
    max_results: Option<usize>,
) -> Vec<(u64, u32, u32, [u8; 20], Option<[u8; 20]>)> {
    let page = svc
        .query_traces(
            QueryTracesRequest {
                from_block: Some(from_block),
                to_block: Some(to_block),
                from_block_hash: None,
                to_block_hash: None,
                order: QueryOrder::Ascending,
                resume_trace_id: None,
                limit: max_results.unwrap_or(usize::MAX),
                filter,
            },
            ExecutionBudget::default(),
        )
        .await
        .expect("query traces");
    page.items
        .into_iter()
        .map(|trace| {
            (
                trace.block_num,
                trace.tx_idx,
                trace.trace_idx,
                trace.from,
                trace.to,
            )
        })
        .collect()
}

fn naive_query(
    blocks: &[FinalizedBlock],
    from_block: u64,
    to_block: u64,
    filter: &LogFilter,
    max_results: Option<usize>,
) -> Vec<Log> {
    let mut out = Vec::new();

    for b in blocks {
        if b.block_num < from_block || b.block_num > to_block {
            continue;
        }

        for l in &b.logs {
            if !matches_address(l, &filter.address)
                || !matches_topic(l.topics.first().copied(), &filter.topic0)
                || !matches_topic(l.topics.get(1).copied(), &filter.topic1)
                || !matches_topic(l.topics.get(2).copied(), &filter.topic2)
                || !matches_topic(l.topics.get(3).copied(), &filter.topic3)
            {
                continue;
            }
            out.push(l.clone());
            if let Some(limit) = max_results
                && out.len() >= limit
            {
                return out;
            }
        }
    }

    out.sort_by_key(|l| (l.block_num, l.tx_idx, l.log_idx));
    out
}

async fn query_range(
    svc: &FinalizedHistoryService<
        LeaseAuthority<MetaPublicationStore<InMemoryMetaStore>>,
        InMemoryMetaStore,
        InMemoryBlobStore,
    >,
    from_block: u64,
    to_block: u64,
    filter: LogFilter,
    max_results: Option<usize>,
) -> Vec<Log> {
    let page = svc
        .query_logs(
            QueryLogsRequest {
                from_block: Some(from_block),
                to_block: Some(to_block),
                from_block_hash: None,
                to_block_hash: None,
                order: QueryOrder::Ascending,
                resume_log_id: None,
                limit: max_results.unwrap_or(usize::MAX),
                filter,
            },
            ExecutionBudget::default(),
        )
        .await
        .expect("query");
    page.items
}

fn matches_address(log: &Log, clause: &Option<Clause<[u8; 20]>>) -> bool {
    match clause {
        None => true,
        Some(Clause::Any) => true,
        Some(Clause::One(v)) => &log.address == v,
        Some(Clause::Or(vs)) => vs.iter().any(|v| v == &log.address),
    }
}

fn matches_topic(topic: Option<[u8; 32]>, clause: &Option<Clause<[u8; 32]>>) -> bool {
    match clause {
        None => true,
        Some(Clause::Any) => true,
        Some(Clause::One(v)) => topic.as_ref() == Some(v),
        Some(Clause::Or(vs)) => topic
            .as_ref()
            .map(|t| vs.iter().any(|v| v == t))
            .unwrap_or(false),
    }
}

#[test]
fn differential_query_matches_naive() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            Config {
                observe_upstream_finalized_block: Arc::new(|| Some(u64::MAX / 4)),
                planner_max_or_terms: 10,
                ..Config::default()
            },
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        let blocks = vec![
            mk_block(
                1,
                [0; 32],
                vec![mk_log(1, 10, 20, 1, 0, 0), mk_log(2, 11, 21, 1, 0, 1)],
            ),
            mk_block(
                2,
                [1; 32],
                vec![mk_log(1, 10, 22, 2, 0, 0), mk_log(3, 12, 23, 2, 0, 1)],
            ),
            mk_block(
                3,
                [2; 32],
                vec![mk_log(2, 10, 24, 3, 0, 0), mk_log(4, 13, 25, 3, 0, 1)],
            ),
        ];

        for b in &blocks {
            svc.ingest_finalized_block(b.clone()).await.expect("ingest");
        }

        let filters = vec![
            (
                1,
                3,
                LogFilter {
                    address: Some(Clause::Or(vec![[1; 20], [2; 20]])),
                    topic0: Some(Clause::One([10; 32])),
                    topic1: None,
                    topic2: None,
                    topic3: None,
                },
            ),
            (
                2,
                3,
                LogFilter {
                    address: None,
                    topic0: Some(Clause::Or(vec![[12; 32], [13; 32]])),
                    topic1: Some(Clause::Any),
                    topic2: None,
                    topic3: None,
                },
            ),
            (
                1,
                3,
                LogFilter {
                    address: Some(Clause::Or(vec![[1; 20], [4; 20]])),
                    topic0: None,
                    topic1: None,
                    topic2: None,
                    topic3: None,
                },
            ),
        ];

        for (from_block, to_block, filter) in filters {
            let got = query_range(&svc, from_block, to_block, filter.clone(), Some(3)).await;
            let want = naive_query(&blocks, from_block, to_block, &filter, Some(3));
            assert_eq!(got, want);
        }
    });
}

#[test]
fn recovery_startup_smoke_check() {
    block_on(async {
        let meta = InMemoryMetaStore::default();
        let blob = InMemoryBlobStore::default();

        let runtime = finalized_history_query::runtime::Runtime::new(
            meta.clone(),
            blob.clone(),
            finalized_history_query::tables::BytesCacheConfig::default(),
        );
        let publication_store = MetaPublicationStore::new(std::sync::Arc::new(meta));
        let rec = finalized_history_query::status::service_status(
            &runtime,
            &publication_store,
            &Families::default(),
            0,
        )
        .await
        .expect("service status");
        assert_eq!(rec.head_state.indexed_finalized_head, 0);
        assert_eq!(rec.log_state.next_log_id, LogId::new(0));
    });
}

#[test]
fn differential_trace_query_matches_naive() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            Config {
                observe_upstream_finalized_block: Arc::new(|| Some(u64::MAX / 4)),
                planner_max_or_terms: 10,
                ..Config::default()
            },
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        let blocks = vec![
            mk_trace_block(
                1,
                [0; 32],
                encode_trace_block(vec![vec![
                    encode_trace_frame([1; 20], Some([7; 20]), &[1], &[0xaa, 0, 0, 1], 1, 0),
                    encode_trace_frame([2; 20], Some([8; 20]), &[], &[0xbb, 0, 0, 2], 1, 1),
                ]]),
            ),
            mk_trace_block(
                2,
                [1; 32],
                encode_trace_block(vec![vec![
                    encode_trace_frame([1; 20], Some([9; 20]), &[5], &[0xaa, 0, 0, 3], 1, 0),
                    encode_trace_frame([3; 20], None, &[7], &[0xcc, 0, 0, 4], 1, 0),
                ]]),
            ),
            mk_trace_block(
                3,
                [2; 32],
                encode_trace_block(vec![vec![encode_trace_frame(
                    [4; 20],
                    Some([7; 20]),
                    &[1],
                    &[0xdd, 0, 0, 5],
                    0,
                    0,
                )]]),
            ),
        ];

        for block in &blocks {
            svc.ingest_finalized_block(block.clone())
                .await
                .expect("ingest");
        }

        let filters = vec![
            (
                1,
                3,
                TraceFilter {
                    from: Some(Clause::Or(vec![[1; 20], [2; 20]])),
                    ..Default::default()
                },
            ),
            (
                1,
                3,
                TraceFilter {
                    to: Some(Clause::One([7; 20])),
                    has_value: Some(true),
                    ..Default::default()
                },
            ),
            (
                1,
                3,
                TraceFilter {
                    selector: Some(Clause::Or(vec![[0xaa, 0, 0, 1], [0xaa, 0, 0, 3]])),
                    is_top_level: Some(true),
                    ..Default::default()
                },
            ),
        ];

        for (from_block, to_block, filter) in filters {
            let got = query_trace_range(&svc, from_block, to_block, filter.clone(), Some(3)).await;
            let want = naive_trace_query(&blocks, from_block, to_block, &filter, Some(3));
            assert_eq!(got, want);
        }
    });
}
