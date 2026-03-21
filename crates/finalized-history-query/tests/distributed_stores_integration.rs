#![cfg(feature = "distributed-stores")]

use std::sync::Arc;

use finalized_history_query::api::{
    ExecutionBudget, FinalizedHistoryService, QueryLogsRequest, QueryOrder,
};
use finalized_history_query::config::Config;
use finalized_history_query::logs::types::{Block, Log};
use finalized_history_query::store::minio::MinioBlobStore;
use finalized_history_query::store::scylla::ScyllaMetaStore;
use finalized_history_query::{Clause, LogFilter};

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scylla_minio_roundtrip_query() {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    let keyspace = format!("finalized_history_query_it_{}", stamp);

    let meta = ScyllaMetaStore::new(&["127.0.0.1:9042".to_string()], &keyspace)
        .await
        .expect("connect scylla");
    meta.set_min_epoch(1).await.expect("set min epoch");

    let blob = MinioBlobStore::new(
        "http://127.0.0.1:9000",
        "us-east-1",
        "minioadmin",
        "minioadmin",
        "finalized-history-query-it",
        &format!("run-{stamp}"),
    )
    .await
    .expect("connect minio");

    let svc = FinalizedHistoryService::new_reader_writer(
        Config {
            observe_upstream_finalized_block: Arc::new(|| Some(u64::MAX / 4)),
            ..Config::default()
        },
        meta,
        blob,
        1,
    );

    let b1 = mk_block(
        1,
        [0; 32],
        vec![mk_log(1, 10, 20, 1, 0, 0), mk_log(2, 11, 21, 1, 0, 1)],
    );
    let b2 = mk_block(
        2,
        b1.block_hash,
        vec![mk_log(1, 10, 22, 2, 0, 0), mk_log(3, 12, 23, 2, 0, 1)],
    );

    svc.ingest_finalized_block(b1).await.expect("ingest b1");
    svc.ingest_finalized_block(b2).await.expect("ingest b2");

    let filter = LogFilter {
        address: Some(Clause::One([1; 20])),
        topic0: Some(Clause::One([10; 32])),
        topic1: None,
        topic2: None,
        topic3: None,
    };

    let got = svc
        .query_logs(
            QueryLogsRequest {
                from_block: 1,
                to_block: 2,
                order: QueryOrder::Ascending,
                resume_log_id: None,
                limit: 100,
                filter,
            },
            ExecutionBudget::default(),
        )
        .await
        .expect("query");

    assert_eq!(got.items.len(), 2);
    assert_eq!(got.items[0].block_num, 1);
    assert_eq!(got.items[1].block_num, 2);
}
