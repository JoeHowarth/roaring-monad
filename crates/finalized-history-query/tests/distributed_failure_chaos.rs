#![cfg(feature = "distributed-stores")]

use std::process::Command;
use std::sync::Arc;

use finalized_history_query::LogFilter;
use finalized_history_query::api::{
    ExecutionBudget, FinalizedHistoryService, QueryLogsRequest, QueryOrder,
};
use finalized_history_query::config::Config;
use finalized_history_query::error::Error;
use finalized_history_query::logs::types::{Block, Log};
use finalized_history_query::store::minio::MinioBlobStore;
use finalized_history_query::store::scylla::ScyllaMetaStore;

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
        txs: Vec::new(),
        traces: Vec::new(),
    }
}

fn docker_control(args: &[&str]) -> bool {
    Command::new("docker")
        .args(args)
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn minio_outage_surfaces_backend_failures_without_latching_service_state() {
    if std::env::var("RUN_DISTRIBUTED_CHAOS") != Ok("1".to_string()) {
        eprintln!("skipping distributed chaos test (set RUN_DISTRIBUTED_CHAOS=1)");
        return;
    }

    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    let keyspace = format!("finalized_history_query_chaos_{}", stamp);

    let meta = ScyllaMetaStore::new(&["127.0.0.1:9042".to_string()], &keyspace)
        .await
        .expect("connect scylla")
        .with_retry_policy(1, 10, 50);
    meta.set_min_epoch(1).await.expect("set min epoch");

    let blob = MinioBlobStore::new(
        "http://127.0.0.1:9000",
        "us-east-1",
        "minioadmin",
        "minioadmin",
        "finalized-history-query-it",
        &format!("chaos-{stamp}"),
    )
    .await
    .expect("connect minio")
    .with_retry_policy(1, 10, 50);

    let svc = FinalizedHistoryService::new_reader_writer(
        Config {
            observe_upstream_finalized_block: Arc::new(|| Some(u64::MAX / 4)),
            ..Config::default()
        },
        meta,
        blob,
        1,
    );

    let b1 = mk_block(1, [0; 32], vec![mk_log(1, 10, 20, 1, 0, 0)]);
    svc.ingest_finalized_block(b1.clone())
        .await
        .expect("ingest b1");

    assert!(docker_control(&["stop", "finalized-history-query-minio"]));

    let b2 = mk_block(2, b1.block_hash, vec![mk_log(2, 11, 21, 2, 0, 0)]);
    let e1 = svc
        .ingest_finalized_block(b2)
        .await
        .expect_err("backend fail");
    assert!(matches!(e1, Error::Backend(_)));

    let e2 = svc
        .query_logs(
            QueryLogsRequest {
                from_block: 1,
                to_block: 1,
                order: QueryOrder::Ascending,
                resume_log_id: None,
                limit: usize::MAX,
                filter: LogFilter {
                    address: None,
                    topic0: None,
                    topic1: None,
                    topic2: None,
                    topic3: None,
                },
            },
            ExecutionBudget::default(),
        )
        .await
        .expect_err("second backend fail");
    assert!(matches!(e2, Error::Backend(_)));

    let h = svc.health().await;
    assert!(
        h.healthy,
        "backend failures should not latch service health"
    );
    assert_eq!(h.message, "ok");

    let e3 = svc
        .query_logs(
            QueryLogsRequest {
                from_block: 1,
                to_block: 2,
                order: QueryOrder::Ascending,
                resume_log_id: None,
                limit: usize::MAX,
                filter: LogFilter {
                    address: None,
                    topic0: None,
                    topic1: None,
                    topic2: None,
                    topic3: None,
                },
            },
            ExecutionBudget::default(),
        )
        .await
        .expect_err("subsequent call still surfaces backend failure");
    assert!(matches!(e3, Error::Backend(_)));

    let _ = docker_control(&["start", "finalized-history-query-minio"]);
}
