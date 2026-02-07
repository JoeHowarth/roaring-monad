#![cfg(feature = "distributed-stores")]

use std::process::Command;

use finalized_log_index::api::{FinalizedIndexService, FinalizedLogIndex};
use finalized_log_index::config::Config;
use finalized_log_index::domain::filter::{LogFilter, QueryOptions};
use finalized_log_index::domain::types::{Block, Log};
use finalized_log_index::error::Error;
use finalized_log_index::store::minio::MinioBlobStore;
use finalized_log_index::store::scylla::ScyllaMetaStore;

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

fn docker_control(args: &[&str]) -> bool {
    Command::new("docker")
        .args(args)
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn minio_outage_trips_retry_budget_and_degrades_service() {
    if std::env::var("RUN_DISTRIBUTED_CHAOS") != Ok("1".to_string()) {
        eprintln!("skipping distributed chaos test (set RUN_DISTRIBUTED_CHAOS=1)");
        return;
    }

    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    let keyspace = format!("finalized_index_chaos_{}", stamp);

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
        "finalized-index-it",
        &format!("chaos-{stamp}"),
    )
    .await
    .expect("connect minio")
    .with_retry_policy(1, 10, 50);

    let svc = FinalizedIndexService::new(
        Config {
            backend_error_throttle_after: 1,
            backend_error_degraded_after: 2,
            target_entries_per_chunk: 2,
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

    assert!(docker_control(&["stop", "finalized-index-minio"]));

    let b2 = mk_block(2, b1.block_hash, vec![mk_log(2, 11, 21, 2, 0, 0)]);
    let e1 = svc.ingest_finalized_block(b2).await.expect_err("backend fail");
    assert!(matches!(e1, Error::Backend(_)));

    let e2 = svc
        .query_finalized(
            LogFilter {
                from_block: Some(1),
                to_block: Some(1),
                block_hash: None,
                address: None,
                topic0: None,
                topic1: None,
                topic2: None,
                topic3: None,
            },
            QueryOptions::default(),
        )
        .await
        .expect_err("second backend fail");
    assert!(matches!(e2, Error::Backend(_)));

    let h = svc.health().await;
    assert!(h.degraded, "service should fail-closed after threshold");

    let e3 = svc
        .query_finalized(
            LogFilter {
                from_block: Some(1),
                to_block: Some(2),
                block_hash: None,
                address: None,
                topic0: None,
                topic1: None,
                topic2: None,
                topic3: None,
            },
            QueryOptions::default(),
        )
        .await
        .expect_err("degraded call blocked");
    assert!(matches!(e3, Error::Degraded(_)));

    let _ = docker_control(&["start", "finalized-index-minio"]);
}
