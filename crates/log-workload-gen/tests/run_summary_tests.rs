use log_workload_gen::config::GeneratorConfig;
use log_workload_gen::pipeline::{run_collect, run_collect_and_generate};
use log_workload_gen::types::{ChainEvent, LogEntry, Message};
use serde_json::Value;
use tempfile::tempdir;
use tokio::sync::mpsc;

fn ev(block_number: u64, block_hash_byte: u8, addr: u8, t0: u8) -> Message {
    Message::ChainEvent(ChainEvent {
        chain_id: 1,
        block_number,
        block_hash: [block_hash_byte; 32],
        timestamp: 1_700_000_000 + block_number,
        logs: vec![LogEntry {
            tx_index: 0,
            log_index: 0,
            address: [addr; 20],
            topics: vec![[t0; 32]],
        }],
    })
}

async fn feed(messages: Vec<Message>) -> mpsc::Receiver<Message> {
    let (tx, rx) = mpsc::channel(32);
    tokio::spawn(async move {
        for msg in messages {
            tx.send(msg).await.expect("send");
        }
    });
    rx
}

#[tokio::test]
async fn run_collect_writes_run_summary_json() {
    let temp = tempdir().expect("tempdir");
    let dataset_dir = temp.path().join("dataset_summary_collect");
    let cfg = GeneratorConfig::default();

    let rx = feed(vec![
        ev(100, 0x10, 0xa1, 0xb1),
        ev(101, 0x11, 0xa2, 0xb2),
        Message::EndOfStream {
            expected_end_block: 101,
        },
    ])
    .await;

    run_collect(cfg, rx, &dataset_dir)
        .await
        .expect("run_collect");

    let bytes = std::fs::read(dataset_dir.join("run_summary.json")).expect("read run_summary");
    let v: Value = serde_json::from_slice(&bytes).expect("parse run_summary");

    assert_eq!(v["dataset_valid"], true);
    assert_eq!(v["blocks_seen"], 2);
    assert_eq!(v["logs_seen"], 2);
    assert!(v["max_queue_depth"].as_u64().expect("u64") >= 1);
}

#[tokio::test]
async fn run_collect_and_generate_updates_trace_counts_in_summary() {
    let temp = tempdir().expect("tempdir");
    let dataset_dir = temp.path().join("dataset_summary_generate");
    let cfg = GeneratorConfig {
        trace_size_per_profile: 3,
        ..GeneratorConfig::default()
    };

    let rx = feed(vec![
        ev(100, 0x10, 0xa1, 0xb1),
        ev(101, 0x11, 0xa2, 0xb2),
        ev(102, 0x12, 0xa3, 0xb3),
        Message::EndOfStream {
            expected_end_block: 102,
        },
    ])
    .await;

    run_collect_and_generate(cfg, rx, &dataset_dir, 7)
        .await
        .expect("run_collect_and_generate");

    let bytes = std::fs::read(dataset_dir.join("run_summary.json")).expect("read run_summary");
    let v: Value = serde_json::from_slice(&bytes).expect("parse run_summary");

    assert_eq!(v["trace_queries_generated"]["expected"], 3);
    assert_eq!(v["trace_queries_generated"]["stress"], 3);
    assert_eq!(v["trace_queries_generated"]["adversarial"], 3);
}
