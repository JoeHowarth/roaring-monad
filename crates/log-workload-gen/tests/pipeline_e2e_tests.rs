use log_workload_gen::artifact::read_dataset_manifest;
use log_workload_gen::config::GeneratorConfig;
use log_workload_gen::pipeline::{run_collect, run_collect_and_generate, run_offline_generate};
use log_workload_gen::types::{ChainEvent, LogEntry, Message};
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
async fn run_collect_writes_required_artifacts() {
    let temp = tempdir().expect("tempdir");
    let dataset_dir = temp.path().join("dataset_collect");
    let cfg = GeneratorConfig {
        trace_size_per_profile: 5,
        ..GeneratorConfig::default()
    };

    let rx = feed(vec![
        ev(100, 0x10, 0xa1, 0xb1),
        ev(101, 0x11, 0xa1, 0xb2),
        Message::EndOfStream {
            expected_end_block: 101,
        },
    ])
    .await;

    let out = run_collect(cfg, rx, &dataset_dir)
        .await
        .expect("run_collect");

    assert!(out.valid);
    assert!(dataset_dir.join("dataset_manifest.json").exists());
    assert!(dataset_dir.join("key_stats.parquet").exists());
    assert!(dataset_dir.join("cooccurrence.parquet").exists());
    assert!(dataset_dir.join("range_stats.parquet").exists());
    assert!(!dataset_dir.join("trace_expected.jsonl").exists());
}

#[tokio::test]
async fn run_collect_respects_small_event_queue_capacity() {
    let temp = tempdir().expect("tempdir");
    let dataset_dir = temp.path().join("dataset_small_queue");
    let cfg = GeneratorConfig {
        trace_size_per_profile: 5,
        event_queue_capacity: 1,
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

    let out = run_collect(cfg, rx, &dataset_dir)
        .await
        .expect("run_collect");
    assert!(out.valid);
}

#[tokio::test]
async fn run_collect_and_generate_writes_trace_files() {
    let temp = tempdir().expect("tempdir");
    let dataset_dir = temp.path().join("dataset_collect_generate");
    let cfg = GeneratorConfig {
        trace_size_per_profile: 5,
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

    let out = run_collect_and_generate(cfg, rx, &dataset_dir, 7)
        .await
        .expect("run_collect_and_generate");

    assert_eq!(out.expected, 5);
    assert_eq!(out.stress, 5);
    assert_eq!(out.adversarial, 5);
    assert!(dataset_dir.join("trace_expected.jsonl").exists());
    assert!(dataset_dir.join("trace_stress.jsonl").exists());
    assert!(dataset_dir.join("trace_adversarial.jsonl").exists());
    let manifest = read_dataset_manifest(&dataset_dir).expect("manifest");
    assert_eq!(manifest.seed, Some(7));
}

#[tokio::test]
async fn run_offline_generate_uses_existing_dataset() {
    let temp = tempdir().expect("tempdir");
    let dataset_dir = temp.path().join("dataset_offline");
    let cfg = GeneratorConfig {
        trace_size_per_profile: 4,
        ..GeneratorConfig::default()
    };

    let rx = feed(vec![
        ev(200, 0x20, 0xa1, 0xb1),
        ev(201, 0x21, 0xa2, 0xb2),
        ev(202, 0x22, 0xa3, 0xb3),
        Message::EndOfStream {
            expected_end_block: 202,
        },
    ])
    .await;

    run_collect(cfg.clone(), rx, &dataset_dir)
        .await
        .expect("collect first");

    let out = run_offline_generate(cfg, &dataset_dir, 99)
        .await
        .expect("offline generate");

    assert_eq!(out.expected, 4);
    assert_eq!(out.stress, 4);
    assert_eq!(out.adversarial, 4);
    assert!(dataset_dir.join("trace_expected.jsonl").exists());
    let manifest = read_dataset_manifest(&dataset_dir).expect("manifest");
    assert_eq!(manifest.seed, Some(99));
}
