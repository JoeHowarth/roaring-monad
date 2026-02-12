use log_workload_gen::ingest::consume_messages;
use log_workload_gen::types::{ChainEvent, LogEntry, Message};

fn ev(block_number: u64, block_hash_byte: u8) -> Message {
    Message::ChainEvent(ChainEvent {
        chain_id: 1,
        block_number,
        block_hash: [block_hash_byte; 32],
        timestamp: 1_700_000_000 + block_number,
        logs: vec![LogEntry {
            tx_index: 0,
            log_index: 0,
            address: [0x11; 20],
            topics: vec![[0x22; 32]],
        }],
    })
}

#[test]
fn accepts_monotonic_stream_with_eos() {
    let summary = consume_messages(vec![
        ev(10, 0x10),
        ev(11, 0x11),
        Message::EndOfStream {
            expected_end_block: 11,
        },
    ]);

    assert!(summary.valid);
    assert_eq!(summary.invalid_reason, None);
    assert_eq!(summary.start_block, Some(10));
    assert_eq!(summary.end_block, Some(11));
    assert_eq!(summary.blocks_observed, 2);
    assert_eq!(summary.gap_count, 0);
    assert_eq!(summary.missing_block_ranges, None);
}

#[test]
fn rejects_non_monotonic_block_number() {
    let summary = consume_messages(vec![
        ev(10, 0x10),
        ev(9, 0x09),
        Message::EndOfStream {
            expected_end_block: 10,
        },
    ]);

    assert!(!summary.valid);
    assert!(
        summary
            .invalid_reason
            .expect("invalid reason")
            .contains("non_monotonic")
    );
}

#[test]
fn rejects_hash_mismatch_on_same_block() {
    let summary = consume_messages(vec![
        ev(10, 0x10),
        ev(10, 0x99),
        Message::EndOfStream {
            expected_end_block: 10,
        },
    ]);

    assert!(!summary.valid);
    assert!(
        summary
            .invalid_reason
            .expect("invalid reason")
            .contains("block_hash_mismatch")
    );
}

#[test]
fn ignores_exact_duplicate_block_event() {
    let summary = consume_messages(vec![
        ev(10, 0x10),
        ev(10, 0x10),
        Message::EndOfStream {
            expected_end_block: 10,
        },
    ]);

    assert!(summary.valid);
    assert_eq!(summary.blocks_observed, 1);
}

#[test]
fn computes_gap_ranges() {
    let summary = consume_messages(vec![
        ev(1, 0x01),
        ev(2, 0x02),
        ev(5, 0x05),
        ev(6, 0x06),
        ev(10, 0x0a),
        Message::EndOfStream {
            expected_end_block: 10,
        },
    ]);

    assert!(summary.valid);
    assert_eq!(summary.gap_count, 2);
    assert_eq!(summary.missing_block_ranges, Some(vec![[3, 4], [7, 9]]));
}

#[test]
fn rejects_end_of_stream_mismatch() {
    let summary = consume_messages(vec![
        ev(10, 0x10),
        ev(11, 0x11),
        Message::EndOfStream {
            expected_end_block: 12,
        },
    ]);

    assert!(!summary.valid);
    assert!(
        summary
            .invalid_reason
            .expect("invalid reason")
            .contains("end_of_stream_mismatch")
    );
}

#[test]
fn marks_invalid_if_channel_closes_without_eos() {
    let summary = consume_messages(vec![ev(10, 0x10), ev(11, 0x11)]);

    assert!(!summary.valid);
    assert_eq!(
        summary.invalid_reason,
        Some("channel_closed_unexpectedly".to_string())
    );
}
