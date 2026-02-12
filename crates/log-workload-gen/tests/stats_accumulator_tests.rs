use log_workload_gen::stats::{
    CooccurrenceAccumulator, KeyStatsAccumulator, KeyType, PairType, RangeMetric,
    RangeStatsAccumulator,
};
use log_workload_gen::types::LogEntry;

fn mk_log(address_byte: u8, topics: Vec<u8>) -> LogEntry {
    LogEntry {
        tx_index: 0,
        log_index: 0,
        address: [address_byte; 20],
        topics: topics.into_iter().map(|b| [b; 32]).collect(),
    }
}

#[test]
fn key_stats_tracks_counts_ranges_and_partner_estimate() {
    let mut acc = KeyStatsAccumulator::new();

    let l1 = mk_log(0xa1, vec![0xb1, 0xc1]);
    let l2 = mk_log(0xa1, vec![0xb1, 0xc2]);
    let l3 = mk_log(0xb2, vec![0xb1]);
    let l4 = mk_log(0xa1, vec![0xb2]);

    acc.observe_log(1, &l1);
    acc.observe_log(1, &l2);
    acc.observe_log(2, &l3);
    acc.observe_log(2, &l4);

    let rows = acc.finalize();

    let addr_a = rows
        .iter()
        .find(|r| r.key_type == KeyType::Address && r.key_value == vec![0xa1; 20])
        .expect("address row");
    assert_eq!(addr_a.count_total, 3);
    assert_eq!(addr_a.first_block, 1);
    assert_eq!(addr_a.last_block, 2);
    assert_eq!(addr_a.active_block_count, 2);
    assert_eq!(addr_a.distinct_partner_estimate, Some(2.0));

    let topic0_b1 = rows
        .iter()
        .find(|r| r.key_type == KeyType::Topic0 && r.key_value == vec![0xb1; 32])
        .expect("topic0 row");
    assert_eq!(topic0_b1.count_total, 3);
    assert_eq!(topic0_b1.active_block_count, 2);
    assert_eq!(topic0_b1.distinct_partner_estimate, Some(2.0));

    let addr_topic0 = rows
        .iter()
        .find(|r| {
            r.key_type == KeyType::AddressTopic0
                && r.key_value == [vec![0xa1; 20], vec![0xb1; 32]].concat()
        })
        .expect("address_topic0 row");
    assert_eq!(addr_topic0.count_total, 2);
    assert_eq!(addr_topic0.distinct_partner_estimate, None);
}

#[test]
fn cooccurrence_tracks_pairs_and_applies_top_k_per_type() {
    let mut acc = CooccurrenceAccumulator::new(1);

    acc.observe_log(1, &mk_log(0xa1, vec![0xb1, 0xc1]));
    acc.observe_log(1, &mk_log(0xa1, vec![0xb1, 0xc2]));
    acc.observe_log(2, &mk_log(0xb2, vec![0xb1, 0xc1]));

    let rows = acc.finalize();

    let at0 = rows
        .iter()
        .find(|r| {
            r.pair_type == PairType::AddressTopic0
                && r.left_key == vec![0xa1; 20]
                && r.right_key == vec![0xb1; 32]
        })
        .expect("top address_topic0");
    assert_eq!(at0.count_total, 2);

    let t01 = rows
        .iter()
        .find(|r| {
            r.pair_type == PairType::Topic0Topic1
                && r.left_key == vec![0xb1; 32]
                && r.right_key == vec![0xc1; 32]
        })
        .expect("top topic0_topic1");
    assert_eq!(t01.count_total, 2);

    assert_eq!(rows.len(), 2);
}

#[test]
fn range_stats_histograms_and_windows_are_correct() {
    let mut acc = RangeStatsAccumulator::new(2);

    acc.observe_block(1, 0, 100);
    acc.observe_block(2, 1, 110);
    acc.observe_block(3, 3, 150);

    let rows = acc.finalize(1, 3);

    let logs_per_block_zero = rows
        .iter()
        .find(|r| {
            r.metric == RangeMetric::LogsPerBlock && r.bucket_lower == 0 && r.bucket_upper == 1
        })
        .expect("logs_per_block zero bucket");
    assert_eq!(logs_per_block_zero.count, 1);

    let logs_per_window_2_4 = rows
        .iter()
        .find(|r| {
            r.metric == RangeMetric::LogsPerWindow
                && r.bucket_lower == 2
                && r.bucket_upper == 4
                && r.window_size_blocks == Some(2)
        })
        .expect("logs_per_window 2..4 bucket");
    assert_eq!(logs_per_window_2_4.count, 1);

    let interarrival_32_64 = rows
        .iter()
        .find(|r| {
            r.metric == RangeMetric::InterarrivalSeconds
                && r.bucket_lower == 32
                && r.bucket_upper == 64
        })
        .expect("interarrival 32..64 bucket");
    assert_eq!(interarrival_32_64.count, 1);
}
