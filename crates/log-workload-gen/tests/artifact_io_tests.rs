use log_workload_gen::artifact::{
    read_dataset_manifest, read_parquet_stats, write_dataset_artifacts,
};
use log_workload_gen::stats::{
    CooccurrenceRow, KeyStatsRow, KeyType, PairType, RangeMetric, RangeStatsRow,
};
use log_workload_gen::types::DatasetManifest;
use tempfile::tempdir;

#[test]
fn writes_and_reads_dataset_manifest_json() {
    let temp = tempdir().expect("tempdir");
    let dataset_dir = temp.path().join("dataset");

    write_dataset_artifacts(
        &dataset_dir,
        &manifest(),
        &sample_key_stats(),
        &sample_cooccurrence(),
        &sample_range_stats(),
    )
    .expect("write dataset artifacts");

    let got = read_dataset_manifest(&dataset_dir).expect("read manifest");
    assert_eq!(got.schema_version, "1.0.0");
    assert_eq!(got.chain_id, 1);
    assert_eq!(got.start_block, 100);
    assert_eq!(got.end_block, 200);
    assert!(got.valid);
}

#[test]
fn parquet_roundtrip_preserves_row_counts() {
    let temp = tempdir().expect("tempdir");
    let dataset_dir = temp.path().join("dataset");

    write_dataset_artifacts(
        &dataset_dir,
        &manifest(),
        &sample_key_stats(),
        &sample_cooccurrence(),
        &sample_range_stats(),
    )
    .expect("write dataset artifacts");

    let stats = read_parquet_stats(&dataset_dir).expect("read parquet stats");
    assert_eq!(stats.key_stats.len(), 2);
    assert_eq!(stats.cooccurrence.len(), 2);
    assert_eq!(stats.range_stats.len(), 3);
}

fn manifest() -> DatasetManifest {
    DatasetManifest {
        schema_version: "1.0.0".to_string(),
        crate_version: "0.1.0".to_string(),
        chain_id: 1,
        start_block: 100,
        end_block: 200,
        blocks_observed: 90,
        gap_count: 2,
        missing_block_ranges: Some(vec![[120, 130], [150, 151]]),
        event_count: 90,
        log_count: 900,
        created_at: "2026-02-12T00:00:00Z".to_string(),
        config_hash: "abcd".to_string(),
        seed: Some(7),
        valid: true,
        invalid_reason: None,
    }
}

fn sample_key_stats() -> Vec<KeyStatsRow> {
    vec![
        KeyStatsRow {
            key_type: KeyType::Address,
            key_value: vec![0xaa; 20],
            count_total: 10,
            first_block: 100,
            last_block: 150,
            active_block_count: 20,
            distinct_partner_estimate: Some(3.0),
        },
        KeyStatsRow {
            key_type: KeyType::Topic0,
            key_value: vec![0xbb; 32],
            count_total: 8,
            first_block: 110,
            last_block: 190,
            active_block_count: 12,
            distinct_partner_estimate: Some(2.0),
        },
    ]
}

fn sample_cooccurrence() -> Vec<CooccurrenceRow> {
    vec![
        CooccurrenceRow {
            pair_type: PairType::AddressTopic0,
            left_key: vec![0xaa; 20],
            right_key: vec![0xbb; 32],
            count_total: 5,
            first_block: 120,
            last_block: 180,
        },
        CooccurrenceRow {
            pair_type: PairType::Topic0Topic1,
            left_key: vec![0xbb; 32],
            right_key: vec![0xcc; 32],
            count_total: 4,
            first_block: 121,
            last_block: 181,
        },
    ]
}

fn sample_range_stats() -> Vec<RangeStatsRow> {
    vec![
        RangeStatsRow {
            metric: RangeMetric::LogsPerBlock,
            bucket_lower: 0,
            bucket_upper: 1,
            count: 5,
            window_size_blocks: None,
        },
        RangeStatsRow {
            metric: RangeMetric::LogsPerWindow,
            bucket_lower: 8,
            bucket_upper: 16,
            count: 3,
            window_size_blocks: Some(1000),
        },
        RangeStatsRow {
            metric: RangeMetric::InterarrivalSeconds,
            bucket_lower: 32,
            bucket_upper: 64,
            count: 2,
            window_size_blocks: None,
        },
    ]
}
