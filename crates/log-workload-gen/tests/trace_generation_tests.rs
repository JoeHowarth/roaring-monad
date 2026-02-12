use log_workload_gen::artifact::ParquetStats;
use log_workload_gen::config::{GeneratorConfig, MaxThreads};
use log_workload_gen::generate::generate_traces;
use log_workload_gen::stats::{KeyStatsRow, KeyType};
use log_workload_gen::types::DatasetManifest;

#[test]
fn generation_is_deterministic_for_same_seed() {
    let cfg = GeneratorConfig {
        trace_size_per_profile: 32,
        scale_factor: 1.0,
        ..GeneratorConfig::default()
    };
    let manifest = manifest();
    let stats = stats();

    let a = generate_traces(&cfg, &manifest, &stats, 42).expect("generate traces");
    let b = generate_traces(&cfg, &manifest, &stats, 42).expect("generate traces");

    assert_eq!(a, b);
}

#[test]
fn generation_changes_when_seed_changes() {
    let cfg = GeneratorConfig {
        trace_size_per_profile: 32,
        scale_factor: 1.0,
        ..GeneratorConfig::default()
    };
    let manifest = manifest();
    let stats = stats();

    let a = generate_traces(&cfg, &manifest, &stats, 1).expect("generate traces");
    let b = generate_traces(&cfg, &manifest, &stats, 2).expect("generate traces");

    assert_ne!(a.expected, b.expected);
}

#[test]
fn generation_respects_sizes_ids_and_block_bounds() {
    let cfg = GeneratorConfig {
        trace_size_per_profile: 10,
        scale_factor: 1.5,
        ..GeneratorConfig::default()
    };
    let manifest = manifest();
    let stats = stats();

    let out = generate_traces(&cfg, &manifest, &stats, 7).expect("generate traces");

    assert_eq!(out.expected.len(), 15);
    assert_eq!(out.stress.len(), 15);
    assert_eq!(out.adversarial.len(), 15);

    for profile in [&out.expected, &out.stress, &out.adversarial] {
        for (idx, entry) in profile.iter().enumerate() {
            assert_eq!(entry.id as usize, idx);
            assert!(entry.from_block >= manifest.start_block);
            assert!(entry.to_block <= manifest.end_block);
            assert!(entry.from_block <= entry.to_block);
            assert!((0.0..=1.0).contains(&entry.observed_block_coverage_ratio));
        }
    }
}

#[test]
fn generation_is_invariant_to_max_threads_setting() {
    let cfg_1 = GeneratorConfig {
        trace_size_per_profile: 32,
        scale_factor: 1.0,
        max_threads: MaxThreads::Value(1),
        ..GeneratorConfig::default()
    };
    let cfg_n = GeneratorConfig {
        trace_size_per_profile: 32,
        scale_factor: 1.0,
        max_threads: MaxThreads::NumCpus,
        ..GeneratorConfig::default()
    };
    let manifest = manifest();
    let stats = stats();

    let a = generate_traces(&cfg_1, &manifest, &stats, 42).expect("generate traces");
    let b = generate_traces(&cfg_n, &manifest, &stats, 42).expect("generate traces");

    assert_eq!(a, b);
}

fn manifest() -> DatasetManifest {
    DatasetManifest {
        schema_version: "1.0.0".to_string(),
        crate_version: "0.1.0".to_string(),
        chain_id: 1,
        start_block: 100,
        end_block: 500,
        blocks_observed: 350,
        gap_count: 1,
        missing_block_ranges: Some(vec![[250, 300]]),
        event_count: 350,
        log_count: 3_500,
        created_at: "2026-02-12T00:00:00Z".to_string(),
        config_hash: "hash".to_string(),
        seed: Some(7),
        valid: true,
        invalid_reason: None,
    }
}

fn stats() -> ParquetStats {
    ParquetStats {
        key_stats: vec![
            KeyStatsRow {
                key_type: KeyType::Address,
                key_value: vec![0xaa; 20],
                count_total: 100,
                first_block: 100,
                last_block: 500,
                active_block_count: 200,
                distinct_partner_estimate: Some(2.0),
            },
            KeyStatsRow {
                key_type: KeyType::Address,
                key_value: vec![0xbb; 20],
                count_total: 50,
                first_block: 120,
                last_block: 420,
                active_block_count: 120,
                distinct_partner_estimate: Some(1.0),
            },
            KeyStatsRow {
                key_type: KeyType::Topic0,
                key_value: vec![0x11; 32],
                count_total: 90,
                first_block: 105,
                last_block: 410,
                active_block_count: 130,
                distinct_partner_estimate: Some(2.0),
            },
            KeyStatsRow {
                key_type: KeyType::Topic0,
                key_value: vec![0x22; 32],
                count_total: 40,
                first_block: 150,
                last_block: 499,
                active_block_count: 80,
                distinct_partner_estimate: Some(1.0),
            },
        ],
        cooccurrence: vec![],
        range_stats: vec![],
    }
}
