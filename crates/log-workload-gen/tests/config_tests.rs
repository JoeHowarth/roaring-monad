use log_workload_gen::config::{BlockRangeMax, GeneratorConfig, MaxThreads, QueryTemplate};
use sha2::{Digest, Sha256};

#[test]
fn default_config_matches_spec_shape_basics() {
    let cfg = GeneratorConfig::default();

    assert_eq!(cfg.trace_size_per_profile, 100_000);
    assert_eq!(cfg.scale_factor, 1.0);
    assert_eq!(cfg.max_threads, MaxThreads::NumCpus("num_cpus".to_string()));
    assert_eq!(cfg.cooccurrence_top_k_per_type, 10_000);
    assert_eq!(cfg.logs_per_window_size_blocks, 1_000);
    assert_eq!(cfg.profiles.expected.address_or_width.min, 1);
    assert_eq!(cfg.profiles.adversarial.topic0_or_width.max, 128);
    assert_eq!(
        cfg.profiles.stress.block_range_blocks.max,
        BlockRangeMax::Value(500_000)
    );
    assert_eq!(
        cfg.profiles
            .expected
            .template_mix
            .get(&QueryTemplate::SingleAddress),
        Some(&0.30)
    );
}

#[test]
fn validate_rejects_invalid_constraints() {
    let cfg = GeneratorConfig {
        scale_factor: 0.0,
        ..GeneratorConfig::default()
    };

    let err = cfg.validate().expect_err("scale_factor must be rejected");
    assert!(err.to_string().contains("scale_factor"));
}

#[test]
fn config_hash_matches_rfc8785_sha256() {
    let cfg = GeneratorConfig::default();
    let got = cfg.config_hash().expect("hash must succeed");

    let json = serde_json::to_value(&cfg).expect("serialize config");
    let canonical = serde_json_canonicalizer::to_vec(&json).expect("canonical json");
    let mut hasher = Sha256::new();
    hasher.update(canonical);
    let expected = format!("{:x}", hasher.finalize());

    assert_eq!(got, expected);
}
