use log_workload_gen::config::GeneratorConfig;
use sha2::{Digest, Sha256};

#[test]
fn validate_rejects_invalid_queue_capacity() {
    let cfg = GeneratorConfig {
        event_queue_capacity: 0,
        ..GeneratorConfig::default()
    };

    let err = cfg
        .validate()
        .expect_err("event_queue_capacity must be rejected");
    assert!(err.to_string().contains("event_queue_capacity"));
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
