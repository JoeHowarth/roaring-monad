use crate::types::{DatasetManifest, SelectivityBucket};
use sha2::{Digest, Sha256};

pub fn derive_seed(root_seed: u64, domain: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(root_seed.to_le_bytes());
    hasher.update(domain.as_bytes());
    hasher.finalize().into()
}

pub fn observed_coverage_ratio(from: u64, to: u64, manifest: &DatasetManifest) -> f64 {
    let total = (to - from + 1) as f64;
    let mut missing = 0u64;
    if let Some(ranges) = &manifest.missing_block_ranges {
        for [s, e] in ranges {
            if *e < from || *s > to {
                continue;
            }
            let ov_start = (*s).max(from);
            let ov_end = (*e).min(to);
            missing += ov_end - ov_start + 1;
        }
    }
    ((total - missing as f64) / total).clamp(0.0, 1.0)
}

pub fn selectivity_bucket(v: u64) -> SelectivityBucket {
    match v {
        0 => SelectivityBucket::Empty,
        1..=9 => SelectivityBucket::Tiny,
        10..=99 => SelectivityBucket::Small,
        100..=9_999 => SelectivityBucket::Medium,
        10_000..=999_999 => SelectivityBucket::Large,
        _ => SelectivityBucket::Huge,
    }
}
