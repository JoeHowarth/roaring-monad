use crate::artifact::ParquetStats;
use crate::config::{BlockRangeMax, GeneratorConfig, ProfileConfig, QueryTemplate};
use crate::error::Error;
use crate::stats::KeyType;
use crate::types::{DatasetManifest, SelectivityBucket, TraceEntry, TraceProfile};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha20Rng;
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, PartialEq)]
pub struct GeneratedTraces {
    pub expected: Vec<TraceEntry>,
    pub stress: Vec<TraceEntry>,
    pub adversarial: Vec<TraceEntry>,
}

pub fn generate_traces(
    config: &GeneratorConfig,
    manifest: &DatasetManifest,
    stats: &ParquetStats,
    seed: u64,
) -> Result<GeneratedTraces, Error> {
    config.validate()?;

    let address_pool = extract_pool(stats, KeyType::Address);
    let topic0_pool = extract_pool(stats, KeyType::Topic0);
    if address_pool.is_empty() || topic0_pool.is_empty() {
        return Err(Error::InputInvalid(
            "trace generation requires address and topic0 key stats".to_string(),
        ));
    }

    let size = ((config.trace_size_per_profile as f64) * config.scale_factor).round() as u64;

    Ok(GeneratedTraces {
        expected: generate_profile(
            TraceProfile::Expected,
            &config.profiles.expected,
            manifest,
            &address_pool,
            &topic0_pool,
            derive_seed(seed, "expected"),
            size,
        )?,
        stress: generate_profile(
            TraceProfile::Stress,
            &config.profiles.stress,
            manifest,
            &address_pool,
            &topic0_pool,
            derive_seed(seed, "stress"),
            size,
        )?,
        adversarial: generate_profile(
            TraceProfile::Adversarial,
            &config.profiles.adversarial,
            manifest,
            &address_pool,
            &topic0_pool,
            derive_seed(seed, "adversarial"),
            size,
        )?,
    })
}

fn generate_profile(
    profile: TraceProfile,
    profile_cfg: &ProfileConfig,
    manifest: &DatasetManifest,
    address_pool: &[Vec<u8>],
    topic0_pool: &[Vec<u8>],
    seed: [u8; 32],
    size: u64,
) -> Result<Vec<TraceEntry>, Error> {
    let mut rng = ChaCha20Rng::from_seed(seed);
    let mut out = Vec::with_capacity(size as usize);

    for id in 0..size {
        let template = sample_template(&mut rng, profile_cfg)?;
        let (from_block, to_block) = sample_block_range(&mut rng, profile_cfg, manifest)?;

        let (address_or, topic0_or) = match template {
            QueryTemplate::SingleAddress => (sample_or(&mut rng, address_pool, 1), Vec::new()),
            QueryTemplate::SingleTopic0 => (Vec::new(), sample_or(&mut rng, topic0_pool, 1)),
            QueryTemplate::AddressTopic0 => (
                sample_or(&mut rng, address_pool, 1),
                sample_or(&mut rng, topic0_pool, 1),
            ),
            QueryTemplate::MultiAddress => {
                let width = sample_width(
                    &mut rng,
                    profile_cfg.address_or_width.min,
                    profile_cfg.address_or_width.max,
                );
                (sample_or(&mut rng, address_pool, width), Vec::new())
            }
            QueryTemplate::MultiTopic0 => {
                let width = sample_width(
                    &mut rng,
                    profile_cfg.topic0_or_width.min,
                    profile_cfg.topic0_or_width.max,
                );
                (Vec::new(), sample_or(&mut rng, topic0_pool, width))
            }
            QueryTemplate::Compound => {
                let aw = sample_width(
                    &mut rng,
                    profile_cfg.address_or_width.min,
                    profile_cfg.address_or_width.max,
                );
                let tw = sample_width(
                    &mut rng,
                    profile_cfg.topic0_or_width.min,
                    profile_cfg.topic0_or_width.max,
                );
                (
                    sample_or(&mut rng, address_pool, aw),
                    sample_or(&mut rng, topic0_pool, tw),
                )
            }
        };

        let range_span = to_block - from_block + 1;
        let estimated = (address_or.len().max(1) * topic0_or.len().max(1)) as u64 * range_span;
        let coverage = observed_coverage_ratio(from_block, to_block, manifest);

        out.push(TraceEntry {
            id,
            profile: profile.clone(),
            template,
            from_block,
            to_block,
            address_or: address_or.into_iter().map(hex::encode).collect(),
            topic0_or: topic0_or.into_iter().map(hex::encode).collect(),
            topic1_or: Vec::new(),
            topic2_or: Vec::new(),
            topic3_or: Vec::new(),
            expected_selectivity_bucket: selectivity_bucket(estimated),
            observed_block_coverage_ratio: coverage,
            notes: None,
        });
    }

    Ok(out)
}

fn sample_template(
    rng: &mut ChaCha20Rng,
    profile_cfg: &ProfileConfig,
) -> Result<QueryTemplate, Error> {
    let mut roll = rng.random::<f64>();
    for (template, weight) in &profile_cfg.template_mix {
        roll -= *weight;
        if roll <= 0.0 {
            return Ok(template.clone());
        }
    }
    profile_cfg
        .template_mix
        .keys()
        .next_back()
        .cloned()
        .ok_or_else(|| Error::ConfigInvalid("template_mix is empty".to_string()))
}

fn sample_block_range(
    rng: &mut ChaCha20Rng,
    profile_cfg: &ProfileConfig,
    manifest: &DatasetManifest,
) -> Result<(u64, u64), Error> {
    if manifest.end_block < manifest.start_block {
        return Err(Error::InputInvalid(
            "manifest block range is invalid".to_string(),
        ));
    }

    let total = manifest.end_block - manifest.start_block + 1;
    let max_raw = match profile_cfg.block_range_blocks.max {
        BlockRangeMax::Value(v) => v,
        BlockRangeMax::FullRange(_) => total,
    };
    let min = profile_cfg.block_range_blocks.min.min(total);
    let max = max_raw.max(min).min(total);
    let span = if min == max {
        min
    } else {
        rng.random_range(min..=max)
    };
    let max_start = manifest.end_block - span + 1;
    let from = if manifest.start_block == max_start {
        manifest.start_block
    } else {
        rng.random_range(manifest.start_block..=max_start)
    };
    Ok((from, from + span - 1))
}

fn sample_width(rng: &mut ChaCha20Rng, min: u32, max: u32) -> usize {
    if min >= max {
        min as usize
    } else {
        rng.random_range(min..=max) as usize
    }
}

fn sample_or(rng: &mut ChaCha20Rng, pool: &[Vec<u8>], width: usize) -> Vec<Vec<u8>> {
    let mut out = Vec::with_capacity(width);
    for _ in 0..width.max(1) {
        let idx = rng.random_range(0..pool.len());
        out.push(pool[idx].clone());
    }
    out
}

fn extract_pool(stats: &ParquetStats, key_type: KeyType) -> Vec<Vec<u8>> {
    stats
        .key_stats
        .iter()
        .filter(|r| r.key_type == key_type)
        .map(|r| r.key_value.clone())
        .collect()
}

fn derive_seed(root_seed: u64, domain: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(root_seed.to_le_bytes());
    hasher.update(domain.as_bytes());
    hasher.finalize().into()
}

fn observed_coverage_ratio(from: u64, to: u64, manifest: &DatasetManifest) -> f64 {
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

fn selectivity_bucket(v: u64) -> SelectivityBucket {
    match v {
        0 => SelectivityBucket::Empty,
        1..=9 => SelectivityBucket::Tiny,
        10..=99 => SelectivityBucket::Small,
        100..=9_999 => SelectivityBucket::Medium,
        10_000..=999_999 => SelectivityBucket::Large,
        _ => SelectivityBucket::Huge,
    }
}
