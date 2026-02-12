mod planner;
mod sampler;

use crate::artifact::ParquetStats;
use crate::config::{GeneratorConfig, ProfileConfig, QueryTemplate};
use crate::error::Error;
use crate::stats::KeyType;
use crate::types::{DatasetManifest, TraceEntry, TraceProfile, TraceSummary};
use planner::{derive_seed, observed_coverage_ratio, selectivity_bucket};
use rand::SeedableRng;
use rand_chacha::ChaCha20Rng;
use sampler::{extract_pool, sample_block_range, sample_or, sample_template, sample_width};

#[derive(Clone, Debug, PartialEq)]
pub struct GeneratedTraces {
    pub expected: Vec<TraceEntry>,
    pub stress: Vec<TraceEntry>,
    pub adversarial: Vec<TraceEntry>,
}

impl GeneratedTraces {
    pub fn summary(&self) -> TraceSummary {
        TraceSummary {
            expected: self.expected.len() as u64,
            stress: self.stress.len() as u64,
            adversarial: self.adversarial.len() as u64,
        }
    }
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
