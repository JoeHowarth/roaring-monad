use crate::artifact::ParquetStats;
use crate::config::{BlockRangeMax, ProfileConfig, QueryTemplate};
use crate::error::Error;
use crate::stats::KeyType;
use crate::types::DatasetManifest;
use rand::Rng;
use rand_chacha::ChaCha20Rng;

pub fn sample_template(
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

pub fn sample_block_range(
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
        BlockRangeMax::FullRange => total,
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

pub fn sample_width(rng: &mut ChaCha20Rng, min: u32, max: u32) -> usize {
    if min >= max {
        min as usize
    } else {
        rng.random_range(min..=max) as usize
    }
}

pub fn sample_or(rng: &mut ChaCha20Rng, pool: &[Vec<u8>], width: usize) -> Vec<Vec<u8>> {
    let mut out = Vec::with_capacity(width);
    for _ in 0..width.max(1) {
        let idx = rng.random_range(0..pool.len());
        out.push(pool[idx].clone());
    }
    out
}

pub fn extract_pool(stats: &ParquetStats, key_type: KeyType) -> Vec<Vec<u8>> {
    stats
        .key_stats
        .iter()
        .filter(|r| r.key_type == key_type)
        .map(|r| r.key_value.clone())
        .collect()
}
