use crate::Error;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GeneratorConfig {
    pub trace_size_per_profile: u64,
    pub scale_factor: f64,
    pub max_threads: MaxThreads,
    pub cooccurrence_top_k_per_type: u64,
    pub logs_per_window_size_blocks: u64,
    pub profiles: ProfilesConfig,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MaxThreads {
    NumCpus(String),
    Value(u32),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProfilesConfig {
    pub expected: ProfileConfig,
    pub stress: ProfileConfig,
    pub adversarial: ProfileConfig,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProfileConfig {
    pub template_mix: BTreeMap<QueryTemplate, f64>,
    pub address_or_width: WidthRange,
    pub topic0_or_width: WidthRange,
    pub block_range_blocks: BlockRangeConfig,
    pub empty_result_target_share: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WidthRange {
    pub min: u32,
    pub max: u32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BlockRangeConfig {
    pub source: BlockRangeSource,
    pub min: u64,
    pub max: BlockRangeMax,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryTemplate {
    SingleAddress,
    SingleTopic0,
    AddressTopic0,
    MultiAddress,
    MultiTopic0,
    Compound,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockRangeSource {
    Empirical,
    EmpiricalUpperTail,
    HeavyNearFullRange,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BlockRangeMax {
    FullRange(String),
    Value(u64),
}

impl Default for GeneratorConfig {
    fn default() -> Self {
        Self {
            trace_size_per_profile: 100_000,
            scale_factor: 1.0,
            max_threads: MaxThreads::NumCpus("num_cpus".to_string()),
            cooccurrence_top_k_per_type: 10_000,
            logs_per_window_size_blocks: 1_000,
            profiles: ProfilesConfig {
                expected: profile(
                    [
                        (QueryTemplate::SingleAddress, 0.30),
                        (QueryTemplate::SingleTopic0, 0.25),
                        (QueryTemplate::AddressTopic0, 0.25),
                        (QueryTemplate::MultiAddress, 0.10),
                        (QueryTemplate::MultiTopic0, 0.08),
                        (QueryTemplate::Compound, 0.02),
                    ],
                    WidthRange { min: 1, max: 4 },
                    WidthRange { min: 1, max: 4 },
                    BlockRangeConfig {
                        source: BlockRangeSource::Empirical,
                        min: 1,
                        max: BlockRangeMax::Value(50_000),
                    },
                    0.0,
                ),
                stress: profile(
                    [
                        (QueryTemplate::SingleAddress, 0.10),
                        (QueryTemplate::SingleTopic0, 0.20),
                        (QueryTemplate::AddressTopic0, 0.20),
                        (QueryTemplate::MultiAddress, 0.20),
                        (QueryTemplate::MultiTopic0, 0.20),
                        (QueryTemplate::Compound, 0.10),
                    ],
                    WidthRange { min: 2, max: 32 },
                    WidthRange { min: 2, max: 32 },
                    BlockRangeConfig {
                        source: BlockRangeSource::EmpiricalUpperTail,
                        min: 1_000,
                        max: BlockRangeMax::Value(500_000),
                    },
                    0.0,
                ),
                adversarial: profile(
                    [
                        (QueryTemplate::SingleAddress, 0.05),
                        (QueryTemplate::SingleTopic0, 0.15),
                        (QueryTemplate::AddressTopic0, 0.15),
                        (QueryTemplate::MultiAddress, 0.25),
                        (QueryTemplate::MultiTopic0, 0.25),
                        (QueryTemplate::Compound, 0.15),
                    ],
                    WidthRange { min: 8, max: 128 },
                    WidthRange { min: 8, max: 128 },
                    BlockRangeConfig {
                        source: BlockRangeSource::HeavyNearFullRange,
                        min: 10_000,
                        max: BlockRangeMax::FullRange("full_range".to_string()),
                    },
                    0.10,
                ),
            },
        }
    }
}

impl GeneratorConfig {
    pub fn validate(&self) -> Result<(), Error> {
        if self.trace_size_per_profile < 1 {
            return Err(Error::ConfigInvalid(
                "trace_size_per_profile must be >= 1".to_string(),
            ));
        }
        if self.scale_factor <= 0.0 {
            return Err(Error::ConfigInvalid("scale_factor must be > 0".to_string()));
        }
        match &self.max_threads {
            MaxThreads::NumCpus(v) if v == "num_cpus" => {}
            MaxThreads::NumCpus(_) => {
                return Err(Error::ConfigInvalid(
                    "max_threads string form must be \"num_cpus\"".to_string(),
                ));
            }
            MaxThreads::Value(v) if *v >= 1 => {}
            MaxThreads::Value(_) => {
                return Err(Error::ConfigInvalid("max_threads must be >= 1".to_string()));
            }
        }
        if self.cooccurrence_top_k_per_type < 1 {
            return Err(Error::ConfigInvalid(
                "cooccurrence_top_k_per_type must be >= 1".to_string(),
            ));
        }
        if self.logs_per_window_size_blocks < 1 {
            return Err(Error::ConfigInvalid(
                "logs_per_window_size_blocks must be >= 1".to_string(),
            ));
        }
        validate_profile("expected", &self.profiles.expected)?;
        validate_profile("stress", &self.profiles.stress)?;
        validate_profile("adversarial", &self.profiles.adversarial)?;
        Ok(())
    }

    pub fn config_hash(&self) -> Result<String, Error> {
        let json =
            serde_json::to_value(self).map_err(|e| Error::Serialization(format!("json: {e}")))?;
        let canonical = serde_json_canonicalizer::to_vec(&json)
            .map_err(|e| Error::Serialization(format!("canonical-json: {e}")))?;
        let mut hasher = Sha256::new();
        hasher.update(canonical);
        Ok(format!("{:x}", hasher.finalize()))
    }
}

fn profile(
    mix: [(QueryTemplate, f64); 6],
    address_or_width: WidthRange,
    topic0_or_width: WidthRange,
    block_range_blocks: BlockRangeConfig,
    empty_result_target_share: f64,
) -> ProfileConfig {
    ProfileConfig {
        template_mix: BTreeMap::from(mix),
        address_or_width,
        topic0_or_width,
        block_range_blocks,
        empty_result_target_share,
    }
}

fn validate_profile(name: &str, profile: &ProfileConfig) -> Result<(), Error> {
    if profile.template_mix.values().any(|v| *v < 0.0) {
        return Err(Error::ConfigInvalid(format!(
            "profiles.{name}.template_mix weights must be >= 0"
        )));
    }
    let sum = profile.template_mix.values().sum::<f64>();
    if (sum - 1.0).abs() > 1e-9 {
        return Err(Error::ConfigInvalid(format!(
            "profiles.{name}.template_mix must sum to 1.0"
        )));
    }
    if profile.address_or_width.min < 1
        || profile.address_or_width.min > profile.address_or_width.max
    {
        return Err(Error::ConfigInvalid(format!(
            "profiles.{name}.address_or_width requires 1 <= min <= max"
        )));
    }
    if profile.topic0_or_width.min < 1 || profile.topic0_or_width.min > profile.topic0_or_width.max
    {
        return Err(Error::ConfigInvalid(format!(
            "profiles.{name}.topic0_or_width requires 1 <= min <= max"
        )));
    }
    if profile.block_range_blocks.min < 1 {
        return Err(Error::ConfigInvalid(format!(
            "profiles.{name}.block_range_blocks.min must be >= 1"
        )));
    }
    match &profile.block_range_blocks.max {
        BlockRangeMax::Value(v) => {
            if *v < profile.block_range_blocks.min {
                return Err(Error::ConfigInvalid(format!(
                    "profiles.{name}.block_range_blocks requires min <= max"
                )));
            }
        }
        BlockRangeMax::FullRange(v) => {
            if v != "full_range" {
                return Err(Error::ConfigInvalid(format!(
                    "profiles.{name}.block_range_blocks.max string form must be \"full_range\""
                )));
            }
        }
    }
    if !(0.0..=1.0).contains(&profile.empty_result_target_share) {
        return Err(Error::ConfigInvalid(format!(
            "profiles.{name}.empty_result_target_share must be in [0, 1]"
        )));
    }
    Ok(())
}
