use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Message {
    ChainEvent(ChainEvent),
    EndOfStream { expected_end_block: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetSummary {
    pub valid: bool,
    pub invalid_reason: Option<String>,
    pub start_block: Option<u64>,
    pub end_block: Option<u64>,
    pub blocks_observed: u64,
    pub gap_count: u64,
    pub missing_block_ranges: Option<Vec<[u64; 2]>>,
    pub event_count: u64,
    pub log_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetManifest {
    pub schema_version: String,
    pub crate_version: String,
    pub chain_id: u64,
    pub start_block: u64,
    pub end_block: u64,
    pub blocks_observed: u64,
    pub gap_count: u64,
    pub missing_block_ranges: Option<Vec<[u64; 2]>>,
    pub event_count: u64,
    pub log_count: u64,
    pub created_at: String,
    pub config_hash: String,
    pub seed: Option<u64>,
    pub valid: bool,
    pub invalid_reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TraceProfile {
    Expected,
    Stress,
    Adversarial,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SelectivityBucket {
    Empty,
    Tiny,
    Small,
    Medium,
    Large,
    Huge,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TraceEntry {
    pub id: u64,
    pub profile: TraceProfile,
    pub template: crate::config::QueryTemplate,
    pub from_block: u64,
    pub to_block: u64,
    pub address_or: Vec<String>,
    pub topic0_or: Vec<String>,
    pub topic1_or: Vec<String>,
    pub topic2_or: Vec<String>,
    pub topic3_or: Vec<String>,
    pub expected_selectivity_bucket: SelectivityBucket,
    pub observed_block_coverage_ratio: f64,
    pub notes: Option<Vec<String>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceSummary {
    pub expected: u64,
    pub stress: u64,
    pub adversarial: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RunSummary {
    pub blocks_seen: u64,
    pub logs_seen: u64,
    pub artifact_write_seconds: f64,
    pub trace_generate_seconds: f64,
    pub trace_queries_generated: TraceSummary,
    pub max_threads_used: u32,
    pub max_queue_depth: u64,
    pub dataset_valid: bool,
    pub invalid_reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChainEvent {
    pub chain_id: u64,
    pub block_number: u64,
    pub block_hash: [u8; 32],
    pub timestamp: u64,
    pub logs: Vec<LogEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    pub tx_index: u32,
    pub log_index: u32,
    pub address: [u8; 20],
    pub topics: Vec<[u8; 32]>,
}

impl DatasetSummary {
    pub fn invalid(reason: impl Into<String>) -> Self {
        Self {
            valid: false,
            invalid_reason: Some(reason.into()),
            start_block: None,
            end_block: None,
            blocks_observed: 0,
            gap_count: 0,
            missing_block_ranges: None,
            event_count: 0,
            log_count: 0,
        }
    }
}
