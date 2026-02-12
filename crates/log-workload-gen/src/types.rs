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
