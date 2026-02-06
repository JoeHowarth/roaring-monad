use serde::{Deserialize, Serialize};

pub type Hash32 = [u8; 32];
pub type Address20 = [u8; 20];
pub type Topic32 = [u8; 32];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Log {
    pub address: Address20,
    pub topics: Vec<Topic32>,
    pub data: Vec<u8>,
    pub block_num: u64,
    pub tx_idx: u32,
    pub log_idx: u32,
    pub block_hash: Hash32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlockMeta {
    pub block_hash: Hash32,
    pub parent_hash: Hash32,
    pub first_log_id: u64,
    pub count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Block {
    pub block_num: u64,
    pub block_hash: Hash32,
    pub parent_hash: Hash32,
    pub logs: Vec<Log>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct MetaState {
    pub indexed_finalized_head: u64,
    pub next_log_id: u64,
    pub writer_epoch: u64,
}

#[derive(Debug, Clone)]
pub struct IngestOutcome {
    pub indexed_finalized_head: u64,
    pub written_logs: usize,
}

#[derive(Debug, Clone)]
pub struct HealthReport {
    pub healthy: bool,
    pub degraded: bool,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Topic0Mode {
    pub log_enabled: bool,
    pub enabled_from_block: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Topic0Stats {
    pub window_len: u32,
    pub blocks_seen_in_window: u32,
    pub ring_cursor: u32,
    pub last_updated_block: u64,
    pub ring_bits: Vec<u8>,
}
