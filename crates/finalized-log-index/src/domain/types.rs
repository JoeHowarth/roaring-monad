use serde::{Deserialize, Serialize};

pub type Hash32 = [u8; 32];
pub type Address20 = [u8; 20];
pub type Topic32 = [u8; 32];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Log {
    pub address: Address20,
    pub topics: Vec<Topic32>,
    pub data: Vec<u8>,
    pub block_num: u64,
    pub tx_idx: u32,
    pub log_idx: u32,
    pub block_hash: Hash32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockMeta {
    pub block_hash: Hash32,
    pub parent_hash: Hash32,
    pub first_log_id: u64,
    pub count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Block {
    pub block_num: u64,
    pub block_hash: Hash32,
    pub parent_hash: Hash32,
    pub logs: Vec<Log>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaState {
    pub indexed_finalized_head: u64,
    pub next_log_id: u64,
    pub writer_epoch: u64,
}

impl Default for MetaState {
    fn default() -> Self {
        Self {
            indexed_finalized_head: 0,
            next_log_id: 0,
            writer_epoch: 0,
        }
    }
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
