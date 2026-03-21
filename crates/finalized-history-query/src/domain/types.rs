use serde::{Deserialize, Serialize};

use crate::store::publication::FinalizedHeadState;

pub type Hash32 = [u8; 32];
pub type Address20 = [u8; 20];
pub type Topic32 = [u8; 32];
pub type SessionId = [u8; 16];

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
pub struct DirBucket {
    pub start_block: u64,
    pub first_log_ids: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DirByBlock {
    pub block_num: u64,
    pub first_log_id: u64,
    pub end_log_id_exclusive: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlockLogHeader {
    pub offsets: Vec<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamBitmapMeta {
    pub block_num: u64,
    pub count: u32,
    pub min_local: u32,
    pub max_local: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlockRecord {
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
pub struct PublicationState {
    pub owner_id: u64,
    pub session_id: SessionId,
    pub indexed_finalized_head: u64,
    pub lease_valid_through_block: u64,
}

impl PublicationState {
    pub fn finalized_head_state(&self) -> FinalizedHeadState {
        FinalizedHeadState {
            indexed_finalized_head: self.indexed_finalized_head,
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
    pub message: String,
}
