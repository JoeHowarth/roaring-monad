use crate::core::ids::LogId;
pub use crate::primitives::Hash32;
pub use crate::store::publication::PublicationState;

pub type Address20 = [u8; 20];
pub type Topic32 = [u8; 32];

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct Log {
    pub address: Address20,
    pub topics: Vec<Topic32>,
    pub data: Vec<u8>,
    pub block_num: u64,
    pub tx_idx: u32,
    pub log_idx: u32,
    pub block_hash: Hash32,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct DirBucket {
    pub start_block: u64,
    pub first_log_ids: Vec<u64>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct DirByBlock {
    pub block_num: u64,
    pub first_log_id: u64,
    pub end_log_id_exclusive: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct BlockLogHeader {
    pub offsets: Vec<u32>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct StreamBitmapMeta {
    pub block_num: u64,
    pub count: u32,
    pub min_local: u32,
    pub max_local: u32,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct BlockRecord {
    pub block_hash: Hash32,
    pub parent_hash: Hash32,
    pub first_log_id: u64,
    pub count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogSequencingState {
    pub next_log_id: LogId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogBlockWindow {
    pub first_log_id: LogId,
    pub count: u32,
}

impl From<&BlockRecord> for LogBlockWindow {
    fn from(value: &BlockRecord) -> Self {
        Self {
            first_log_id: LogId::new(value.first_log_id),
            count: value.count,
        }
    }
}
