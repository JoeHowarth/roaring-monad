use crate::core::directory::{PrimaryDirBucket, PrimaryDirFragment};
use crate::core::ids::LogId;
use crate::core::state::BlockRecordLike;
use crate::family::Hash32;
use crate::query::types::BlockWindow;

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

pub type DirBucket = PrimaryDirBucket;
pub type DirByBlock = PrimaryDirFragment;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct BlockLogHeader {
    pub offsets: Vec<u32>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct StreamBitmapMeta {
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

impl BlockWindow<LogId> for LogBlockWindow {
    fn first_id(self) -> LogId {
        self.first_log_id
    }

    fn count(self) -> u32 {
        self.count
    }
}

impl BlockRecordLike for BlockRecord {
    fn block_hash(&self) -> [u8; 32] {
        self.block_hash
    }

    fn parent_hash(&self) -> [u8; 32] {
        self.parent_hash
    }
}
