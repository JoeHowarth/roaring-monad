use crate::core::directory::{PrimaryDirBucket, PrimaryDirFragment};
use crate::core::ids::LogId;
use crate::core::offsets::BucketedOffsets;
use crate::error::{Error, Result};
use crate::family::Hash32;
pub use crate::streams::StreamBitmapMeta;

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
    pub offsets: BucketedOffsets,
}

impl BlockLogHeader {
    pub fn log_count(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    pub fn offset(&self, local_ordinal: usize) -> Result<u64> {
        self.offsets
            .get(local_ordinal)
            .ok_or(Error::Decode("log ordinal out of bounds"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogSequencingState {
    pub next_log_id: LogId,
}
