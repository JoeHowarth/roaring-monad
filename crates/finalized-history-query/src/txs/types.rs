use crate::core::directory::{PrimaryDirBucket, PrimaryDirFragment};
use crate::core::ids::TxId;
use crate::core::offsets::BucketedOffsets;
use crate::error::{Error, Result};
use crate::family::Hash32;
pub use crate::streams::StreamBitmapMeta;

pub type Address20 = [u8; 20];
pub type Selector4 = [u8; 4];

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct IngestTx {
    pub tx_idx: u32,
    pub tx_hash: Hash32,
    pub sender: Address20,
    pub signed_tx_bytes: Vec<u8>,
}

pub type DirBucket = PrimaryDirBucket;
pub type DirByBlock = PrimaryDirFragment;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct BlockTxHeader {
    pub offsets: BucketedOffsets,
}

impl BlockTxHeader {
    pub fn tx_count(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    pub fn offset(&self, local_ordinal: usize) -> Result<u64> {
        self.offsets
            .get(local_ordinal)
            .ok_or(Error::Decode("tx ordinal out of bounds"))
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct StoredTxEnvelope {
    pub tx_hash: Hash32,
    pub sender: Address20,
    pub signed_tx_bytes: Vec<u8>,
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct TxLocation {
    pub block_num: u64,
    pub tx_idx: u32,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TxFamilyState {
    pub next_tx_id: TxId,
}
