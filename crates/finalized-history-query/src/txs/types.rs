use crate::core::directory::{PrimaryDirBucket, PrimaryDirFragment};
use crate::core::ids::TxId;
use crate::core::offsets::BucketedOffsets;
use crate::family::Hash32;
pub use crate::streams::StreamBitmapMeta;

pub type Address20 = [u8; 20];
pub type Selector4 = [u8; 4];

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct Tx {
    pub tx_idx: u32,
    pub tx_hash: Hash32,
    pub block_num: u64,
    pub block_hash: Hash32,
}

pub type DirBucket = PrimaryDirBucket;
pub type DirByBlock = PrimaryDirFragment;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct BlockTxHeader {
    pub offsets: BucketedOffsets,
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
