use crate::core::directory::{PrimaryDirBucket, PrimaryDirFragment};
use crate::core::ids::TraceId;
use crate::core::offsets::BucketedOffsets;
use crate::error::{Error, Result};
use crate::family::Hash32;
pub use crate::streams::StreamBitmapMeta;

pub type Address20 = [u8; 20];
pub type Selector4 = [u8; 4];

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct Trace {
    pub block_num: u64,
    pub block_hash: Hash32,
    pub tx_idx: u32,
    pub trace_idx: u32,
    pub typ: u8,
    pub flags: u64,
    pub from: Address20,
    pub to: Option<Address20>,
    pub value: Vec<u8>,
    pub gas: u64,
    pub gas_used: u64,
    pub input: Vec<u8>,
    pub output: Vec<u8>,
    pub status: u8,
    pub depth: u64,
}

pub type DirBucket = PrimaryDirBucket;
pub type DirByBlock = PrimaryDirFragment;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct BlockTraceHeader {
    pub encoding_version: u32,
    pub offsets: BucketedOffsets,
    pub tx_starts: Vec<u32>,
}

impl BlockTraceHeader {
    pub fn trace_count(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    pub fn offset(&self, local_ordinal: usize) -> Result<u64> {
        self.offsets
            .get(local_ordinal)
            .ok_or(Error::Decode("trace ordinal out of bounds"))
    }

    pub fn tx_idx_for_trace(&self, local_ordinal: usize) -> Option<u32> {
        let local_ordinal = u32::try_from(local_ordinal).ok()?;
        let upper = self
            .tx_starts
            .partition_point(|start| *start <= local_ordinal);
        upper
            .checked_sub(1)
            .and_then(|index| u32::try_from(index).ok())
    }

    pub fn trace_idx_in_tx(&self, local_ordinal: usize) -> Option<u32> {
        let tx_idx = usize::try_from(self.tx_idx_for_trace(local_ordinal)?).ok()?;
        let tx_start = *self.tx_starts.get(tx_idx)?;
        let local_ordinal = u32::try_from(local_ordinal).ok()?;
        local_ordinal.checked_sub(tx_start)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraceSequencingState {
    pub next_trace_id: TraceId,
}
