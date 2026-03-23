use crate::core::ids::TraceId;
use crate::core::offsets::BucketedOffsets;
use crate::family::Hash32;

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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Default)]
pub struct DirBucket {
    pub start_block: u64,
    pub first_trace_ids: Vec<u64>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct DirByBlock {
    pub block_num: u64,
    pub first_trace_id: u64,
    pub end_trace_id_exclusive: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct BlockTraceHeader {
    pub encoding_version: u32,
    pub offsets: BucketedOffsets,
    pub tx_starts: Vec<u32>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct StreamBitmapMeta {
    pub block_num: u64,
    pub count: u32,
    pub min_local: u32,
    pub max_local: u32,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct TraceBlockRecord {
    pub block_hash: Hash32,
    pub parent_hash: Hash32,
    pub first_trace_id: u64,
    pub count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraceSequencingState {
    pub next_trace_id: TraceId,
}

pub type TraceStartupState = TraceSequencingState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraceBlockWindow {
    pub first_trace_id: TraceId,
    pub count: u32,
}

impl From<&TraceBlockRecord> for TraceBlockWindow {
    fn from(value: &TraceBlockRecord) -> Self {
        Self {
            first_trace_id: TraceId::new(value.first_trace_id),
            count: value.count,
        }
    }
}
