use crate::domain::types::{BlockMeta, MetaState};

pub use crate::domain::types::{
    Block, BlockLogHeader, HealthReport, IngestOutcome, Log, LogDirectoryBucket,
};

pub type Hash32 = [u8; 32];
pub type Address20 = [u8; 20];
pub type Topic32 = [u8; 32];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogSequencingState {
    pub next_log_id: u64,
}

impl From<&MetaState> for LogSequencingState {
    fn from(value: &MetaState) -> Self {
        Self {
            next_log_id: value.next_log_id,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogBlockWindow {
    pub first_log_id: u64,
    pub count: u32,
}

impl From<&BlockMeta> for LogBlockWindow {
    fn from(value: &BlockMeta) -> Self {
        Self {
            first_log_id: value.first_log_id,
            count: value.count,
        }
    }
}
