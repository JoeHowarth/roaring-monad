use crate::core::ids::LogId;
use crate::domain::types::BlockRecord;

pub use crate::domain::types::PublicationState;
pub use crate::domain::types::{
    Block, BlockLogHeader, DirBucket, HealthReport, IngestOutcome, Log,
};

pub type Hash32 = [u8; 32];
pub type Address20 = [u8; 20];
pub type Topic32 = [u8; 32];

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
