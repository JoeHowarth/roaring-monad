use crate::core::state::BlockRecord;
use crate::error::Result;
use crate::family::FinalizedBlock;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogIngestPlan {
    pub block_record: BlockRecord,
    pub written_logs: usize,
}

pub fn plan_log_ingest(_block: &FinalizedBlock) -> Result<LogIngestPlan> {
    todo!("implement logs-family ingest planning")
}
