pub mod service;

pub use crate::core::page::{QueryOrder, QueryPage, QueryPageMeta};
pub use crate::core::refs::BlockRef;
use crate::logs::filter::LogFilter;
pub use crate::startup::StartupPlan;
pub use service::FinalizedHistoryService;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryLogsRequest {
    pub from_block: u64,
    pub to_block: u64,
    pub order: QueryOrder,
    pub resume_log_id: Option<u64>,
    pub limit: usize,
    pub filter: LogFilter,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExecutionBudget {
    pub max_results: Option<usize>,
}
