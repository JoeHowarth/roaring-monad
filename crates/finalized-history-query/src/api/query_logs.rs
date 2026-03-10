use async_trait::async_trait;

use crate::core::page::{QueryOrder, QueryPage};
use crate::error::Result;
use crate::logs::filter::LogFilter;
use crate::logs::types::Log;

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

#[async_trait]
pub trait FinalizedLogQueries: Send + Sync {
    async fn query_logs(
        &self,
        request: QueryLogsRequest,
        budget: ExecutionBudget,
    ) -> Result<QueryPage<Log>>;
}
