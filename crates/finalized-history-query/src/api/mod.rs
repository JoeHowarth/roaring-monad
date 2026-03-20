pub mod service;

pub use crate::core::page::{QueryOrder, QueryPage, QueryPageMeta};
pub use crate::core::refs::BlockRef;
use crate::error::Result;
use crate::logs::filter::LogFilter;
use crate::logs::types::{Block, IngestOutcome, Log};
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

#[allow(async_fn_in_trait)]
pub trait FinalizedLogQueries: Send + Sync {
    async fn query_logs(
        &self,
        request: QueryLogsRequest,
        budget: ExecutionBudget,
    ) -> Result<QueryPage<Log>>;
}

#[allow(async_fn_in_trait)]
pub trait FinalizedHistoryWriter: Send + Sync {
    async fn ingest_finalized_block(&self, block: Block) -> Result<IngestOutcome>;

    async fn ingest_finalized_blocks(&self, blocks: Vec<Block>) -> Result<IngestOutcome>;
}
