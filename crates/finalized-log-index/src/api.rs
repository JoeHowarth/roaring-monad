use crate::domain::filter::{LogFilter, QueryOptions};
use crate::domain::types::{Block, HealthReport, IngestOutcome, Log};
use crate::error::Result;

#[async_trait::async_trait]
pub trait FinalizedLogIndex: Send + Sync {
    async fn ingest_finalized_block(&self, block: Block) -> Result<IngestOutcome>;
    async fn query_finalized(&self, filter: LogFilter, options: QueryOptions) -> Result<Vec<Log>>;
    async fn indexed_finalized_head(&self) -> Result<u64>;
    async fn health(&self) -> HealthReport;
}
