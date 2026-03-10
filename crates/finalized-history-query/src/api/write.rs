use async_trait::async_trait;

use crate::error::Result;
use crate::logs::types::{Block, IngestOutcome};

#[async_trait]
pub trait FinalizedHistoryWriter: Send + Sync {
    async fn ingest_finalized_block(&self, block: Block) -> Result<IngestOutcome>;
}
