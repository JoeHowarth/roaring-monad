use crate::error::Result;
use crate::logs::types::{Block, IngestOutcome};

#[allow(async_fn_in_trait)]
pub trait FinalizedHistoryWriter: Send + Sync {
    async fn ingest_finalized_block(&self, block: Block) -> Result<IngestOutcome>;

    async fn ingest_finalized_blocks(&self, blocks: Vec<Block>) -> Result<IngestOutcome>;
}
