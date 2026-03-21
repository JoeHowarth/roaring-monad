use crate::api::{ExecutionBudget, FinalizedHistoryWriter, FinalizedLogQueries, QueryLogsRequest};
use crate::error::Result;
use crate::ingest::authority::WriteAuthority;
use crate::logs::types::{Block, IngestOutcome, Log};
use crate::store::traits::{BlobStore, MetaStore};

use super::{FinalizedHistoryService, reader_only_mode_error};

impl<A: WriteAuthority, M: MetaStore, B: BlobStore> FinalizedHistoryService<A, M, B> {
    pub async fn query_logs(
        &self,
        request: QueryLogsRequest,
        budget: ExecutionBudget,
    ) -> Result<crate::core::page::QueryPage<Log>> {
        <Self as FinalizedLogQueries>::query_logs(self, request, budget).await
    }

    pub async fn ingest_finalized_block(&self, block: Block) -> Result<IngestOutcome> {
        <Self as FinalizedHistoryWriter>::ingest_finalized_block(self, block).await
    }

    pub async fn ingest_finalized_blocks(&self, blocks: Vec<Block>) -> Result<IngestOutcome> {
        <Self as FinalizedHistoryWriter>::ingest_finalized_blocks(self, blocks).await
    }

    async fn ingest_blocks_with_startup(&self, blocks: Vec<Block>) -> Result<IngestOutcome> {
        if !self.allows_writes {
            return Err(reader_only_mode_error());
        }

        self.ingest.ingest_finalized_blocks(&blocks).await
    }
}

impl<A, M, B> FinalizedHistoryWriter for FinalizedHistoryService<A, M, B>
where
    A: WriteAuthority,
    M: MetaStore,
    B: BlobStore,
{
    async fn ingest_finalized_block(&self, block: Block) -> Result<IngestOutcome> {
        self.ingest_finalized_blocks(vec![block]).await
    }

    async fn ingest_finalized_blocks(&self, blocks: Vec<Block>) -> Result<IngestOutcome> {
        self.ingest_blocks_with_startup(blocks).await
    }
}

impl<A, M, B> FinalizedLogQueries for FinalizedHistoryService<A, M, B>
where
    A: WriteAuthority,
    M: MetaStore,
    B: BlobStore,
{
    async fn query_logs(
        &self,
        request: QueryLogsRequest,
        budget: ExecutionBudget,
    ) -> Result<crate::core::page::QueryPage<Log>> {
        self.query
            .query_logs(self.tables(), &self.publication_store, request, budget)
            .await
    }
}
