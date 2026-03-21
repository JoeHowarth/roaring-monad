use std::sync::atomic::Ordering;

use crate::api::{ExecutionBudget, FinalizedHistoryWriter, FinalizedLogQueries, QueryLogsRequest};
use crate::error::{Error, Result};
use crate::ingest::authority::WriteAuthority;
use crate::logs::types::{Block, IngestOutcome, Log};
use crate::store::publication::PublicationStore;
use crate::store::traits::{BlobStore, MetaStore};

use super::{FinalizedHistoryService, reader_only_mode_error, should_clear_writer};

impl<A: WriteAuthority, M: MetaStore + PublicationStore, B: BlobStore>
    FinalizedHistoryService<A, M, B>
{
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

    async fn ingest_blocks_with_startup(&self, blocks: Vec<Block>) -> Result<IngestOutcome>
    where
        M: PublicationStore,
    {
        if !self.allows_writes {
            return Err(reader_only_mode_error());
        }

        let _write_guard = self.write_guard.lock().await;
        let _ = self.startup_locked().await?;

        match self.ingest.ingest_finalized_blocks(&blocks).await {
            Ok(outcome) => Ok(outcome),
            Err(error) => {
                if should_clear_writer(&error) {
                    self.ingest.authority.clear().await;
                }
                Err(error)
            }
        }
    }
}

impl<A, M, B> FinalizedHistoryWriter for FinalizedHistoryService<A, M, B>
where
    A: WriteAuthority,
    M: MetaStore + PublicationStore,
    B: BlobStore,
{
    async fn ingest_finalized_block(&self, block: Block) -> Result<IngestOutcome> {
        self.ingest_finalized_blocks(vec![block]).await
    }

    async fn ingest_finalized_blocks(&self, blocks: Vec<Block>) -> Result<IngestOutcome> {
        if self.state.degraded.load(Ordering::Relaxed) {
            return Err(Error::Degraded(self.state.reason()));
        }

        let result = self.ingest_blocks_with_startup(blocks).await;
        self.update_backend_state(&result);
        if let Err(Error::InvalidParent | Error::FinalityViolation) = &result {
            self.state
                .set_degraded("finality violation or parent mismatch".to_string());
        }
        result
    }
}

impl<A, M, B> FinalizedLogQueries for FinalizedHistoryService<A, M, B>
where
    A: WriteAuthority,
    M: MetaStore + PublicationStore,
    B: BlobStore,
{
    async fn query_logs(
        &self,
        request: QueryLogsRequest,
        budget: ExecutionBudget,
    ) -> Result<crate::core::page::QueryPage<Log>> {
        if self.state.degraded.load(Ordering::Relaxed) {
            return Err(Error::Degraded(self.state.reason()));
        }

        let result = self.query.query_logs(self.tables(), request, budget).await;
        self.update_backend_state(&result);
        result
    }
}
