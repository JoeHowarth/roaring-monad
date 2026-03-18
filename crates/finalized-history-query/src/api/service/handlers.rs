use std::sync::atomic::Ordering;

use crate::api::query_logs::{ExecutionBudget, FinalizedLogQueries, QueryLogsRequest};
use crate::api::write::FinalizedHistoryWriter;
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
        if !self.role.allows_writes() {
            return Err(reader_only_mode_error());
        }

        let mut writer = self.writer.lock().await;
        if writer.is_none() {
            let _ = self.startup_locked(&mut writer).await?;
        }

        let token = (*writer).ok_or(Error::PublicationConflict)?;
        match self.ingest.ingest_finalized_blocks(&blocks, token).await {
            Ok((outcome, next_token)) => {
                *writer = Some(next_token);
                Ok(outcome)
            }
            Err(error) => {
                if should_clear_writer(&error) {
                    *writer = None;
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
        if self.state.throttled.load(Ordering::Relaxed) {
            return Err(Error::Throttled(self.state.reason()));
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

        let result = self
            .query
            .query_logs_with_cache(
                &self.ingest.meta_store,
                &self.ingest.blob_store,
                &self.cache,
                request,
                budget,
            )
            .await;
        self.update_backend_state(&result);
        result
    }
}
