use crate::core::range::resolve_block_window;
use crate::core::state::BlockRecord;
use crate::error::{MonadChainDataError, Result};
use crate::family::{FinalizedBlock, validate_block_continuity};
use crate::kernel::storage;
use crate::logs::{QueryLogsRequest, QueryLogsResponse, plan_log_ingest};
use crate::query::runner::execute_unfiltered_block_query;
use crate::store::{BlobStore, MetaStore};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApiSurface;

pub struct MonadChainDataService<M: MetaStore, B: BlobStore> {
    meta_store: M,
    blob_store: B,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestOutcome {
    pub indexed_finalized_head: u64,
    pub block_record: BlockRecord,
    pub written_logs: usize,
}

impl<M: MetaStore, B: BlobStore> MonadChainDataService<M, B> {
    pub fn new(meta_store: M, blob_store: B) -> Self {
        Self {
            meta_store,
            blob_store,
        }
    }

    pub fn meta_store(&self) -> &M {
        &self.meta_store
    }

    pub fn blob_store(&self) -> &B {
        &self.blob_store
    }

    pub async fn ingest_block(&self, block: FinalizedBlock) -> Result<IngestOutcome> {
        let current_head = storage::load_published_head(&self.meta_store).await?;
        validate_block_continuity(&block, current_head, |block_number| async move {
            storage::load_block_record(&self.meta_store, block_number).await
        })
        .await?;

        let plan = plan_log_ingest(&block)?;
        storage::store_log_block_blob(
            &self.blob_store,
            block.block_number,
            plan.block_log_blob.clone(),
        )
        .await?;
        storage::store_log_block_header(
            &self.meta_store,
            block.block_number,
            &plan.block_log_header,
        )
        .await?;
        storage::store_block_record(&self.meta_store, block.block_number, &plan.block_record)
            .await?;
        storage::store_publication_state(
            &self.meta_store,
            crate::core::state::PublicationState {
                indexed_finalized_head: block.block_number,
            },
        )
        .await?;

        Ok(IngestOutcome {
            indexed_finalized_head: block.block_number,
            block_record: plan.block_record,
            written_logs: plan.written_logs,
        })
    }

    pub async fn query_logs(&self, request: QueryLogsRequest) -> Result<QueryLogsResponse> {
        if request.limit == 0 {
            return Err(MonadChainDataError::InvalidRequest(
                "limit must be at least 1",
            ));
        }

        let head = storage::load_published_head(&self.meta_store)
            .await?
            .ok_or(MonadChainDataError::MissingData("no published blocks"))?;
        let window = resolve_block_window(&request, head, |block_number| async move {
            storage::load_block_record(&self.meta_store, block_number).await
        })
        .await?;

        // First pass: all log queries use the fallback block-scan path.
        // Indexed execution lands in later commits once log IDs and bitmap artifacts exist.
        execute_unfiltered_block_query(&self.meta_store, &self.blob_store, &request, window).await
    }
}
