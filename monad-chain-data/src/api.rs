use crate::core::state::BlockRecord;
use crate::error::Result;
use crate::family::FinalizedBlock;
use crate::logs::{QueryLogsRequest, QueryLogsResponse};
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
        let _ = block;
        todo!("implement minimal logs-only ingest pipeline")
    }

    pub async fn query_logs(&self, request: QueryLogsRequest) -> Result<QueryLogsResponse> {
        let _ = request;
        todo!("implement minimal block-scan query path")
    }
}
