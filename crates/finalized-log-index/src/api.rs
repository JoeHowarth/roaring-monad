use crate::config::Config;
use crate::domain::filter::{LogFilter, QueryOptions};
use crate::domain::types::{Block, HealthReport, IngestOutcome, Log};
use crate::error::Result;
use crate::ingest::engine::IngestEngine;
use crate::query::engine::QueryEngine;
use crate::store::traits::{BlobStore, MetaStore};

#[async_trait::async_trait]
pub trait FinalizedLogIndex: Send + Sync {
    async fn ingest_finalized_block(&self, block: Block) -> Result<IngestOutcome>;
    async fn query_finalized(&self, filter: LogFilter, options: QueryOptions) -> Result<Vec<Log>>;
    async fn indexed_finalized_head(&self) -> Result<u64>;
    async fn health(&self) -> HealthReport;
}

pub struct FinalizedIndexService<M: MetaStore, B: BlobStore> {
    pub ingest: IngestEngine<M, B>,
    pub query: QueryEngine,
    pub writer_epoch: u64,
}

impl<M: MetaStore, B: BlobStore> FinalizedIndexService<M, B> {
    pub fn new(config: Config, meta_store: M, blob_store: B, writer_epoch: u64) -> Self {
        let query = QueryEngine::from_config(&config);
        let ingest = IngestEngine::new(config, meta_store, blob_store);
        Self {
            ingest,
            query,
            writer_epoch,
        }
    }
}

#[async_trait::async_trait]
impl<M: MetaStore, B: BlobStore> FinalizedLogIndex for FinalizedIndexService<M, B> {
    async fn ingest_finalized_block(&self, block: Block) -> Result<IngestOutcome> {
        self.ingest
            .ingest_finalized_block(&block, self.writer_epoch)
            .await
    }

    async fn query_finalized(&self, filter: LogFilter, options: QueryOptions) -> Result<Vec<Log>> {
        self.query
            .query_finalized(
                &self.ingest.meta_store,
                &self.ingest.blob_store,
                filter,
                options,
            )
            .await
    }

    async fn indexed_finalized_head(&self) -> Result<u64> {
        use crate::codec::log::decode_meta_state;
        use crate::domain::keys::META_STATE_KEY;

        let state = self.ingest.meta_store.get(META_STATE_KEY).await?;
        match state {
            Some(r) => Ok(decode_meta_state(&r.value)?.indexed_finalized_head),
            None => Ok(0),
        }
    }

    async fn health(&self) -> HealthReport {
        HealthReport {
            healthy: true,
            degraded: false,
            message: "ok".to_string(),
        }
    }
}
