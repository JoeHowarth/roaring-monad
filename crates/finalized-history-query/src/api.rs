use std::sync::Arc;

use crate::config::Config;
pub use crate::core::page::{QueryOrder, QueryPage, QueryPageMeta};
pub use crate::core::refs::BlockRef;
use crate::error::{Error, Result};
use crate::family::StartupFamily;
use crate::ingest::authority::{LeaseAuthority, ReadOnlyAuthority, WriteAuthority, WriteSession};
use crate::ingest::engine::IngestEngine;
use crate::logs::family::LogsFamily;
use crate::logs::filter::LogFilter;
use crate::logs::query::LogsQueryEngine;
use crate::logs::types::{Block, HealthReport, IngestOutcome, Log};
pub use crate::startup::StartupPlan;
use crate::startup::startup_plan;
use crate::store::publication::{MetaPublicationStore, PublicationStore};
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::{BytesCacheMetrics, Tables};

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

pub struct FinalizedHistoryService<A: WriteAuthority, M: MetaStore, B: BlobStore> {
    pub ingest: IngestEngine<A, Arc<M>, Arc<B>, LogsFamily>,
    publication_store: MetaPublicationStore<M>,
    query: LogsQueryEngine,
    family: LogsFamily,
    tables: Tables<M, B>,
    allows_writes: bool,
}

impl<A: WriteAuthority, M: MetaStore, B: BlobStore> FinalizedHistoryService<A, M, B> {
    pub(crate) fn with_authority(
        config: Config,
        meta_store: M,
        blob_store: B,
        authority: A,
        allows_writes: bool,
    ) -> Self {
        let query = LogsQueryEngine::from_config(&config);
        let family = LogsFamily;
        let meta_store = Arc::new(meta_store);
        let blob_store = Arc::new(blob_store);
        let publication_store = MetaPublicationStore::new(Arc::clone(&meta_store));
        let bytes_cache = config.bytes_cache;
        let tables = Tables::new(
            Arc::clone(&meta_store),
            Arc::clone(&blob_store),
            bytes_cache,
        );
        let ingest = IngestEngine::new(config, authority, meta_store, blob_store, family);
        Self {
            ingest,
            publication_store,
            query,
            family,
            tables,
            allows_writes,
        }
    }

    pub async fn health(&self) -> HealthReport {
        HealthReport {
            healthy: true,
            message: "ok".to_string(),
        }
    }

    pub fn cache_metrics(&self) -> BytesCacheMetrics {
        self.tables.metrics_snapshot()
    }

    pub(crate) fn tables(&self) -> &Tables<M, B> {
        &self.tables
    }

    pub async fn query_logs(
        &self,
        request: QueryLogsRequest,
        budget: ExecutionBudget,
    ) -> Result<crate::core::page::QueryPage<Log>> {
        self.query
            .query_logs(self.tables(), &self.publication_store, request, budget)
            .await
    }

    pub async fn ingest_finalized_block(&self, block: Block) -> Result<IngestOutcome> {
        self.ingest_finalized_blocks(vec![block]).await
    }

    pub async fn ingest_finalized_blocks(&self, blocks: Vec<Block>) -> Result<IngestOutcome> {
        self.ingest_blocks_with_startup(blocks).await
    }

    pub async fn startup(&self) -> Result<StartupPlan> {
        if !self.allows_writes {
            return startup_plan(self.tables(), &self.publication_store, 0).await;
        }

        self.startup_locked().await
    }

    pub async fn indexed_finalized_head(&self) -> Result<u64> {
        self.publication_store
            .load_finalized_head_state()
            .await
            .map(|state| state.indexed_finalized_head)
    }

    async fn ingest_blocks_with_startup(&self, blocks: Vec<Block>) -> Result<IngestOutcome> {
        if !self.allows_writes {
            return Err(reader_only_mode_error());
        }

        self.ingest.ingest_finalized_blocks(&blocks).await
    }

    async fn startup_locked(&self) -> Result<StartupPlan> {
        debug_assert!(self.allows_writes);
        let session = self
            .ingest
            .authority
            .begin_write(self.ingest.config.observe_upstream_finalized_block.as_ref()())
            .await?;
        self.recover_and_plan(session.state().indexed_finalized_head)
            .await
    }

    async fn recover_and_plan(&self, indexed_finalized_head: u64) -> Result<StartupPlan> {
        let log_state = self
            .family
            .load_startup_state(self.tables(), indexed_finalized_head)
            .await?;
        Ok(StartupPlan {
            head_state: crate::store::publication::FinalizedHeadState {
                indexed_finalized_head,
            },
            log_state,
            warm_streams: 0,
        })
    }
}

fn reader_only_mode_error() -> Error {
    Error::ReadOnlyMode("reader-only service cannot acquire write authority")
}

impl<M, B> FinalizedHistoryService<LeaseAuthority<MetaPublicationStore<M>>, M, B>
where
    M: MetaStore + Clone,
    B: BlobStore,
{
    pub fn new_reader_writer(config: Config, meta_store: M, blob_store: B, owner_id: u64) -> Self {
        let authority = LeaseAuthority::new(
            MetaPublicationStore::new(Arc::new(meta_store.clone())),
            owner_id,
            config.publication_lease_blocks,
            config.publication_lease_renew_threshold_blocks,
        );
        Self::with_authority(config, meta_store, blob_store, authority, true)
    }
}

impl<M, B> FinalizedHistoryService<ReadOnlyAuthority, M, B>
where
    M: MetaStore + Clone,
    B: BlobStore,
{
    pub fn new_reader_only(config: Config, meta_store: M, blob_store: B) -> Self {
        Self::with_authority(config, meta_store, blob_store, ReadOnlyAuthority, false)
    }
}
