use crate::block::FinalizedBlock;
use crate::config::Config;
pub use crate::core::page::{QueryOrder, QueryPage, QueryPageMeta};
pub use crate::core::refs::BlockRef;
use crate::error::{Error, Result};
use crate::family::Families;
use crate::ingest::authority::{LeaseAuthority, ReadOnlyAuthority, WriteAuthority, WriteSession};
use crate::ingest::engine::IngestEngine;
use crate::logs::filter::LogFilter;
use crate::logs::query::LogsQueryEngine;
use crate::logs::types::Log;
use crate::runtime::Runtime;
pub use crate::startup::StartupPlan;
use crate::startup::{startup_plan, startup_plan_from_head};
use crate::store::publication::{MetaPublicationStore, PublicationStore};
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::BytesCacheMetrics;

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

#[derive(Debug, Clone)]
pub struct IngestOutcome {
    pub indexed_finalized_head: u64,
    pub written_logs: usize,
    pub written_txs: usize,
    pub written_traces: usize,
}

pub struct FinalizedHistoryService<A: WriteAuthority, M: MetaStore, B: BlobStore> {
    pub ingest: IngestEngine<A>,
    publication_store: MetaPublicationStore<M>,
    query: LogsQueryEngine,
    runtime: Runtime<M, B>,
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
        let runtime = Runtime::new(meta_store, blob_store, config.bytes_cache);
        let publication_store = MetaPublicationStore::new(runtime.meta_store().clone());
        let ingest = IngestEngine::new(config, authority, Families::default());
        Self {
            ingest,
            publication_store,
            query,
            runtime,
            allows_writes,
        }
    }

    pub fn cache_metrics(&self) -> BytesCacheMetrics {
        self.runtime.tables().metrics_snapshot()
    }

    pub fn meta_store(&self) -> &M {
        self.runtime.meta_store()
    }

    pub fn blob_store(&self) -> &B {
        self.runtime.blob_store()
    }

    pub(crate) fn runtime(&self) -> &Runtime<M, B> {
        &self.runtime
    }

    pub async fn query_logs(
        &self,
        request: QueryLogsRequest,
        budget: ExecutionBudget,
    ) -> Result<crate::core::page::QueryPage<Log>> {
        self.query
            .query_logs(
                self.runtime.tables(),
                &self.publication_store,
                request,
                budget,
            )
            .await
    }

    pub async fn ingest_finalized_block(&self, block: FinalizedBlock) -> Result<IngestOutcome> {
        self.ingest_finalized_blocks(vec![block]).await
    }

    pub async fn ingest_finalized_blocks(
        &self,
        blocks: Vec<FinalizedBlock>,
    ) -> Result<IngestOutcome> {
        self.ingest_blocks_with_startup(blocks).await
    }

    pub async fn startup(&self) -> Result<StartupPlan> {
        if !self.allows_writes {
            return startup_plan(
                self.runtime(),
                &self.publication_store,
                &self.ingest.families,
                0,
            )
            .await;
        }

        self.startup_locked().await
    }

    pub async fn indexed_finalized_head(&self) -> Result<u64> {
        self.publication_store
            .load_finalized_head_state()
            .await
            .map(|state| state.indexed_finalized_head)
    }

    async fn ingest_blocks_with_startup(
        &self,
        blocks: Vec<FinalizedBlock>,
    ) -> Result<IngestOutcome> {
        if !self.allows_writes {
            return Err(reader_only_mode_error());
        }

        self.ingest
            .ingest_finalized_blocks(self.runtime(), &blocks)
            .await
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
        startup_plan_from_head(
            self.runtime(),
            &self.ingest.families,
            indexed_finalized_head,
            0,
        )
        .await
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
            MetaPublicationStore::new(meta_store.clone()),
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
