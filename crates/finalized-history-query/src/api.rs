use crate::config::Config;
pub use crate::core::page::{QueryOrder, QueryPage, QueryPageMeta};
pub use crate::core::refs::BlockRef;
use crate::error::{Error, Result};
use crate::family::Families;
use crate::family::FinalizedBlock;
use crate::ingest::authority::{LeaseAuthority, ReadOnlyAuthority, WriteAuthority};
use crate::ingest::engine::IngestEngine;
use crate::kernel::cache::BytesCacheMetrics;
use crate::logs::filter::LogFilter;
use crate::logs::query::LogsQueryEngine;
use crate::logs::types::Log;
use crate::runtime::Runtime;
pub use crate::status::ServiceStatus;
use crate::status::service_status;
use crate::store::publication::{MetaPublicationStore, PublicationStore};
use crate::store::traits::{BlobStore, MetaStore};
use crate::traces::filter::TraceFilter;
use crate::traces::query::TracesQueryEngine;
use crate::traces::types::Trace;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryLogsRequest {
    pub from_block: Option<u64>,
    pub to_block: Option<u64>,
    pub from_block_hash: Option<[u8; 32]>,
    pub to_block_hash: Option<[u8; 32]>,
    pub order: QueryOrder,
    pub resume_log_id: Option<u64>,
    pub limit: usize,
    pub filter: LogFilter,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryTracesRequest {
    pub from_block: Option<u64>,
    pub to_block: Option<u64>,
    pub from_block_hash: Option<[u8; 32]>,
    pub to_block_hash: Option<[u8; 32]>,
    pub order: QueryOrder,
    pub resume_trace_id: Option<u64>,
    pub limit: usize,
    pub filter: TraceFilter,
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
    ingest: IngestEngine<A>,
    publication_store: MetaPublicationStore<M>,
    logs_query: LogsQueryEngine,
    traces_query: TracesQueryEngine,
    pub(crate) runtime: Runtime<M, B>,
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
        let logs_query = LogsQueryEngine::from_config(&config);
        let traces_query = TracesQueryEngine::from_config(&config);
        let runtime = Runtime::new(meta_store, blob_store, config.bytes_cache);
        let publication_store = MetaPublicationStore::new(runtime.meta_store.clone());
        let ingest = IngestEngine::new(config, authority, Families::default());
        Self {
            ingest,
            publication_store,
            logs_query,
            traces_query,
            runtime,
            allows_writes,
        }
    }

    pub fn cache_metrics(&self) -> BytesCacheMetrics {
        self.runtime.tables.metrics_snapshot()
    }

    pub fn meta_store(&self) -> &M {
        &self.runtime.meta_store
    }

    pub fn blob_store(&self) -> &B {
        &self.runtime.blob_store
    }

    pub async fn query_logs(
        &self,
        request: QueryLogsRequest,
        budget: ExecutionBudget,
    ) -> Result<crate::core::page::QueryPage<Log>> {
        self.logs_query
            .query_logs(
                &self.runtime.tables,
                &self.publication_store,
                request,
                budget,
            )
            .await
    }

    pub async fn query_traces(
        &self,
        request: QueryTracesRequest,
        budget: ExecutionBudget,
    ) -> Result<crate::core::page::QueryPage<Trace>> {
        self.traces_query
            .query_traces(
                &self.runtime.tables,
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
        self.ingest_blocks(blocks).await
    }

    pub async fn indexed_finalized_head(&self) -> Result<u64> {
        self.publication_store
            .load_finalized_head_state()
            .await
            .map(|state| state.indexed_finalized_head)
    }

    pub async fn status(&self) -> Result<ServiceStatus> {
        service_status(
            &self.runtime,
            &self.publication_store,
            &self.ingest.families,
        )
        .await
    }

    async fn ingest_blocks(&self, blocks: Vec<FinalizedBlock>) -> Result<IngestOutcome> {
        if !self.allows_writes {
            return Err(reader_only_mode_error());
        }

        self.ingest
            .ingest_finalized_blocks(&self.runtime, &blocks)
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
