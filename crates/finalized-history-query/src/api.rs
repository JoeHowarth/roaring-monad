use crate::blocks::{Block, BlocksQueryEngine, load_block};
use crate::config::Config;
use crate::core::header::{EvmBlockHeader, load_block_header};
pub use crate::core::page::{QueryOrder, QueryPage, QueryPageMeta};
pub use crate::core::refs::BlockRef;
use crate::error::{Error, Result};
use crate::family::Families;
use crate::family::FinalizedBlock;
use crate::ingest::authority::{LeaseAuthority, ReadOnlyAuthority, WriteAuthority};
use crate::ingest::engine::IngestEngine;
use crate::kernel::cache::BytesCacheMetrics;
use crate::logs::filter::LogFilter;
use crate::logs::log_ref::LogRef;
use crate::logs::materialize::LogMaterializer;
use crate::query::engine::{FamilyQueryTables, QueryLimits, execute_family_query};
use crate::runtime::Runtime;
pub use crate::status::ServiceStatus;
use crate::status::service_status;
use crate::store::publication::{MetaPublicationStore, PublicationStore};
use crate::store::traits::{BlobStore, MetaStore};
use crate::traces::filter::TraceFilter;
use crate::traces::materialize::TraceMaterializer;
use crate::traces::view::TraceRef;
use crate::txs::materialize::TxMaterializer;
use crate::txs::{TxFilter, TxRef};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedQueryRequest<F> {
    pub from_block: Option<u64>,
    pub to_block: Option<u64>,
    pub from_block_hash: Option<[u8; 32]>,
    pub to_block_hash: Option<[u8; 32]>,
    pub order: QueryOrder,
    pub resume_id: Option<u64>,
    pub limit: usize,
    pub filter: F,
}

pub type QueryLogsRequest = IndexedQueryRequest<LogFilter>;
pub type QueryTransactionsRequest = IndexedQueryRequest<TxFilter>;
pub type QueryTracesRequest = IndexedQueryRequest<TraceFilter>;

pub type BlockHeader = EvmBlockHeader;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxReceipt {
    pub tx_hash: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryBlocksRequest {
    pub from_block: Option<u64>,
    pub to_block: Option<u64>,
    pub from_block_hash: Option<[u8; 32]>,
    pub to_block_hash: Option<[u8; 32]>,
    pub order: QueryOrder,
    pub limit: usize,
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
    blocks_query: BlocksQueryEngine,
    planner_max_or_terms: usize,
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
        let planner_max_or_terms = config.planner_max_or_terms;
        let blocks_query = BlocksQueryEngine;
        let runtime = Runtime::new(meta_store, blob_store, config.bytes_cache);
        let publication_store = MetaPublicationStore::new(runtime.meta_store.clone());
        let ingest = IngestEngine::new(config, authority, Families::default());
        Self {
            ingest,
            publication_store,
            blocks_query,
            planner_max_or_terms,
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

    /// Resolves the finalized block window for a blocks request and returns a
    /// page of finalized block identities from shared block metadata.
    pub async fn query_blocks(
        &self,
        request: QueryBlocksRequest,
        budget: ExecutionBudget,
    ) -> Result<crate::core::page::QueryPage<BlockHeader>> {
        self.blocks_query
            .query_blocks(
                &self.runtime.tables,
                &self.publication_store,
                request,
                budget,
            )
            .await
    }

    /// Resolves the finalized block window for a logs request and executes the
    /// indexed query pipeline, returning a resumable page of matching logs.
    pub async fn query_logs(
        &self,
        request: QueryLogsRequest,
        budget: ExecutionBudget,
    ) -> Result<crate::core::page::QueryPage<LogRef>> {
        let mut materializer = LogMaterializer::new(&self.runtime.tables);
        execute_family_query(
            FamilyQueryTables {
                tables: &self.runtime.tables,
                stream_tables: &self.runtime.tables.log_streams,
            },
            &self.publication_store,
            &request,
            QueryLimits {
                budget,
                max_or_terms: self.planner_max_or_terms,
            },
            &mut materializer,
            |record| record.logs,
        )
        .await
    }

    /// Resolves the finalized block window for a transactions request and
    /// executes the indexed query pipeline, returning a resumable page of
    /// matching transactions.
    pub async fn query_transactions(
        &self,
        request: QueryTransactionsRequest,
        budget: ExecutionBudget,
    ) -> Result<crate::core::page::QueryPage<TxRef>> {
        let mut materializer = TxMaterializer::new(&self.runtime.tables);
        execute_family_query(
            FamilyQueryTables {
                tables: &self.runtime.tables,
                stream_tables: &self.runtime.tables.tx_streams,
            },
            &self.publication_store,
            &request,
            QueryLimits {
                budget,
                max_or_terms: self.planner_max_or_terms,
            },
            &mut materializer,
            |record| record.txs,
        )
        .await
    }

    /// Resolves the finalized block window for a traces request and executes
    /// the indexed query pipeline, returning a resumable page of matching traces.
    pub async fn query_traces(
        &self,
        request: QueryTracesRequest,
        budget: ExecutionBudget,
    ) -> Result<crate::core::page::QueryPage<TraceRef>> {
        let mut materializer = TraceMaterializer::new(&self.runtime.tables);
        execute_family_query(
            FamilyQueryTables {
                tables: &self.runtime.tables,
                stream_tables: &self.runtime.tables.trace_streams,
            },
            &self.publication_store,
            &request,
            QueryLimits {
                budget,
                max_or_terms: self.planner_max_or_terms,
            },
            &mut materializer,
            |record| record.traces,
        )
        .await
    }

    pub async fn get_tx(&self, tx_hash: [u8; 32]) -> Result<Option<TxRef>> {
        let Some(location) = self.runtime.tables.tx_hash_index.get(&tx_hash).await? else {
            return Ok(None);
        };
        self.runtime
            .tables
            .block_tx_blobs
            .load_tx_at(location.block_num, location.tx_idx)
            .await
    }

    pub async fn get_block(&self, number: u64) -> Result<Option<Block>> {
        load_block(&self.runtime.tables, number).await
    }

    pub async fn get_block_header(&self, number: u64) -> Result<Option<BlockHeader>> {
        load_block_header(&self.runtime.tables, number).await
    }

    pub async fn get_tx_receipt(&self, _tx_hash: [u8; 32]) -> Result<Option<TxReceipt>> {
        Err(Error::InvalidParams("get_tx_receipt is not implemented"))
    }

    pub async fn get_block_receipts(&self, _number: u64) -> Result<Option<Vec<TxReceipt>>> {
        Err(Error::InvalidParams(
            "get_block_receipts is not implemented",
        ))
    }

    pub async fn ingest_finalized_block(&self, block: FinalizedBlock) -> Result<IngestOutcome> {
        self.ingest_finalized_blocks(vec![block]).await
    }

    /// Ingests a contiguous batch of finalized blocks and advances publication
    /// only after all participating families have written their authoritative artifacts.
    pub async fn ingest_finalized_blocks(
        &self,
        blocks: Vec<FinalizedBlock>,
    ) -> Result<IngestOutcome> {
        if !self.allows_writes {
            return Err(reader_only_mode_error());
        }

        self.ingest
            .ingest_finalized_blocks(&self.runtime, &blocks)
            .await
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
}

fn reader_only_mode_error() -> Error {
    Error::ReadOnlyMode("reader-only service cannot acquire write authority")
}

impl<M, B> FinalizedHistoryService<LeaseAuthority<MetaPublicationStore<M>>, M, B>
where
    M: MetaStore + Clone,
    B: BlobStore,
{
    /// Builds a read-write service that acquires lease-based write authority
    /// over publication while sharing the same runtime for query and ingest.
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
    /// Builds a query-only service that shares the normal runtime but refuses
    /// any ingest call that would require write authority.
    pub fn new_reader_only(config: Config, meta_store: M, blob_store: B) -> Self {
        Self::with_authority(config, meta_store, blob_store, ReadOnlyAuthority, false)
    }
}
