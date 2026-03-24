use super::clause::{
    ClauseKind, IndexedClauseSpec, LogsStreamFamily, build_clause_specs, prepare_shard_clauses,
};
use crate::api::{ExecutionBudget, QueryLogsRequest};
use crate::config::Config;
use crate::core::ids::{LogId, LogLocalId, LogShard, family_local_range_for_shard};
use crate::core::page::QueryPage;
use crate::error::Result;
use crate::logs::filter::LogFilter;
use crate::logs::materialize::LogMaterializer;
use crate::logs::state::resolve_log_window;
use crate::logs::types::Log;
use crate::query::engine::{IndexedQueryFamily, IndexedQueryRequest, execute_family_query};
use crate::query::planner::PreparedClause;
use crate::store::publication::PublicationStore;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;

#[derive(Debug, Clone)]
pub struct LogsQueryEngine {
    max_or_terms: usize,
}

impl LogsQueryEngine {
    pub fn from_config(config: &Config) -> Self {
        Self {
            max_or_terms: config.planner_max_or_terms,
        }
    }

    pub async fn query_logs<M: MetaStore, P: PublicationStore, B: BlobStore>(
        &self,
        tables: &Tables<M, B>,
        publication_store: &P,
        request: QueryLogsRequest,
        budget: ExecutionBudget,
    ) -> Result<QueryPage<Log>> {
        execute_family_query::<M, P, B, _, LogsQueryFamily>(
            tables,
            publication_store,
            &request,
            budget,
            self.max_or_terms,
        )
        .await
    }
}

impl IndexedQueryRequest for QueryLogsRequest {
    type Filter = LogFilter;

    fn start_block_num(&self) -> Option<u64> {
        self.from_block
    }

    fn end_block_num(&self) -> Option<u64> {
        self.to_block
    }

    fn start_block_hash(&self) -> Option<[u8; 32]> {
        self.from_block_hash
    }

    fn end_block_hash(&self) -> Option<[u8; 32]> {
        self.to_block_hash
    }

    fn order(&self) -> crate::core::page::QueryOrder {
        self.order
    }

    fn resume_id(&self) -> Option<u64> {
        self.resume_log_id
    }

    fn limit(&self) -> usize {
        self.limit
    }

    fn filter(&self) -> &Self::Filter {
        &self.filter
    }
}

struct LogsQueryFamily;

impl IndexedQueryFamily for LogsQueryFamily {
    type Id = LogId;
    type Shard = LogShard;
    type ClauseKind = ClauseKind;
    type ClauseSpec = IndexedClauseSpec;
    type Filter = LogFilter;
    type Output = Log;
    type StreamFamily = LogsStreamFamily;
    type Materializer<'a, M: MetaStore + 'a, B: BlobStore + 'a> = LogMaterializer<'a, M, B>;

    fn build_clause_specs(filter: &Self::Filter) -> Vec<Self::ClauseSpec> {
        build_clause_specs(filter)
    }

    fn shard_from_raw(shard_raw: u64) -> Self::Shard {
        LogShard::new(shard_raw).expect("shard derived from LogId range")
    }

    fn local_range_for_shard(
        from: Self::Id,
        to_inclusive: Self::Id,
        shard: Self::Shard,
    ) -> (u32, u32) {
        family_local_range_for_shard(from, to_inclusive, shard.get())
    }

    async fn prepare_shard_clauses<M: MetaStore, B: BlobStore>(
        tables: &Tables<M, B>,
        clause_specs: &[Self::ClauseSpec],
        shard: Self::Shard,
        local_from: u32,
        local_to: u32,
    ) -> Result<Vec<PreparedClause<Self::ClauseKind>>> {
        prepare_shard_clauses(
            tables,
            clause_specs,
            shard,
            LogLocalId::new(local_from).expect("local range start must fit local-id"),
            LogLocalId::new(local_to).expect("local range end must fit local-id"),
        )
        .await
    }

    async fn resolve_window<M: MetaStore, B: BlobStore>(
        tables: &Tables<M, B>,
        block_range: &crate::core::range::ResolvedBlockRange,
    ) -> Result<Option<crate::core::ids::FamilyIdRange<Self::Id>>> {
        resolve_log_window(tables, block_range).await
    }

    fn make_materializer<'a, M: MetaStore, B: BlobStore>(
        tables: &'a Tables<M, B>,
    ) -> Self::Materializer<'a, M, B> {
        LogMaterializer::new(tables)
    }

    fn indexed_clause_error() -> &'static str {
        "query must include at least one indexed address or topic clause"
    }

    fn resume_error() -> &'static str {
        "resume_log_id outside resolved block window"
    }
}
