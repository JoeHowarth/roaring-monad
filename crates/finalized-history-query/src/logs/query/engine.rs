use super::clause::{
    IndexedClauseSpec, build_clause_specs, load_prepared_clause_bitmap, prepare_shard_clauses,
};
use crate::api::{ExecutionBudget, QueryLogsRequest};
use crate::config::Config;
use crate::core::ids::{LogId, LogLocalId, LogShard, family_local_range_for_shard};
use crate::core::page::QueryPage;
use crate::error::Result;
use crate::logs::filter::LogFilter;
use crate::logs::materialize::LogMaterializer;
use crate::logs::types::Log;
use crate::query::engine::{IndexedQueryRequest, QueryLimits, execute_family_query};
use crate::query::planner::PreparedClause;
use crate::query::runner::QueryDescriptor;
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
        let descriptor = LogsQueryDescriptor;
        let mut materializer = LogMaterializer::new(tables);
        execute_family_query(
            tables,
            publication_store,
            &request,
            QueryLimits {
                budget,
                max_or_terms: self.max_or_terms,
            },
            &descriptor,
            &mut materializer,
            |record| record.logs,
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

struct LogsQueryDescriptor;

impl QueryDescriptor for LogsQueryDescriptor {
    type Id = LogId;
    type ClauseSpec = IndexedClauseSpec;
    type Filter = LogFilter;

    fn build_clause_specs(&self, filter: &Self::Filter) -> Vec<Self::ClauseSpec> {
        build_clause_specs(filter)
    }

    fn local_range_for_shard(
        &self,
        from: Self::Id,
        to_inclusive: Self::Id,
        shard_raw: u64,
    ) -> (u32, u32) {
        family_local_range_for_shard(from, to_inclusive, shard_raw)
    }

    async fn prepare_shard_clauses<M: MetaStore, B: BlobStore>(
        &self,
        tables: &Tables<M, B>,
        clause_specs: &[Self::ClauseSpec],
        shard_raw: u64,
        local_from: u32,
        local_to: u32,
    ) -> Result<Vec<PreparedClause>> {
        prepare_shard_clauses(
            tables,
            clause_specs,
            LogShard::new(shard_raw).expect("shard derived from LogId range"),
            LogLocalId::new(local_from).expect("local range start must fit local-id"),
            LogLocalId::new(local_to).expect("local range end must fit local-id"),
        )
        .await
    }

    async fn load_prepared_clause_bitmap<M: MetaStore, B: BlobStore>(
        &self,
        tables: &Tables<M, B>,
        prepared_clause: &PreparedClause,
        local_from: u32,
        local_to: u32,
    ) -> Result<roaring::RoaringBitmap> {
        load_prepared_clause_bitmap(
            tables,
            prepared_clause,
            LogLocalId::new(local_from).expect("local range start must fit local-id"),
            LogLocalId::new(local_to).expect("local range end must fit local-id"),
        )
        .await
    }
}
