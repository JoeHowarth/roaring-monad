use super::clause::{
    ClauseKind, IndexedClauseSpec, build_clause_specs, load_prepared_clause_bitmap,
    prepare_shard_clauses,
};
use crate::api::{ExecutionBudget, QueryTracesRequest};
use crate::config::Config;
use crate::core::ids::{TraceId, TraceLocalId, TraceShard, family_local_range_for_shard};
use crate::core::page::QueryPage;
use crate::error::Result;
use crate::query::engine::{IndexedQueryRequest, QueryLimits, execute_family_query};
use crate::query::planner::PreparedClause;
use crate::query::runner::QueryDescriptor;
use crate::store::publication::PublicationStore;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;
use crate::traces::filter::TraceFilter;
use crate::traces::materialize::TraceMaterializer;
use crate::traces::types::Trace;

#[derive(Debug, Clone)]
pub struct TracesQueryEngine {
    max_or_terms: usize,
}

impl TracesQueryEngine {
    pub fn from_config(config: &Config) -> Self {
        Self {
            max_or_terms: config.planner_max_or_terms,
        }
    }

    pub async fn query_traces<M: MetaStore, P: PublicationStore, B: BlobStore>(
        &self,
        tables: &Tables<M, B>,
        publication_store: &P,
        request: QueryTracesRequest,
        budget: ExecutionBudget,
    ) -> Result<QueryPage<Trace>> {
        let descriptor = TracesQueryDescriptor;
        let mut materializer = TraceMaterializer::new(tables);
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
            |record| record.traces,
        )
        .await
    }
}

impl IndexedQueryRequest for QueryTracesRequest {
    type Filter = TraceFilter;

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
        self.resume_trace_id
    }

    fn limit(&self) -> usize {
        self.limit
    }

    fn filter(&self) -> &Self::Filter {
        &self.filter
    }
}

struct TracesQueryDescriptor;

impl QueryDescriptor for TracesQueryDescriptor {
    type Id = TraceId;
    type ClauseKind = ClauseKind;
    type ClauseSpec = IndexedClauseSpec;
    type Filter = TraceFilter;

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
    ) -> Result<Vec<PreparedClause<Self::ClauseKind>>> {
        prepare_shard_clauses(
            tables,
            clause_specs,
            TraceShard::new(shard_raw).expect("shard derived from TraceId range"),
            TraceLocalId::new(local_from).expect("local range start must fit trace local-id"),
            TraceLocalId::new(local_to).expect("local range end must fit trace local-id"),
        )
        .await
    }

    async fn load_prepared_clause_bitmap<M: MetaStore, B: BlobStore>(
        &self,
        tables: &Tables<M, B>,
        prepared_clause: &PreparedClause<Self::ClauseKind>,
        local_from: u32,
        local_to: u32,
    ) -> Result<roaring::RoaringBitmap> {
        load_prepared_clause_bitmap(
            tables,
            prepared_clause,
            TraceLocalId::new(local_from).expect("local range start must fit trace local-id"),
            TraceLocalId::new(local_to).expect("local range end must fit trace local-id"),
        )
        .await
    }
}
