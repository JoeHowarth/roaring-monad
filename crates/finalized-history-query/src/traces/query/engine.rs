use crate::api::{ExecutionBudget, QueryTracesRequest};
use crate::config::Config;
use crate::core::ids::{TraceId, family_local_range_for_shard};
use crate::core::page::QueryPage;
use crate::error::Result;
use crate::query::engine::{
    FamilyQueryTables, IndexedQueryRequest, QueryLimits, execute_family_query,
};
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
            FamilyQueryTables {
                tables,
                stream_tables: &tables.trace_streams,
            },
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

    fn local_range_for_shard(
        &self,
        from: Self::Id,
        to_inclusive: Self::Id,
        shard_raw: u64,
    ) -> (u32, u32) {
        family_local_range_for_shard(from, to_inclusive, shard_raw)
    }
}
