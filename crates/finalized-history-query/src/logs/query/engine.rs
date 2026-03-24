use crate::api::{ExecutionBudget, QueryLogsRequest};
use crate::config::Config;
use crate::core::page::QueryPage;
use crate::error::Result;
use crate::logs::filter::LogFilter;
use crate::logs::materialize::LogMaterializer;
use crate::logs::types::Log;
use crate::query::engine::{
    FamilyQueryTables, IndexedQueryRequest, QueryLimits, execute_family_query,
};
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
        let mut materializer = LogMaterializer::new(tables);
        execute_family_query(
            FamilyQueryTables {
                tables,
                stream_tables: &tables.log_streams,
            },
            publication_store,
            &request,
            QueryLimits {
                budget,
                max_or_terms: self.max_or_terms,
            },
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
