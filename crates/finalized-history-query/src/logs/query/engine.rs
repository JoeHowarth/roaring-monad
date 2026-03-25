use crate::api::{ExecutionBudget, QueryLogsRequest};
use crate::config::Config;
use crate::core::page::QueryPage;
use crate::error::Result;
use crate::logs::log_ref::LogRef;
use crate::logs::materialize::LogMaterializer;
use crate::query::engine::{FamilyQueryTables, QueryLimits, execute_family_query};
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
    ) -> Result<QueryPage<LogRef>> {
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
