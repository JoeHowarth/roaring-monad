use crate::api::{ExecutionBudget, QueryTransactionsRequest};
use crate::config::Config;
use crate::core::page::QueryPage;
use crate::error::Result;
use crate::query::engine::{FamilyQueryTables, QueryLimits, execute_family_query};
use crate::store::publication::PublicationStore;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;
use crate::txs::materialize::TxMaterializer;
use crate::txs::view::TxRef;

#[derive(Debug, Clone)]
pub struct TxsQueryEngine {
    max_or_terms: usize,
}

impl TxsQueryEngine {
    pub fn from_config(config: &Config) -> Self {
        Self {
            max_or_terms: config.planner_max_or_terms,
        }
    }

    pub async fn query_transactions<M: MetaStore, P: PublicationStore, B: BlobStore>(
        &self,
        tables: &Tables<M, B>,
        publication_store: &P,
        request: QueryTransactionsRequest,
        budget: ExecutionBudget,
    ) -> Result<QueryPage<TxRef>> {
        let mut materializer = TxMaterializer::new(tables);
        execute_family_query(
            FamilyQueryTables {
                tables,
                stream_tables: &tables.tx_streams,
            },
            publication_store,
            &request,
            QueryLimits {
                budget,
                max_or_terms: self.max_or_terms,
            },
            &mut materializer,
            |record| record.txs,
        )
        .await
    }
}
