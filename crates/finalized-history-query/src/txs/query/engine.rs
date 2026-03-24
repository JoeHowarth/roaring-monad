use crate::api::{ExecutionBudget, QueryTransactionsRequest};
use crate::config::Config;
use crate::core::page::QueryPage;
use crate::error::Result;
use crate::store::publication::PublicationStore;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;
use crate::txs::view::Tx;

#[derive(Debug, Clone, Copy, Default)]
pub struct TxsQueryEngine;

impl TxsQueryEngine {
    pub fn from_config(_config: &Config) -> Self {
        Self
    }

    pub async fn query_transactions<M: MetaStore, P: PublicationStore, B: BlobStore>(
        &self,
        _tables: &Tables<M, B>,
        _publication_store: &P,
        _request: QueryTransactionsRequest,
        _budget: ExecutionBudget,
    ) -> Result<QueryPage<Tx>> {
        todo!("tx indexed query execution is not implemented")
    }
}
