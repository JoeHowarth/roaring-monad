use crate::config::Config;
use crate::core::ids::TxId;
use crate::core::page::QueryPage;
use crate::core::state::BlockRecord;
use crate::error::{Error, Result};
use crate::family::{FinalizedBlock, Hash32};
use crate::runtime::Runtime;
use crate::store::publication::PublicationStore;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct Tx {
    pub tx_idx: u32,
    pub tx_hash: Hash32,
    pub block_num: u64,
    pub block_hash: Hash32,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TxFamilyState {
    pub next_tx_id: TxId,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TxFilter;

#[derive(Debug, Clone, Copy, Default)]
pub struct TxsFamily;

#[derive(Debug, Clone, Copy, Default)]
pub struct TxsQueryEngine;

impl TxsFamily {
    pub fn load_state_from_head_record(
        &self,
        head_record: Option<&BlockRecord>,
    ) -> Result<TxFamilyState> {
        let next_tx_id = match head_record {
            None => 0,
            Some(block_record) => {
                let window = block_record.txs.ok_or(Error::NotFound)?;
                window
                    .first_primary_id
                    .saturating_add(u64::from(window.count))
            }
        };
        Ok(TxFamilyState {
            next_tx_id: TxId::new(next_tx_id),
        })
    }

    pub async fn ingest_block<M: MetaStore, B: BlobStore>(
        &self,
        _config: &Config,
        _runtime: &Runtime<M, B>,
        _state: &mut TxFamilyState,
        block: &FinalizedBlock,
    ) -> Result<usize> {
        if !block.txs.is_empty() {
            return Err(Error::Unsupported("tx ingest not implemented"));
        }
        Ok(0)
    }
}

impl TxsQueryEngine {
    pub fn from_config(_config: &Config) -> Self {
        Self
    }

    pub async fn query_transactions<M: MetaStore, P: PublicationStore, B: BlobStore>(
        &self,
        _tables: &Tables<M, B>,
        _publication_store: &P,
        _request: crate::api::QueryTransactionsRequest,
        _budget: crate::api::ExecutionBudget,
    ) -> Result<QueryPage<Tx>> {
        Err(Error::Unsupported("tx query not implemented"))
    }
}
