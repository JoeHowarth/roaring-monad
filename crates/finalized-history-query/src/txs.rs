use crate::config::Config;
use crate::error::{Error, Result};
use crate::family::{FinalizedBlock, Hash32};
use crate::runtime::Runtime;
use crate::store::traits::{BlobStore, MetaStore};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct Tx {
    pub tx_idx: u32,
    pub tx_hash: Hash32,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TxStartupState;

#[derive(Debug, Clone, Copy, Default)]
pub struct TxsFamily;

impl TxsFamily {
    pub async fn load_startup_state<M: MetaStore, B: BlobStore>(
        &self,
        _runtime: &Runtime<M, B>,
        _indexed_finalized_head: u64,
    ) -> Result<TxStartupState> {
        Ok(TxStartupState)
    }

    pub async fn ingest_block<M: MetaStore, B: BlobStore>(
        &self,
        _config: &Config,
        _runtime: &Runtime<M, B>,
        _state: &mut TxStartupState,
        block: &FinalizedBlock,
    ) -> Result<usize> {
        if !block.txs.is_empty() {
            return Err(Error::Unsupported("tx ingest not implemented"));
        }
        Ok(0)
    }
}
