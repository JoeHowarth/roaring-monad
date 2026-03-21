use crate::block::FinalizedBlock;
use crate::config::Config;
use crate::error::{Error, Result};
use crate::family::Family;
use crate::runtime::Runtime;
use crate::store::traits::{BlobStore, MetaStore};
use crate::txs::types::TxStartupState;

#[derive(Debug, Clone, Copy, Default)]
pub struct TxsFamily;

impl<M: MetaStore, B: BlobStore> Family<M, B> for TxsFamily {
    type State = TxStartupState;

    async fn load_startup_state(
        &self,
        _runtime: &Runtime<M, B>,
        _indexed_finalized_head: u64,
    ) -> Result<Self::State> {
        Ok(TxStartupState)
    }

    async fn ingest_block(
        &self,
        _config: &Config,
        _runtime: &Runtime<M, B>,
        _state: &mut Self::State,
        block: &FinalizedBlock,
    ) -> Result<usize> {
        if !block.txs.is_empty() {
            return Err(Error::Unsupported("tx ingest not implemented"));
        }
        Ok(0)
    }
}
