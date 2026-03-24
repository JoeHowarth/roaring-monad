use crate::config::Config;
use crate::core::ids::TxId;
use crate::core::state::BlockRecord;
use crate::error::{Error, Result};
use crate::family::FinalizedBlock;
use crate::runtime::Runtime;
use crate::store::traits::{BlobStore, MetaStore};
use crate::txs::types::TxFamilyState;

#[derive(Debug, Clone, Copy, Default)]
pub struct TxsFamily;

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
