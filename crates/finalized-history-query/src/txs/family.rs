use crate::config::Config;
use crate::core::ids::TxId;
use crate::core::state::BlockRecord;
use crate::error::{Error, Result};
use crate::family::FinalizedBlock;
use crate::runtime::Runtime;
use crate::store::traits::{BlobStore, MetaStore};
use crate::txs::ingest::persist_tx_artifacts;
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
        config: &Config,
        runtime: &Runtime<M, B>,
        state: &mut TxFamilyState,
        block: &FinalizedBlock,
    ) -> Result<usize> {
        let from_next_tx_id = state.next_tx_id.get();
        let tx_count =
            persist_tx_artifacts(config, &runtime.tables, block, from_next_tx_id).await?;
        let tx_count_u32 =
            u32::try_from(tx_count).map_err(|_| Error::Decode("tx count overflow"))?;

        runtime
            .tables
            .tx_dir
            .persist_block_fragment(block.block_num, from_next_tx_id, tx_count_u32)
            .await?;

        state.next_tx_id = TxId::new(from_next_tx_id.saturating_add(tx_count as u64));
        Ok(tx_count)
    }
}
