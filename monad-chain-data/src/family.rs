use core::future::Future;

use crate::core::state::BlockRecord;
use crate::error::{MonadChainDataError, Result};
use alloy_primitives::{B256, Log};

pub type Hash32 = B256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizedBlock {
    pub block_number: u64,
    pub block_hash: Hash32,
    pub parent_hash: Hash32,
    pub logs_by_tx: Vec<Vec<Log>>,
}

pub async fn validate_block_continuity<F, Fut>(
    block: &FinalizedBlock,
    current_head: Option<u64>,
    mut load_block_record: F,
) -> Result<()>
where
    F: FnMut(u64) -> Fut,
    Fut: Future<Output = Result<Option<BlockRecord>>>,
{
    match current_head {
        None => {
            if block.block_number != 1 {
                return Err(MonadChainDataError::InvalidRequest(
                    "first ingested block must be block 1 in the first pass",
                ));
            }
        }
        Some(head) => {
            if block.block_number != head + 1 {
                return Err(MonadChainDataError::InvalidRequest(
                    "block_number must extend the published head contiguously",
                ));
            }

            let previous = load_block_record(head)
                .await?
                .ok_or(MonadChainDataError::MissingData(
                    "missing previous block record",
                ))?;
            if previous.block_hash != block.parent_hash {
                return Err(MonadChainDataError::InvalidRequest(
                    "parent_hash must match the previous published block",
                ));
            }
        }
    }

    Ok(())
}
