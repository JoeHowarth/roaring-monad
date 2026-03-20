use crate::core::ids::LogId;
use crate::core::state::{FinalizedHeadState, derive_next_log_id, load_finalized_head_state};
use crate::error::Result;
use crate::logs::types::LogSequencingState;
use crate::store::publication::PublicationStore;
use crate::store::traits::{BlobStore, MetaStore};

#[derive(Debug, Clone)]
pub struct RecoveryPlan {
    pub head_state: FinalizedHeadState,
    pub log_state: LogSequencingState,
    pub warm_streams: usize,
}

pub async fn startup_plan<M: MetaStore + PublicationStore, B: BlobStore>(
    meta_store: &M,
    _blob_store: &B,
    warm_streams: usize,
) -> Result<RecoveryPlan> {
    let head_state = load_finalized_head_state(meta_store).await?;
    let next_log_id = derive_next_log_id(meta_store, head_state.indexed_finalized_head).await?;
    Ok(build_recovery_plan(
        head_state.indexed_finalized_head,
        head_state.publication_epoch,
        next_log_id,
        warm_streams,
    ))
}

pub(crate) fn build_recovery_plan(
    indexed_finalized_head: u64,
    publication_epoch: u64,
    next_log_id: u64,
    warm_streams: usize,
) -> RecoveryPlan {
    RecoveryPlan {
        head_state: FinalizedHeadState {
            indexed_finalized_head,
            publication_epoch,
        },
        log_state: LogSequencingState {
            next_log_id: LogId::new(next_log_id),
        },
        warm_streams,
    }
}
