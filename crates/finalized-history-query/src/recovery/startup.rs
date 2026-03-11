use crate::core::ids::LogId;
use crate::core::state::{FinalizedHeadState, derive_next_log_id, load_finalized_head_state};
use crate::error::Result;
use crate::logs::types::LogSequencingState;
use crate::store::traits::MetaStore;

#[derive(Debug, Clone)]
pub struct RecoveryPlan {
    pub head_state: FinalizedHeadState,
    pub log_state: LogSequencingState,
    pub warm_streams: usize,
}

pub async fn startup_plan<S: MetaStore>(store: &S, warm_streams: usize) -> Result<RecoveryPlan> {
    let head_state = load_finalized_head_state(store).await?;
    let next_log_id = derive_next_log_id(store, head_state.indexed_finalized_head).await?;

    Ok(RecoveryPlan {
        head_state,
        log_state: LogSequencingState {
            next_log_id: LogId::new(next_log_id),
        },
        warm_streams,
    })
}
