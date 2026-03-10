use crate::codec::finalized_state::decode_meta_state;
use crate::core::state::{FinalizedHeadState, load_finalized_head_state};
use crate::domain::keys::META_STATE_KEY;
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
    let state = match store.get(META_STATE_KEY).await? {
        Some(r) => decode_meta_state(&r.value)?,
        None => Default::default(),
    };

    Ok(RecoveryPlan {
        head_state,
        log_state: LogSequencingState::from(&state),
        warm_streams,
    })
}
