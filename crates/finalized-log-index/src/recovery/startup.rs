use crate::codec::log::decode_meta_state;
use crate::domain::keys::META_STATE_KEY;
use crate::domain::types::MetaState;
use crate::error::Result;
use crate::store::traits::MetaStore;

#[derive(Debug, Clone)]
pub struct RecoveryPlan {
    pub state: MetaState,
    pub warm_streams: usize,
}

pub async fn startup_plan<S: MetaStore>(store: &S, warm_streams: usize) -> Result<RecoveryPlan> {
    let state = match store.get(META_STATE_KEY).await? {
        Some(r) => decode_meta_state(&r.value)?,
        None => MetaState::default(),
    };

    Ok(RecoveryPlan {
        state,
        warm_streams,
    })
}
