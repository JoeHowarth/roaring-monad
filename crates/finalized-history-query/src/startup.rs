use crate::core::ids::LogId;
use crate::core::state::derive_next_log_id;
use crate::error::Result;
use crate::logs::types::LogSequencingState;
use crate::store::publication::{FinalizedHeadState, PublicationStore};
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;

#[derive(Debug, Clone)]
pub struct StartupPlan {
    pub head_state: FinalizedHeadState,
    pub log_state: LogSequencingState,
    pub warm_streams: usize,
}

pub async fn startup_plan<M: MetaStore + PublicationStore, B: BlobStore>(
    tables: &Tables<M, B>,
    warm_streams: usize,
) -> Result<StartupPlan> {
    let head_state = tables.meta_store().load_finalized_head_state().await?;
    let next_log_id = derive_next_log_id(tables, head_state.indexed_finalized_head).await?;
    Ok(build_startup_plan(
        head_state.indexed_finalized_head,
        next_log_id,
        warm_streams,
    ))
}

pub(crate) fn build_startup_plan(
    indexed_finalized_head: u64,
    next_log_id: u64,
    warm_streams: usize,
) -> StartupPlan {
    StartupPlan {
        head_state: FinalizedHeadState {
            indexed_finalized_head,
        },
        log_state: LogSequencingState {
            next_log_id: LogId::new(next_log_id),
        },
        warm_streams,
    }
}
