use crate::error::Result;
use crate::family as family_boundary;
use crate::logs::family::LogsFamily;
use crate::logs::types::LogSequencingState;
use crate::runtime::Runtime;
use crate::store::publication::{FinalizedHeadState, PublicationStore};
use crate::store::traits::{BlobStore, MetaStore};

#[derive(Debug, Clone)]
pub struct StartupPlan {
    pub head_state: FinalizedHeadState,
    pub log_state: LogSequencingState,
    pub warm_streams: usize,
}

pub async fn startup_plan<M: MetaStore, P: PublicationStore, B: BlobStore>(
    runtime: &Runtime<M, B>,
    publication_store: &P,
    warm_streams: usize,
) -> Result<StartupPlan> {
    let state =
        family_boundary::startup_state(runtime, publication_store, &LogsFamily, warm_streams)
            .await?;
    Ok(StartupPlan {
        head_state: state.head_state,
        log_state: state.family_state,
        warm_streams: state.warm_streams,
    })
}
