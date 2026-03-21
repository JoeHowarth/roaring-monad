use crate::error::Result;
use crate::family::{Families, startup_state};
use crate::logs::types::LogSequencingState;
use crate::runtime::Runtime;
use crate::store::publication::{FinalizedHeadState, PublicationStore};
use crate::store::traits::{BlobStore, MetaStore};
use crate::traces::types::TraceStartupState;
use crate::txs::types::TxStartupState;

#[derive(Debug, Clone)]
pub struct StartupPlan {
    pub head_state: FinalizedHeadState,
    pub log_state: LogSequencingState,
    pub tx_state: TxStartupState,
    pub trace_state: TraceStartupState,
    pub warm_streams: usize,
}

pub async fn startup_plan<M: MetaStore, P: PublicationStore, B: BlobStore>(
    runtime: &Runtime<M, B>,
    publication_store: &P,
    warm_streams: usize,
) -> Result<StartupPlan> {
    let state = startup_state(
        runtime,
        publication_store,
        &Families::default(),
        warm_streams,
    )
    .await?;
    Ok(StartupPlan {
        head_state: state.head_state,
        log_state: state.family_states.logs,
        tx_state: state.family_states.txs,
        trace_state: state.family_states.traces,
        warm_streams: state.warm_streams,
    })
}
