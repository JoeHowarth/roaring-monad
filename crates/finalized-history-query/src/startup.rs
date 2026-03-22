use crate::error::Result;
use crate::family::Families;
use crate::logs::types::LogSequencingState;
use crate::runtime::Runtime;
use crate::store::publication::{FinalizedHeadState, PublicationStore};
use crate::store::traits::{BlobStore, MetaStore};
use crate::traces::TraceStartupState;
use crate::txs::TxStartupState;

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
    families: &Families,
    warm_streams: usize,
) -> Result<StartupPlan> {
    let head_state = publication_store.load_finalized_head_state().await?;
    let family_states = families
        .load_startup_state(runtime, head_state.indexed_finalized_head)
        .await?;
    Ok(StartupPlan {
        head_state,
        log_state: family_states.logs,
        tx_state: family_states.txs,
        trace_state: family_states.traces,
        warm_streams,
    })
}

pub async fn startup_plan_from_head<M: MetaStore, B: BlobStore>(
    runtime: &Runtime<M, B>,
    families: &Families,
    indexed_finalized_head: u64,
    warm_streams: usize,
) -> Result<StartupPlan> {
    let family_states = families
        .load_startup_state(runtime, indexed_finalized_head)
        .await?;
    Ok(StartupPlan {
        head_state: FinalizedHeadState {
            indexed_finalized_head,
        },
        log_state: family_states.logs,
        tx_state: family_states.txs,
        trace_state: family_states.traces,
        warm_streams,
    })
}
