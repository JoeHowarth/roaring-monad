use crate::error::Result;
use crate::family::Families;
use crate::logs::types::LogSequencingState;
use crate::runtime::Runtime;
use crate::store::publication::{FinalizedHeadState, PublicationStore};
use crate::store::traits::{BlobStore, MetaStore};
use crate::traces::TraceSequencingState;
use crate::txs::TxFamilyState;

#[derive(Debug, Clone)]
pub struct ServiceStatus {
    pub head_state: FinalizedHeadState,
    pub log_state: LogSequencingState,
    pub tx_state: TxFamilyState,
    pub trace_state: TraceSequencingState,
}

pub async fn service_status<M: MetaStore, P: PublicationStore, B: BlobStore>(
    runtime: &Runtime<M, B>,
    publication_store: &P,
    families: &Families,
) -> Result<ServiceStatus> {
    let head_state = publication_store.load_finalized_head_state().await?;
    let family_states = families
        .load_state_from_head(runtime, head_state.indexed_finalized_head)
        .await?;
    Ok(ServiceStatus {
        head_state,
        log_state: family_states.logs,
        tx_state: family_states.txs,
        trace_state: family_states.traces,
    })
}
