use crate::core::ids::LogId;
use crate::core::state::{FinalizedHeadState, derive_next_log_id, load_finalized_head_state};
use crate::error::Result;
use crate::ingest::open_pages::repair_open_stream_page_markers;
use crate::ingest::recovery::cleanup_unpublished_suffix;
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
    blob_store: &B,
    warm_streams: usize,
) -> Result<RecoveryPlan> {
    let head_state = load_finalized_head_state(meta_store).await?;
    let _cleaned =
        cleanup_unpublished_suffix(meta_store, blob_store, head_state.indexed_finalized_head, 0)
            .await?;
    let next_log_id = derive_next_log_id(meta_store, head_state.indexed_finalized_head).await?;
    repair_open_stream_page_markers(meta_store, blob_store, next_log_id, 0).await?;

    Ok(RecoveryPlan {
        head_state,
        log_state: LogSequencingState {
            next_log_id: LogId::new(next_log_id),
        },
        warm_streams,
    })
}
