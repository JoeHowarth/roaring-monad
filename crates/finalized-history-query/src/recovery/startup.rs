use crate::config::Config;
use crate::core::ids::LogId;
use crate::core::state::{FinalizedHeadState, derive_next_log_id, load_finalized_head_state};
use crate::error::Result;
use crate::ingest::open_pages::repair_open_stream_page_markers;
use crate::ingest::publication::{
    PublicationLease, acquire_publication_with_session, current_time_ms, new_session_id,
};
use crate::ingest::recovery::cleanup_unpublished_suffix;
use crate::logs::types::LogSequencingState;
use crate::store::publication::{FenceStore, PublicationStore};
use crate::store::traits::{BlobStore, MetaStore};

#[derive(Debug, Clone)]
pub struct RecoveryPlan {
    pub head_state: FinalizedHeadState,
    pub log_state: LogSequencingState,
    pub warm_streams: usize,
}

pub async fn startup_with_writer<M: MetaStore + PublicationStore + FenceStore, B: BlobStore>(
    config: &Config,
    meta_store: &M,
    blob_store: &B,
    warm_streams: usize,
    writer_id: u64,
) -> Result<(RecoveryPlan, PublicationLease)> {
    let lease = acquire_publication_with_session(
        meta_store,
        writer_id,
        new_session_id(writer_id),
        current_time_ms(),
        config.publication_lease_duration_ms,
    )
    .await?;
    let fence = lease.fence_token();
    let _cleaned =
        cleanup_unpublished_suffix(meta_store, blob_store, lease.indexed_finalized_head, fence)
            .await?;
    let next_log_id = derive_next_log_id(meta_store, lease.indexed_finalized_head).await?;
    repair_open_stream_page_markers(meta_store, blob_store, next_log_id, fence).await?;
    Ok((
        build_recovery_plan(
            lease.indexed_finalized_head,
            lease.epoch,
            next_log_id,
            warm_streams,
        ),
        lease,
    ))
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

fn build_recovery_plan(
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
