use crate::core::ids::LogId;
use crate::core::state::{FinalizedHeadState, derive_next_log_id};
use crate::error::Result;
use crate::ingest::open_pages::repair_open_stream_page_markers;
use crate::ingest::publication::{PublicationLease, acquire_publication};
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

pub async fn startup_with_lease<M: MetaStore + PublicationStore, B: BlobStore>(
    meta_store: &M,
    blob_store: &B,
    warm_streams: usize,
) -> Result<(RecoveryPlan, PublicationLease)> {
    startup_with_writer(meta_store, blob_store, warm_streams, 0).await
}

pub async fn startup_with_writer<M: MetaStore + PublicationStore, B: BlobStore>(
    meta_store: &M,
    blob_store: &B,
    warm_streams: usize,
    writer_id: u64,
) -> Result<(RecoveryPlan, PublicationLease)> {
    let lease = acquire_publication(meta_store, writer_id).await?;
    let _cleaned = cleanup_unpublished_suffix(
        meta_store,
        blob_store,
        lease.indexed_finalized_head,
        writer_id,
    )
    .await?;
    let next_log_id = derive_next_log_id(meta_store, lease.indexed_finalized_head).await?;
    repair_open_stream_page_markers(meta_store, blob_store, next_log_id, writer_id).await?;
    let head_state = FinalizedHeadState {
        indexed_finalized_head: lease.indexed_finalized_head,
        publication_epoch: lease.epoch,
    };

    Ok((
        RecoveryPlan {
            head_state,
            log_state: LogSequencingState {
                next_log_id: LogId::new(next_log_id),
            },
            warm_streams,
        },
        lease,
    ))
}

pub async fn startup_plan<M: MetaStore + PublicationStore, B: BlobStore>(
    meta_store: &M,
    blob_store: &B,
    warm_streams: usize,
) -> Result<RecoveryPlan> {
    startup_with_lease(meta_store, blob_store, warm_streams)
        .await
        .map(|(plan, _)| plan)
}
