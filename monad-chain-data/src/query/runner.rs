use crate::core::range::ResolvedBlockWindow;
use crate::error::Result;
use crate::logs::QueryLogsResponse;
use crate::store::{BlobStore, MetaStore};

pub async fn execute_unfiltered_block_query<M: MetaStore, B: BlobStore>(
    meta_store: &M,
    blob_store: &B,
    request: &crate::logs::QueryLogsRequest,
    window: ResolvedBlockWindow,
) -> Result<QueryLogsResponse> {
    let _ = (meta_store, blob_store, request, window);
    todo!("execute the fallback block-scan query path")
}
