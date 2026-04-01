use crate::core::range::ResolvedBlockWindow;
use crate::error::Result;
use crate::logs::{
    QueryLogsRequest, QueryLogsResponse, load_filtered_block_logs_for_block,
};
use crate::store::{BlobStore, MetaStore};
use crate::QueryOrder;

pub async fn execute_unfiltered_block_query<M: MetaStore, B: BlobStore>(
    meta_store: &M,
    blob_store: &B,
    request: &QueryLogsRequest,
    window: ResolvedBlockWindow,
) -> Result<QueryLogsResponse> {
    let mut logs = Vec::new();
    let mut cursor_block = window.from_block;

    match request.order {
        QueryOrder::Ascending => {
            for block_number in window.from_block.number..=window.to_block.number {
                let (block_ref, block_logs) =
                    load_filtered_block_logs_for_block(meta_store, blob_store, block_number, request)
                        .await?;
                logs.extend(block_logs);
                cursor_block = block_ref;
                if logs.len() >= request.limit {
                    break;
                }
            }
        }
        QueryOrder::Descending => {
            for block_number in (window.to_block.number..=window.from_block.number).rev() {
                let (block_ref, block_logs) =
                    load_filtered_block_logs_for_block(meta_store, blob_store, block_number, request)
                        .await?;
                logs.extend(block_logs);
                cursor_block = block_ref;
                if logs.len() >= request.limit {
                    break;
                }
            }
        }
    }

    Ok(QueryLogsResponse {
        logs,
        from_block: window.from_block,
        to_block: window.to_block,
        cursor_block,
    })
}
