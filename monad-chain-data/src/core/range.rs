use crate::core::refs::BlockRef;
use crate::error::Result;
use crate::logs::QueryLogsRequest;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedBlockWindow {
    pub from_block: BlockRef,
    pub to_block: BlockRef,
}

pub async fn resolve_block_window(
    request: &QueryLogsRequest,
    published_head: u64,
) -> Result<ResolvedBlockWindow> {
    let _ = (request, published_head);
    todo!("resolve the published block window for the current query request")
}
