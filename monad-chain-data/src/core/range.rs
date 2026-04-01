use core::future::Future;

use crate::core::page::QueryOrder;
use crate::core::refs::BlockRef;
use crate::core::state::BlockRecord;
use crate::error::{MonadChainDataError, Result};
use crate::logs::QueryLogsRequest;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedBlockWindow {
    pub from_block: BlockRef,
    pub to_block: BlockRef,
}

pub fn resolve_query_block_numbers(
    request: &QueryLogsRequest,
    published_head: u64,
) -> Result<(u64, u64)> {
    match request.order {
        QueryOrder::Ascending => {
            let requested_from = request.from_block.unwrap_or(1).max(1);
            let requested_to = request.to_block.unwrap_or(published_head);
            let resolved_to = requested_to.min(published_head);
            if requested_from > resolved_to {
                return Err(MonadChainDataError::InvalidRequest(
                    "from_block must be within the published ascending range",
                ));
            }
            Ok((requested_from, resolved_to))
        }
        QueryOrder::Descending => {
            let requested_from = request.from_block.unwrap_or(published_head).min(published_head);
            let requested_to = request.to_block.unwrap_or(1).max(1);
            if requested_from < requested_to {
                return Err(MonadChainDataError::InvalidRequest(
                    "from_block must be greater than or equal to to_block in descending order",
                ));
            }
            Ok((requested_from, requested_to))
        }
    }
}

pub async fn resolve_block_window<F, Fut>(
    request: &QueryLogsRequest,
    published_head: u64,
    mut load_block_record: F,
) -> Result<ResolvedBlockWindow>
where
    F: FnMut(u64) -> Fut,
    Fut: Future<Output = Result<Option<BlockRecord>>>,
{
    let (from_number, to_number) = resolve_query_block_numbers(request, published_head)?;

    let from_record = load_block_record(from_number)
        .await?
        .ok_or(MonadChainDataError::MissingData("missing from_block record"))?;
    let to_record = load_block_record(to_number)
        .await?
        .ok_or(MonadChainDataError::MissingData("missing to_block record"))?;

    Ok(ResolvedBlockWindow {
        from_block: BlockRef::from(&from_record),
        to_block: BlockRef::from(&to_record),
    })
}
