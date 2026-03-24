use crate::core::ids::FamilyIdRange;
use crate::core::range::ResolvedBlockRange;
use crate::core::state::{BlockRecord, PrimaryWindowRecord};
use crate::error::Result;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;

use super::types::PrimaryId;

pub(crate) async fn resolve_primary_window<M, B, I, F>(
    tables: &Tables<M, B>,
    block_range: &ResolvedBlockRange,
    select_window: F,
) -> Result<Option<FamilyIdRange<I>>>
where
    M: MetaStore,
    B: BlobStore,
    I: PrimaryId,
    F: Fn(&BlockRecord) -> Option<PrimaryWindowRecord>,
{
    if block_range.is_empty() {
        return Ok(None);
    }

    let Some(start) = first_primary_id_in_range(
        tables,
        &select_window,
        block_range.from_block,
        block_range.to_block,
    )
    .await?
    else {
        return Ok(None);
    };
    let Some(end_exclusive): Option<I> = end_primary_id_exclusive_in_range(
        tables,
        &select_window,
        block_range.from_block,
        block_range.to_block,
    )
    .await?
    else {
        return Ok(None);
    };

    Ok(FamilyIdRange::new(
        start,
        <I as crate::core::ids::FamilyIdValue>::new(end_exclusive.get().saturating_sub(1)),
    ))
}

async fn first_primary_id_in_range<M, B, I, F>(
    tables: &Tables<M, B>,
    select_window: &F,
    from_block: u64,
    to_block: u64,
) -> Result<Option<I>>
where
    M: MetaStore,
    B: BlobStore,
    I: PrimaryId,
    F: Fn(&BlockRecord) -> Option<PrimaryWindowRecord>,
{
    let mut block_num = from_block;
    while block_num <= to_block {
        let Some(record) = tables.block_records().get(block_num).await? else {
            return Ok(None);
        };
        let Some(window) = select_window(&record) else {
            return Ok(None);
        };
        if window.count > 0 {
            return Ok(Some(I::new(window.first_primary_id)));
        }
        block_num = block_num.saturating_add(1);
    }
    Ok(None)
}

async fn end_primary_id_exclusive_in_range<M, B, I, F>(
    tables: &Tables<M, B>,
    select_window: &F,
    from_block: u64,
    to_block: u64,
) -> Result<Option<I>>
where
    M: MetaStore,
    B: BlobStore,
    I: PrimaryId,
    F: Fn(&BlockRecord) -> Option<PrimaryWindowRecord>,
{
    let mut block_num = to_block;
    loop {
        let Some(record) = tables.block_records().get(block_num).await? else {
            return Ok(None);
        };
        let Some(window) = select_window(&record) else {
            return Ok(None);
        };
        if window.count > 0 {
            return Ok(Some(I::new(
                window
                    .first_primary_id
                    .saturating_add(u64::from(window.count)),
            )));
        }
        if block_num == from_block {
            break;
        }
        block_num = block_num.saturating_sub(1);
    }
    Ok(None)
}
