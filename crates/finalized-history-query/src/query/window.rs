use crate::core::range::ResolvedBlockRange;
use crate::error::Result;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;

use super::types::{BlockWindow, PrimaryId, PrimaryRange};

#[allow(async_fn_in_trait)]
pub(crate) trait PrimaryWindowSource {
    type Id: PrimaryId;
    type Range: PrimaryRange<Id = Self::Id>;
    type Window: BlockWindow<Self::Id>;

    async fn load_block_window<M: MetaStore, B: BlobStore>(
        &self,
        tables: &Tables<M, B>,
        block_num: u64,
    ) -> Result<Option<Self::Window>>;
}

pub(crate) async fn resolve_primary_window<M: MetaStore, B: BlobStore, S: PrimaryWindowSource>(
    tables: &Tables<M, B>,
    block_range: &ResolvedBlockRange,
    source: &S,
) -> Result<Option<S::Range>> {
    if block_range.is_empty() {
        return Ok(None);
    }

    let Some(start) =
        first_primary_id_in_range(tables, source, block_range.from_block, block_range.to_block)
            .await?
    else {
        return Ok(None);
    };
    let Some(end_exclusive) = end_primary_id_exclusive_in_range(
        tables,
        source,
        block_range.from_block,
        block_range.to_block,
    )
    .await?
    else {
        return Ok(None);
    };

    Ok(S::Range::new(
        start,
        S::Id::new(end_exclusive.get().saturating_sub(1)),
    ))
}

async fn first_primary_id_in_range<M: MetaStore, B: BlobStore, S: PrimaryWindowSource>(
    tables: &Tables<M, B>,
    source: &S,
    from_block: u64,
    to_block: u64,
) -> Result<Option<S::Id>> {
    let mut block_num = from_block;
    while block_num <= to_block {
        let Some(window) = source.load_block_window(tables, block_num).await? else {
            return Ok(None);
        };
        if window.count() > 0 {
            return Ok(Some(window.first_id()));
        }
        block_num = block_num.saturating_add(1);
    }
    Ok(None)
}

async fn end_primary_id_exclusive_in_range<M: MetaStore, B: BlobStore, S: PrimaryWindowSource>(
    tables: &Tables<M, B>,
    source: &S,
    from_block: u64,
    to_block: u64,
) -> Result<Option<S::Id>> {
    let mut block_num = to_block;
    loop {
        let Some(window) = source.load_block_window(tables, block_num).await? else {
            return Ok(None);
        };
        if window.count() > 0 {
            return Ok(Some(S::Id::new(
                window
                    .first_id()
                    .get()
                    .saturating_add(window.count() as u64),
            )));
        }
        if block_num == from_block {
            break;
        }
        block_num = block_num.saturating_sub(1);
    }
    Ok(None)
}
