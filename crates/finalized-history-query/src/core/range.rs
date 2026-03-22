use crate::core::page::QueryOrder;
use crate::core::refs::BlockRef;
use crate::core::state::load_block_identity;
use crate::error::{Error, Result};
use crate::store::publication::PublicationStore;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedBlockRange {
    pub from_block: u64,
    pub to_block: u64,
    pub resolved_from_ref: BlockRef,
    pub resolved_to_ref: BlockRef,
    pub examined_endpoint_ref: BlockRef,
}

impl ResolvedBlockRange {
    pub fn empty(anchor: BlockRef) -> Self {
        Self {
            from_block: anchor.number.saturating_add(1),
            to_block: anchor.number,
            resolved_from_ref: anchor,
            resolved_to_ref: anchor,
            examined_endpoint_ref: anchor,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.from_block > self.to_block
    }
}

pub async fn resolve_block_range<M: MetaStore, P: PublicationStore>(
    tables: &Tables<M, impl BlobStore>,
    publication_store: &P,
    from_block: u64,
    to_block: u64,
    order: QueryOrder,
) -> Result<ResolvedBlockRange> {
    match order {
        QueryOrder::Ascending => {}
    }

    if from_block > to_block {
        return Err(Error::InvalidParams(
            "from_block must be less than or equal to to_block",
        ));
    }

    let finalized_head = publication_store
        .load_finalized_head_state()
        .await?
        .indexed_finalized_head;

    let anchor = if finalized_head == 0 {
        BlockRef::zero(0)
    } else {
        load_block_ref(tables, finalized_head)
            .await?
            .unwrap_or_else(|| BlockRef::zero(finalized_head))
    };

    if finalized_head == 0 || from_block > finalized_head {
        return Ok(ResolvedBlockRange::empty(anchor));
    }

    let clipped_to = to_block.min(finalized_head);
    let Some(resolved_from_ref) = load_block_ref(tables, from_block).await? else {
        return Ok(ResolvedBlockRange::empty(anchor));
    };
    let Some(resolved_to_ref) = load_block_ref(tables, clipped_to).await? else {
        return Ok(ResolvedBlockRange::empty(anchor));
    };

    Ok(ResolvedBlockRange {
        from_block,
        to_block: clipped_to,
        resolved_from_ref,
        resolved_to_ref,
        examined_endpoint_ref: resolved_to_ref,
    })
}

pub async fn load_block_ref<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    block_num: u64,
) -> Result<Option<BlockRef>> {
    Ok(load_block_identity(tables, block_num)
        .await?
        .map(|identity| identity.into_block_ref()))
}
