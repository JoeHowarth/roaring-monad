use crate::codec::finalized_state::{decode_block_meta, decode_meta_state};
use crate::core::page::QueryOrder;
use crate::core::refs::BlockRef;
use crate::core::state::{BlockIdentity, FinalizedHeadState};
use crate::domain::keys::{META_STATE_KEY, block_meta_key};
use crate::error::{Error, Result};
use crate::store::traits::MetaStore;

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

#[derive(Debug, Clone, Copy, Default)]
pub struct RangeResolver;

impl RangeResolver {
    pub async fn resolve<M: MetaStore>(
        &self,
        meta_store: &M,
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

        let finalized_head = match meta_store.get(META_STATE_KEY).await? {
            Some(record) => {
                FinalizedHeadState::from(&decode_meta_state(&record.value)?).indexed_finalized_head
            }
            None => 0,
        };

        let anchor = if finalized_head == 0 {
            BlockRef::zero(0)
        } else {
            self.load_block_ref(meta_store, finalized_head)
                .await?
                .unwrap_or_else(|| BlockRef::zero(finalized_head))
        };

        if finalized_head == 0 || from_block > finalized_head {
            return Ok(ResolvedBlockRange::empty(anchor));
        }

        let clipped_to = to_block.min(finalized_head);
        let Some(resolved_from_ref) = self.load_block_ref(meta_store, from_block).await? else {
            return Ok(ResolvedBlockRange::empty(anchor));
        };
        let Some(resolved_to_ref) = self.load_block_ref(meta_store, clipped_to).await? else {
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

    pub async fn load_block_ref<M: MetaStore>(
        &self,
        meta_store: &M,
        block_num: u64,
    ) -> Result<Option<BlockRef>> {
        let Some(record) = meta_store.get(&block_meta_key(block_num)).await? else {
            return Ok(None);
        };
        let block_meta = decode_block_meta(&record.value)?;
        Ok(Some(
            BlockIdentity::from((block_num, &block_meta)).into_block_ref(),
        ))
    }
}
