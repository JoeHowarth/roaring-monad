use crate::api::{ExecutionBudget, QueryBlocksRequest};
use crate::core::page::{QueryPage, QueryPageMeta};
use crate::core::range::{ResolvedBlockRange, resolve_block_range};
use crate::core::refs::BlockRef;
use crate::core::state::{BlockIdentity, load_block_identity};
use crate::error::{Error, Result};
use crate::query::engine::resolve_request_block_bounds;
use crate::query::normalized::effective_limit;
use crate::query::runner::empty_page;
use crate::store::publication::PublicationStore;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct Block {
    pub number: u64,
    pub hash: [u8; 32],
    pub parent_hash: [u8; 32],
}

impl From<BlockIdentity> for Block {
    fn from(identity: BlockIdentity) -> Self {
        Self {
            number: identity.number,
            hash: identity.hash,
            parent_hash: identity.parent_hash,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct BlocksQueryEngine;

impl BlocksQueryEngine {
    pub async fn query_blocks<M: MetaStore, P: PublicationStore, B: BlobStore>(
        &self,
        tables: &Tables<M, B>,
        publication_store: &P,
        request: QueryBlocksRequest,
        budget: ExecutionBudget,
    ) -> Result<QueryPage<Block>> {
        let (from_block, to_block) = resolve_request_block_bounds(tables, &request).await?;
        let block_range = resolve_block_range(
            tables,
            publication_store,
            from_block,
            to_block,
            request.order,
        )
        .await?;
        let effective_limit = effective_limit(request.limit, budget)?;
        if block_range.is_empty() {
            return Ok(empty_page(&block_range));
        }

        let mut items = Vec::with_capacity(effective_limit.saturating_add(1));
        let take = effective_limit.saturating_add(1);
        let mut block_num = block_range.from_block;
        loop {
            if block_num > block_range.to_block || items.len() >= take {
                break;
            }

            let Some(identity) = load_block_identity(tables, block_num).await? else {
                return Err(Error::NotFound);
            };
            items.push((identity.into_block_ref(), Block::from(identity)));

            if block_num == block_range.to_block {
                break;
            }
            block_num = block_num.saturating_add(1);
        }

        Ok(build_block_page(block_range, effective_limit, items))
    }
}

fn build_block_page(
    block_range: ResolvedBlockRange,
    effective_limit: usize,
    mut items: Vec<(BlockRef, Block)>,
) -> QueryPage<Block> {
    let has_more = items.len() > effective_limit;
    if has_more {
        items.truncate(effective_limit);
    }

    let cursor_block = items
        .last()
        .map(|(block_ref, _)| *block_ref)
        .unwrap_or(block_range.examined_endpoint_ref);
    let items = items
        .into_iter()
        .map(|(_, block)| block)
        .collect::<Vec<_>>();

    QueryPage {
        items,
        meta: QueryPageMeta {
            resolved_from_block: block_range.resolved_from_ref,
            resolved_to_block: block_range.resolved_to_ref,
            cursor_block,
            has_more,
            next_resume_id: None,
        },
    }
}
