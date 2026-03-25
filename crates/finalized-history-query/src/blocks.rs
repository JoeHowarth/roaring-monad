use crate::api::{ExecutionBudget, QueryBlocksRequest};
use crate::core::header::{EvmBlockHeader, load_block_header};
use crate::core::page::{QueryPage, QueryPageMeta};
use crate::core::range::{ResolvedBlockRange, resolve_block_range};
use crate::core::refs::BlockRef;
use crate::error::{Error, Result};
use crate::query::bounds::resolve_request_block_bounds;
use crate::query::normalized::effective_limit;
use crate::query::runner::empty_page;
use crate::store::publication::PublicationStore;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;
use crate::txs::view::TxRef;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Block {
    pub header: EvmBlockHeader,
    pub txs: Vec<TxRef>,
}

impl core::ops::Deref for Block {
    type Target = EvmBlockHeader;

    fn deref(&self) -> &Self::Target {
        &self.header
    }
}

pub async fn load_block<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    block_num: u64,
) -> Result<Option<Block>> {
    let Some(header) = load_block_header(tables, block_num).await? else {
        return Ok(None);
    };
    let txs = tables
        .block_tx_blobs
        .load_block(block_num)
        .await?
        .ok_or(Error::NotFound)?;
    Ok(Some(Block { header, txs }))
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
        let (from_block, to_block) = resolve_request_block_bounds(
            tables,
            request.from_block,
            request.to_block,
            request.from_block_hash,
            request.to_block_hash,
        )
        .await?;
        let effective_limit = effective_limit(request.limit, budget)?;
        let block_range = resolve_block_range(
            tables,
            publication_store,
            from_block,
            to_block,
            request.order,
        )
        .await?;
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

            let Some(block) = load_block(tables, block_num).await? else {
                return Err(Error::NotFound);
            };
            items.push((
                BlockRef {
                    number: block.header.number,
                    hash: block.header.hash,
                    parent_hash: block.header.parent_hash,
                },
                block,
            ));

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
