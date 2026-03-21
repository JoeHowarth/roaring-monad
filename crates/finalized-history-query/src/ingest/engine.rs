use crate::config::Config;
use crate::core::state::{derive_next_log_id, load_block_identity};
use crate::domain::types::{Block, IngestOutcome};
use crate::error::{Error, Result};
use crate::ingest::authority::{WriteAuthority, WriteSession};
use crate::ingest::open_pages::{
    OpenBitmapPage, collect_newly_sealed_open_bitmap_pages, delete_open_bitmap_page,
    mark_open_bitmap_page_if_absent,
};
use crate::logs::ingest::{
    compact_newly_sealed_directory, compact_stream_page, parse_stream_shard, persist_log_artifacts,
    persist_log_block_record, persist_log_dir_by_block, persist_stream_fragments,
};
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;

pub struct IngestEngine<A: WriteAuthority, M: MetaStore, B: BlobStore> {
    pub config: Config,
    pub authority: A,
    pub meta_store: M,
    pub blob_store: B,
}

impl<A: WriteAuthority, M: MetaStore + Clone, B: BlobStore + Clone> IngestEngine<A, M, B> {
    pub fn new(config: Config, authority: A, meta_store: M, blob_store: B) -> Self {
        Self {
            config,
            authority,
            meta_store,
            blob_store,
        }
    }

    pub async fn ingest_finalized_block(&self, block: &Block) -> Result<IngestOutcome> {
        self.ingest_finalized_blocks(core::slice::from_ref(block))
            .await
    }

    pub async fn ingest_finalized_blocks(&self, blocks: &[Block]) -> Result<IngestOutcome> {
        let Some(last_block) = blocks.last() else {
            return Err(Error::InvalidParams("ingest requires at least one block"));
        };

        let session = self
            .authority
            .begin_write(self.config.observe_upstream_finalized_block.as_ref()())
            .await?;
        let indexed_finalized_head = session.state().indexed_finalized_head;

        let tables = Tables::without_cache(
            std::sync::Arc::new(self.meta_store.clone()),
            std::sync::Arc::new(self.blob_store.clone()),
        );
        validate_block_sequence(&tables, blocks, indexed_finalized_head).await?;
        let from_next_log_id = derive_next_log_id(&tables, indexed_finalized_head).await?;
        let mut next_log_id = from_next_log_id;
        let mut opened_during = Vec::<OpenBitmapPage>::new();

        for block in blocks {
            persist_log_artifacts(
                &self.config,
                &self.meta_store,
                &self.blob_store,
                block.block_num,
                &block.logs,
                next_log_id,
            )
            .await?;
            persist_log_block_record(&self.meta_store, block, next_log_id).await?;
            persist_log_dir_by_block(
                &self.meta_store,
                block.block_num,
                next_log_id,
                block.logs.len() as u32,
            )
            .await?;
            let touched_pages =
                persist_stream_fragments(&self.meta_store, &self.blob_store, block, next_log_id)
                    .await?;
            opened_during.extend(touched_pages.into_iter().filter_map(
                |(stream_id, page_start)| {
                    parse_stream_shard(&stream_id).map(|shard| OpenBitmapPage {
                        shard,
                        page_start_local: page_start,
                        stream_id,
                    })
                },
            ));
            next_log_id = next_log_id.saturating_add(block.logs.len() as u64);
        }

        for page in opened_during
            .iter()
            .filter(|page| !page.is_sealed_at(next_log_id))
        {
            mark_open_bitmap_page_if_absent(&self.meta_store, page).await?;
        }

        compact_newly_sealed_directory(&self.meta_store, from_next_log_id, next_log_id).await?;

        for page in collect_newly_sealed_open_bitmap_pages(
            &self.meta_store,
            &opened_during,
            from_next_log_id,
            next_log_id,
        )
        .await?
        {
            let _ = compact_stream_page(
                &self.meta_store,
                &self.blob_store,
                &page.stream_id,
                page.page_start_local,
            )
            .await?;
            delete_open_bitmap_page(&self.meta_store, &page).await?;
        }

        session
            .publish(
                last_block.block_num,
                self.config.observe_upstream_finalized_block.as_ref()(),
            )
            .await?;

        Ok(IngestOutcome {
            indexed_finalized_head: last_block.block_num,
            written_logs: blocks.iter().map(|block| block.logs.len()).sum(),
        })
    }
}

async fn validate_block_sequence<M: MetaStore + Clone, B: BlobStore + Clone>(
    tables: &Tables<M, B>,
    blocks: &[Block],
    indexed_finalized_head: u64,
) -> Result<()> {
    let expected_first = indexed_finalized_head.saturating_add(1);
    if blocks[0].block_num != expected_first {
        return Err(Error::InvalidSequence {
            expected: expected_first,
            got: blocks[0].block_num,
        });
    }

    let expected_parent = if indexed_finalized_head == 0 {
        [0u8; 32]
    } else {
        load_block_identity(tables, indexed_finalized_head)
            .await?
            .ok_or(Error::NotFound)?
            .hash
    };
    if blocks[0].parent_hash != expected_parent {
        return Err(Error::InvalidParent);
    }

    for pair in blocks.windows(2) {
        let current = &pair[0];
        let next = &pair[1];
        let expected_block_num = current.block_num.saturating_add(1);
        if next.block_num != expected_block_num {
            return Err(Error::InvalidSequence {
                expected: expected_block_num,
                got: next.block_num,
            });
        }
        if next.parent_hash != current.block_hash {
            return Err(Error::InvalidParent);
        }
    }

    Ok(())
}
