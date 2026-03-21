use crate::block::FinalizedBlock;
use crate::config::Config;
use crate::core::ids::LogId;
use crate::core::state::derive_next_log_id;
use crate::error::Result;
use crate::family::Family;
use crate::ingest::open_pages::{
    OpenBitmapPage, collect_newly_sealed_open_bitmap_pages, delete_open_bitmap_page,
    mark_open_bitmap_page_if_absent,
};
use crate::logs::ingest::{
    compact_newly_sealed_directory, compact_stream_page, parse_stream_shard, persist_log_artifacts,
    persist_log_block_record, persist_log_dir_by_block, persist_stream_fragments,
};
use crate::logs::types::LogSequencingState;
use crate::runtime::Runtime;
use crate::store::traits::{BlobStore, MetaStore};

#[derive(Debug, Clone, Copy, Default)]
pub struct LogsFamily;

impl<M: MetaStore, B: BlobStore> Family<M, B> for LogsFamily {
    type State = LogSequencingState;

    async fn load_startup_state(
        &self,
        runtime: &Runtime<M, B>,
        indexed_finalized_head: u64,
    ) -> Result<Self::State> {
        let next_log_id = derive_next_log_id(runtime.tables(), indexed_finalized_head).await?;
        Ok(LogSequencingState {
            next_log_id: LogId::new(next_log_id),
        })
    }

    async fn ingest_block(
        &self,
        config: &Config,
        runtime: &Runtime<M, B>,
        state: &mut Self::State,
        block: &FinalizedBlock,
    ) -> Result<usize> {
        let from_next_log_id = state.next_log_id.get();
        let mut opened_during = Vec::<OpenBitmapPage>::new();

        persist_log_artifacts(
            config,
            runtime.tables(),
            block.block_num,
            &block.logs,
            from_next_log_id,
        )
        .await?;
        persist_log_block_record(
            runtime.tables(),
            runtime.meta_store(),
            block,
            from_next_log_id,
        )
        .await?;
        persist_log_dir_by_block(
            runtime.tables(),
            block.block_num,
            from_next_log_id,
            block.logs.len() as u32,
        )
        .await?;
        let touched_pages =
            persist_stream_fragments(runtime.tables(), block, from_next_log_id).await?;
        opened_during.extend(
            touched_pages
                .into_iter()
                .filter_map(|(stream_id, page_start)| {
                    parse_stream_shard(&stream_id).map(|shard| OpenBitmapPage {
                        shard,
                        page_start_local: page_start,
                        stream_id,
                    })
                }),
        );

        let next_log_id = from_next_log_id.saturating_add(block.logs.len() as u64);
        for page in opened_during
            .iter()
            .filter(|page| !page.is_sealed_at(next_log_id))
        {
            mark_open_bitmap_page_if_absent(runtime.meta_store(), page).await?;
        }

        compact_newly_sealed_directory(runtime.tables(), from_next_log_id, next_log_id).await?;

        for page in collect_newly_sealed_open_bitmap_pages(
            runtime.meta_store(),
            &opened_during,
            from_next_log_id,
            next_log_id,
        )
        .await?
        {
            let _ = compact_stream_page(runtime.tables(), &page.stream_id, page.page_start_local)
                .await?;
            delete_open_bitmap_page(runtime.meta_store(), &page).await?;
        }

        state.next_log_id = LogId::new(next_log_id);
        Ok(block.logs.len())
    }
}
