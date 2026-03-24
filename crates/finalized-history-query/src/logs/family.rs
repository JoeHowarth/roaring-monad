use crate::config::Config;
use crate::core::ids::LogId;
use crate::core::state::derive_next_log_id;
use crate::error::Result;
use crate::family::FinalizedBlock;
use crate::ingest::bitmap_pages;
use crate::ingest::open_pages::{
    OpenBitmapPage, collect_newly_sealed_open_bitmap_pages, delete_open_bitmap_page,
    mark_open_bitmap_page_if_absent,
};
use crate::ingest::primary_dir::compact_newly_sealed_primary_directory;
use crate::kernel::sharded_streams::parse_stream_shard;
use crate::logs::ingest::{
    persist_log_artifacts, persist_log_dir_by_block, persist_stream_fragments,
};
use crate::logs::keys::{LOG_PRIMARY_DIR_LAYOUT, STREAM_PAGE_LOCAL_ID_SPAN};
use crate::logs::types::LogSequencingState;
use crate::logs::types::StreamBitmapMeta;
use crate::runtime::Runtime;
use crate::store::traits::{BlobStore, MetaStore};

#[derive(Debug, Clone, Copy, Default)]
pub struct LogsFamily;

impl LogsFamily {
    pub async fn load_startup_state<M: MetaStore, B: BlobStore>(
        &self,
        runtime: &Runtime<M, B>,
        indexed_finalized_head: u64,
    ) -> Result<LogSequencingState> {
        let next_log_id = derive_next_log_id(&runtime.tables, indexed_finalized_head).await?;
        Ok(LogSequencingState {
            next_log_id: LogId::new(next_log_id),
        })
    }

    pub async fn ingest_block<M: MetaStore, B: BlobStore>(
        &self,
        config: &Config,
        runtime: &Runtime<M, B>,
        state: &mut LogSequencingState,
        block: &FinalizedBlock,
    ) -> Result<usize> {
        let from_next_log_id = state.next_log_id.get();
        let mut opened_during = Vec::<OpenBitmapPage>::new();

        persist_log_artifacts(
            config,
            &runtime.tables,
            block.block_num,
            &block.logs,
            from_next_log_id,
        )
        .await?;
        persist_log_dir_by_block(
            &runtime.tables,
            block.block_num,
            from_next_log_id,
            block.logs.len() as u32,
        )
        .await?;
        let touched_pages =
            persist_stream_fragments(&runtime.tables, block, from_next_log_id).await?;
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
            .filter(|page| !page.is_sealed_at(next_log_id, STREAM_PAGE_LOCAL_ID_SPAN))
        {
            mark_open_bitmap_page_if_absent(&runtime.tables.log_open_bitmap_pages, page).await?;
        }

        compact_newly_sealed_primary_directory(
            &runtime.tables.log_dir,
            from_next_log_id,
            next_log_id,
            LOG_PRIMARY_DIR_LAYOUT,
        )
        .await?;

        for page in collect_newly_sealed_open_bitmap_pages(
            &runtime.tables.log_open_bitmap_pages,
            &opened_during,
            from_next_log_id,
            next_log_id,
            STREAM_PAGE_LOCAL_ID_SPAN,
        )
        .await?
        {
            let _ = bitmap_pages::compact_stream_page(
                &runtime.tables.log_streams,
                &page.stream_id,
                page.page_start_local,
                |count, min_local, max_local| StreamBitmapMeta {
                    count,
                    min_local,
                    max_local,
                },
            )
            .await?;
            delete_open_bitmap_page(&runtime.tables.log_open_bitmap_pages, &page).await?;
        }

        state.next_log_id = LogId::new(next_log_id);
        Ok(block.logs.len())
    }
}
