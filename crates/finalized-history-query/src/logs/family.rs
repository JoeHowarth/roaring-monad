use crate::config::Config;
use crate::core::ids::LogId;
use crate::core::state::BlockRecord;
use crate::error::{Error, Result};
use crate::family::FinalizedBlock;
use crate::ingest::bitmap_pages;
use crate::ingest::open_pages::{OpenBitmapPage, collect_newly_sealed_open_bitmap_pages};
use crate::ingest::primary_dir::compact_newly_sealed_primary_directory;
use crate::kernel::sharded_streams::parse_stream_shard;
use crate::logs::ingest::{
    persist_log_artifacts, persist_log_dir_by_block, persist_stream_fragments,
};
use crate::logs::keys::STREAM_PAGE_LOCAL_ID_SPAN;
use crate::logs::types::{LogSequencingState, StreamBitmapMeta};
use crate::runtime::Runtime;
use crate::store::traits::{BlobStore, MetaStore};

#[derive(Debug, Clone, Copy, Default)]
pub struct LogsFamily;

impl LogsFamily {
    pub fn load_state_from_head_record(
        &self,
        head_record: Option<&BlockRecord>,
    ) -> Result<LogSequencingState> {
        let next_log_id = match head_record {
            None => 0,
            Some(block_record) => {
                let window = block_record.logs.ok_or(Error::NotFound)?;
                window
                    .first_primary_id
                    .saturating_add(u64::from(window.count))
            }
        };
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
            runtime
                .tables
                .log_open_bitmap_pages
                .mark_if_absent(page)
                .await?;
        }

        compact_newly_sealed_primary_directory(
            &runtime.tables.log_dir,
            from_next_log_id,
            next_log_id,
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
            runtime.tables.log_open_bitmap_pages.delete(&page).await?;
        }

        state.next_log_id = LogId::new(next_log_id);
        Ok(block.logs.len())
    }
}
