pub(crate) mod codec;
pub mod filter;
pub(crate) mod ingest;
pub mod keys;
pub(crate) mod materialize;
pub(crate) mod query;
pub mod table_specs;
pub mod types;
pub mod view;

use crate::config::Config;
use crate::core::ids::TraceId;
use crate::core::state::BlockRecord;
use crate::error::{Error, Result};
use crate::family::FinalizedBlock;
use crate::ingest::bitmap_pages;
use crate::ingest::open_pages::{
    OpenBitmapPage, collect_newly_sealed_open_bitmap_pages, delete_open_bitmap_page,
    mark_open_bitmap_page_if_absent,
};
use crate::ingest::primary_dir::compact_newly_sealed_primary_directory;
use crate::kernel::sharded_streams::parse_stream_shard;
use crate::runtime::Runtime;
use crate::store::traits::{BlobStore, MetaStore};
use crate::traces::ingest::{
    persist_trace_artifacts, persist_trace_dir_by_block, persist_trace_stream_fragments,
};
use crate::traces::keys::TRACE_STREAM_PAGE_LOCAL_ID_SPAN;
use crate::traces::types::StreamBitmapMeta;

pub use filter::TraceFilter;
pub use types::{Trace, TraceSequencingState};

#[derive(Debug, Clone, Copy, Default)]
pub struct TracesFamily;

impl TracesFamily {
    pub fn load_state_from_head_record(
        &self,
        head_record: Option<&BlockRecord>,
    ) -> Result<TraceSequencingState> {
        let next_trace_id = match head_record {
            None => 0,
            Some(block_record) => {
                let window = block_record.traces.ok_or(Error::NotFound)?;
                window
                    .first_primary_id
                    .saturating_add(u64::from(window.count))
            }
        };
        Ok(TraceSequencingState {
            next_trace_id: TraceId::new(next_trace_id),
        })
    }

    pub async fn ingest_block<M: MetaStore, B: BlobStore>(
        &self,
        _config: &Config,
        runtime: &Runtime<M, B>,
        state: &mut TraceSequencingState,
        block: &FinalizedBlock,
    ) -> Result<usize> {
        let from_next_trace_id = state.next_trace_id.get();
        let trace_count =
            persist_trace_artifacts(&runtime.tables, block.block_num, &block.trace_rlp).await?;
        let trace_count_u32 =
            u32::try_from(trace_count).map_err(|_| Error::Decode("trace count overflow"))?;

        persist_trace_dir_by_block(
            &runtime.tables,
            block.block_num,
            from_next_trace_id,
            trace_count_u32,
        )
        .await?;

        let next_trace_id = from_next_trace_id + trace_count as u64;

        let touched_pages =
            persist_trace_stream_fragments(&runtime.tables, block, from_next_trace_id).await?;

        let mut opened_during = Vec::<OpenBitmapPage>::new();
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

        for page in opened_during
            .iter()
            .filter(|page| !page.is_sealed_at(next_trace_id, TRACE_STREAM_PAGE_LOCAL_ID_SPAN))
        {
            mark_open_bitmap_page_if_absent(&runtime.tables.trace_open_bitmap_pages, page).await?;
        }

        compact_newly_sealed_primary_directory(
            &runtime.tables.trace_dir,
            from_next_trace_id,
            next_trace_id,
        )
        .await?;

        for page in collect_newly_sealed_open_bitmap_pages(
            &runtime.tables.trace_open_bitmap_pages,
            &opened_during,
            from_next_trace_id,
            next_trace_id,
            TRACE_STREAM_PAGE_LOCAL_ID_SPAN,
        )
        .await?
        {
            let _ = bitmap_pages::compact_stream_page(
                &runtime.tables.trace_streams,
                &page.stream_id,
                page.page_start_local,
                |count, min_local, max_local| StreamBitmapMeta {
                    count,
                    min_local,
                    max_local,
                },
            )
            .await?;
            delete_open_bitmap_page(&runtime.tables.trace_open_bitmap_pages, &page).await?;
        }

        state.next_trace_id = TraceId::new(next_trace_id);
        Ok(trace_count)
    }
}
