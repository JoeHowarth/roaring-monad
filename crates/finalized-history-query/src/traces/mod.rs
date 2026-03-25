pub(crate) mod codec;
pub mod filter;
pub(crate) mod ingest;
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
use crate::ingest::indexed_family::finalize_indexed_family_ingest;
use crate::runtime::Runtime;
use crate::store::traits::{BlobStore, MetaStore};
use crate::traces::ingest::{
    persist_trace_artifacts, persist_trace_stream_fragments, plan_trace_ingest,
};
use crate::traces::types::StreamBitmapMeta;

pub use filter::TraceFilter;
pub use types::{Trace, TraceSequencingState};
pub use view::TraceRef;

pub(crate) const TRACE_STREAM_PAGE_LOCAL_ID_SPAN: u32 = 4_096;

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

    /// Persists one finalized block's trace artifacts, updates trace-side
    /// indexing state, and seals any directories or bitmap pages that closed.
    pub async fn ingest_block<M: MetaStore, B: BlobStore>(
        &self,
        _config: &Config,
        runtime: &Runtime<M, B>,
        state: &mut TraceSequencingState,
        block: &FinalizedBlock,
    ) -> Result<usize> {
        let from_next_trace_id = state.next_trace_id.get();
        let ingest_plan = plan_trace_ingest(&block.trace_rlp, from_next_trace_id)?;
        let trace_count =
            persist_trace_artifacts(&runtime.tables, block.block_num, &ingest_plan).await?;
        let trace_count_u32 =
            u32::try_from(trace_count).map_err(|_| Error::Decode("trace count overflow"))?;

        let touched_pages = persist_trace_stream_fragments(
            &runtime.tables,
            block.block_num,
            &ingest_plan.grouped_stream_values,
        )
        .await?;
        let next_trace_id = finalize_indexed_family_ingest(
            &runtime.tables.trace_dir,
            &runtime.tables.trace_streams,
            &runtime.tables.trace_open_bitmap_pages,
            block.block_num,
            from_next_trace_id,
            trace_count_u32,
            touched_pages,
            TRACE_STREAM_PAGE_LOCAL_ID_SPAN,
            |count, min_local, max_local| StreamBitmapMeta {
                count,
                min_local,
                max_local,
            },
        )
        .await?;

        state.next_trace_id = TraceId::new(next_trace_id);
        Ok(trace_count)
    }
}
