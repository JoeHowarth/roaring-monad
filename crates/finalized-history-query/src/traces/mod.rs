pub(crate) mod codec;
pub mod filter;
pub(crate) mod ingest;
pub mod keys;
pub(crate) mod materialize;
pub(crate) mod query;
pub(crate) mod state;
pub(crate) mod table_specs;
pub mod types;
pub mod view;

use crate::config::Config;
use crate::core::ids::TraceId;
use crate::error::{Error, Result};
use crate::family::FinalizedBlock;
use crate::runtime::Runtime;
use crate::store::traits::{BlobStore, MetaStore};
use crate::traces::ingest::{
    compact_newly_sealed_trace_directory, compact_sealed_trace_stream_pages,
    persist_trace_artifacts, persist_trace_block_record, persist_trace_dir_by_block,
    persist_trace_stream_fragments,
};

pub use filter::TraceFilter;
pub use types::{Trace, TraceSequencingState, TraceStartupState};

#[derive(Debug, Clone, Copy, Default)]
pub struct TracesFamily;

impl TracesFamily {
    pub async fn load_startup_state<M: MetaStore, B: BlobStore>(
        &self,
        runtime: &Runtime<M, B>,
        indexed_finalized_head: u64,
    ) -> Result<TraceStartupState> {
        let next_trace_id = if indexed_finalized_head == 0 {
            0
        } else {
            state::derive_next_trace_id(runtime.tables(), indexed_finalized_head).await?
        };
        Ok(TraceSequencingState {
            next_trace_id: TraceId::new(next_trace_id),
        })
    }

    pub async fn ingest_block<M: MetaStore, B: BlobStore>(
        &self,
        _config: &Config,
        runtime: &Runtime<M, B>,
        state: &mut TraceStartupState,
        block: &FinalizedBlock,
    ) -> Result<usize> {
        let from_next_trace_id = state.next_trace_id.get();
        let trace_count =
            persist_trace_artifacts(runtime.tables(), block.block_num, &block.trace_rlp).await?;
        let trace_count_u32 =
            u32::try_from(trace_count).map_err(|_| Error::Decode("trace count overflow"))?;

        persist_trace_block_record(runtime.tables(), block, from_next_trace_id, trace_count_u32)
            .await?;
        persist_trace_dir_by_block(
            runtime.tables(),
            block.block_num,
            from_next_trace_id,
            trace_count_u32,
        )
        .await?;

        let next_trace_id = from_next_trace_id + trace_count as u64;

        let touched_pages =
            persist_trace_stream_fragments(runtime.tables(), block, from_next_trace_id).await?;
        compact_newly_sealed_trace_directory(runtime.tables(), from_next_trace_id, next_trace_id)
            .await?;
        compact_sealed_trace_stream_pages(
            runtime.tables(),
            &touched_pages,
            from_next_trace_id,
            next_trace_id,
        )
        .await?;

        state.next_trace_id = TraceId::new(next_trace_id);
        Ok(trace_count)
    }
}
