use crate::config::Config;
use crate::core::ids::LogId;
use crate::core::state::BlockRecord;
use crate::error::{Error, Result};
use crate::family::FinalizedBlock;
use crate::ingest::indexed_family::finalize_indexed_family_ingest;
use crate::logs::STREAM_PAGE_LOCAL_ID_SPAN;
use crate::logs::ingest::{persist_log_artifacts, persist_stream_fragments};
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

    /// Persists one finalized block's logs, updates log-side indexing state,
    /// and seals any directories or bitmap pages that became complete in the process.
    pub async fn ingest_block<M: MetaStore, B: BlobStore>(
        &self,
        config: &Config,
        runtime: &Runtime<M, B>,
        state: &mut LogSequencingState,
        block: &FinalizedBlock,
    ) -> Result<usize> {
        let from_next_log_id = state.next_log_id.get();

        persist_log_artifacts(
            config,
            &runtime.tables,
            block.block_num,
            &block.logs,
            from_next_log_id,
        )
        .await?;

        let touched_pages =
            persist_stream_fragments(&runtime.tables, block, from_next_log_id).await?;
        let next_log_id = finalize_indexed_family_ingest(
            &runtime.tables.log_dir,
            &runtime.tables.log_streams,
            &runtime.tables.log_open_bitmap_pages,
            block.block_num,
            from_next_log_id,
            block.logs.len() as u32,
            touched_pages,
            STREAM_PAGE_LOCAL_ID_SPAN,
            |count, min_local, max_local| StreamBitmapMeta {
                count,
                min_local,
                max_local,
            },
        )
        .await?;

        state.next_log_id = LogId::new(next_log_id);
        Ok(block.logs.len())
    }
}
