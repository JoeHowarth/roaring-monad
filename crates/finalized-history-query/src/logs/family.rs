use crate::core::ids::LogId;
use crate::core::state::BlockRecord;
use crate::error::{Error, Result};
use crate::family::FinalizedBlock;
use crate::ingest::indexed_family::{
    IndexedFamilyFinalizeResult, IndexedFamilyIngestArtifacts, IndexedFamilyTables,
    finalize_indexed_family_ingest,
};
use crate::logs::STREAM_PAGE_LOCAL_ID_SPAN;
use crate::logs::ingest::{persist_log_artifacts, persist_log_stream_fragments, plan_log_ingest};
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
        runtime: &Runtime<M, B>,
        state: &mut LogSequencingState,
        block: &FinalizedBlock,
    ) -> Result<usize> {
        let from_next_log_id = state.next_log_id.get();
        let plan = plan_log_ingest(block, from_next_log_id)?;

        let written_count = persist_log_artifacts(&runtime.tables, block.block_num, &plan).await?;

        let touched_pages = persist_log_stream_fragments(
            &runtime.tables,
            block.block_num,
            &plan.stream_appends_by_stream,
        )
        .await?;
        let IndexedFamilyFinalizeResult { next_primary_id } = finalize_indexed_family_ingest(
            IndexedFamilyTables {
                dir: &runtime.tables.log_dir,
                streams: &runtime.tables.log_streams,
                open_bitmap_pages: &runtime.tables.log_open_bitmap_pages,
            },
            IndexedFamilyIngestArtifacts {
                block_num: block.block_num,
                from_next_primary_id: from_next_log_id,
                written_count: written_count as u32,
                touched_pages,
                stream_page_local_id_span: STREAM_PAGE_LOCAL_ID_SPAN,
                make_meta: |count, min_local, max_local| StreamBitmapMeta {
                    count,
                    min_local,
                    max_local,
                },
            },
        )
        .await?;

        state.next_log_id = LogId::new(next_primary_id);
        Ok(written_count)
    }
}
