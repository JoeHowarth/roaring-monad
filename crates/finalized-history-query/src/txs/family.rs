use crate::core::ids::TxId;
use crate::core::state::BlockRecord;
use crate::error::{Error, Result};
use crate::family::FinalizedBlock;
use crate::ingest::indexed_family::{
    IndexedFamilyFinalizeResult, IndexedFamilyIngestArtifacts, IndexedFamilyTables,
    finalize_indexed_family_ingest,
};
use crate::runtime::Runtime;
use crate::store::traits::{BlobStore, MetaStore};
use crate::txs::TX_STREAM_PAGE_LOCAL_ID_SPAN;
use crate::txs::ingest::{persist_stream_fragments, persist_tx_artifacts, plan_tx_ingest};
use crate::txs::types::{StreamBitmapMeta, TxFamilyState};

#[derive(Debug, Clone, Copy, Default)]
pub struct TxsFamily;

impl TxsFamily {
    pub fn load_state_from_head_record(
        &self,
        head_record: Option<&BlockRecord>,
    ) -> Result<TxFamilyState> {
        let next_tx_id = match head_record {
            None => 0,
            Some(block_record) => {
                let window = block_record.txs.ok_or(Error::NotFound)?;
                window
                    .first_primary_id
                    .saturating_add(u64::from(window.count))
            }
        };
        Ok(TxFamilyState {
            next_tx_id: TxId::new(next_tx_id),
        })
    }

    pub async fn ingest_block<M: MetaStore, B: BlobStore>(
        &self,
        runtime: &Runtime<M, B>,
        state: &mut TxFamilyState,
        block: &FinalizedBlock,
    ) -> Result<usize> {
        let from_next_tx_id = state.next_tx_id.get();
        let plan = plan_tx_ingest(block, from_next_tx_id)?;
        let tx_count = persist_tx_artifacts(&runtime.tables, block.block_num, &plan).await?;
        let tx_count_u32 =
            u32::try_from(tx_count).map_err(|_| Error::Decode("tx count overflow"))?;
        let touched_pages = persist_stream_fragments(
            &runtime.tables,
            block.block_num,
            &plan.stream_appends_by_stream,
        )
        .await?;
        let IndexedFamilyFinalizeResult { next_primary_id } = finalize_indexed_family_ingest(
            IndexedFamilyTables {
                dir: &runtime.tables.tx_dir,
                streams: &runtime.tables.tx_streams,
                open_bitmap_pages: &runtime.tables.tx_open_bitmap_pages,
            },
            IndexedFamilyIngestArtifacts {
                block_num: block.block_num,
                from_next_primary_id: from_next_tx_id,
                written_count: tx_count_u32,
                touched_pages,
                stream_page_local_id_span: TX_STREAM_PAGE_LOCAL_ID_SPAN,
                make_meta: |count, min_local, max_local| StreamBitmapMeta {
                    count,
                    min_local,
                    max_local,
                },
            },
        )
        .await?;

        state.next_tx_id = TxId::new(next_primary_id);
        Ok(tx_count)
    }
}
