use crate::core::ids::{TraceId, TraceIdRange};
use crate::core::range::ResolvedBlockRange;
use crate::error::{Error, Result};
use crate::query::window::{PrimaryWindowSource, resolve_primary_window};
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;
use crate::traces::types::TraceBlockWindow;

pub async fn derive_next_trace_id<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    indexed_finalized_head: u64,
) -> Result<u64> {
    if indexed_finalized_head == 0 {
        return Ok(0);
    }

    let Some(block_record) = tables
        .trace_block_records()
        .get(indexed_finalized_head)
        .await?
    else {
        return Err(Error::NotFound);
    };
    Ok(block_record
        .first_trace_id
        .saturating_add(u64::from(block_record.count)))
}

pub async fn resolve_trace_window<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    block_range: &ResolvedBlockRange,
) -> Result<Option<TraceIdRange>> {
    resolve_primary_window(tables, block_range, &TraceWindowSource).await
}

struct TraceWindowSource;

impl PrimaryWindowSource for TraceWindowSource {
    type Id = TraceId;
    type Range = TraceIdRange;
    type Window = TraceBlockWindow;

    async fn load_block_window<M: MetaStore, B: BlobStore>(
        &self,
        tables: &Tables<M, B>,
        block_num: u64,
    ) -> Result<Option<Self::Window>> {
        Ok(tables
            .trace_block_records()
            .get(block_num)
            .await?
            .as_ref()
            .map(TraceBlockWindow::from))
    }
}
