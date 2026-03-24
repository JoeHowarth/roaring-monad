use crate::core::ids::{TraceId, TraceIdRange};
use crate::core::range::ResolvedBlockRange;
use crate::error::{Error, Result};
use crate::query::window::resolve_primary_window;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;

pub async fn derive_next_trace_id<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    indexed_finalized_head: u64,
) -> Result<u64> {
    if indexed_finalized_head == 0 {
        return Ok(0);
    }

    let Some(block_record) = tables.block_records.get(indexed_finalized_head).await? else {
        return Err(Error::NotFound);
    };
    let Some(window) = block_record.traces else {
        return Err(Error::NotFound);
    };
    Ok(window
        .first_primary_id
        .saturating_add(u64::from(window.count)))
}

pub async fn resolve_trace_window<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    block_range: &ResolvedBlockRange,
) -> Result<Option<TraceIdRange>> {
    resolve_primary_window::<_, _, TraceId, _>(tables, block_range, |record| record.traces).await
}
