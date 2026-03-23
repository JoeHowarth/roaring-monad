use crate::core::ids::{TraceId, TraceIdRange};
use crate::core::range::ResolvedBlockRange;
use crate::error::{Error, Result};
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;
use crate::traces::types::{TraceBlockRecord, TraceBlockWindow};

pub async fn load_trace_block_record<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    block_num: u64,
) -> Result<Option<TraceBlockRecord>> {
    tables.trace_block_records().get(block_num).await
}

pub async fn load_trace_block_window<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    block_num: u64,
) -> Result<Option<TraceBlockWindow>> {
    Ok(load_trace_block_record(tables, block_num)
        .await?
        .as_ref()
        .map(TraceBlockWindow::from))
}

pub async fn derive_next_trace_id<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    indexed_finalized_head: u64,
) -> Result<u64> {
    if indexed_finalized_head == 0 {
        return Ok(0);
    }

    let Some(block_record) = tables.trace_block_records().get(indexed_finalized_head).await? else {
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
    if block_range.is_empty() {
        return Ok(None);
    }

    let Some(from_block_window) = load_trace_block_window(tables, block_range.from_block).await?
    else {
        return Ok(None);
    };
    let Some(to_block_window) = load_trace_block_window(tables, block_range.to_block).await? else {
        return Ok(None);
    };

    let start = from_block_window.first_trace_id;
    let end_exclusive = TraceId::new(
        to_block_window
            .first_trace_id
            .get()
            .saturating_add(to_block_window.count as u64),
    );
    if start.get() >= end_exclusive.get() {
        return Ok(None);
    }

    Ok(TraceIdRange::new(
        TraceId::new(start.get()),
        TraceId::new(end_exclusive.get().saturating_sub(1)),
    ))
}
