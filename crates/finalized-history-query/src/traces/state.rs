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
    if block_range.is_empty() {
        return Ok(None);
    }

    let Some(start) =
        first_trace_id_in_range(tables, block_range.from_block, block_range.to_block).await?
    else {
        return Ok(None);
    };
    let Some(end_exclusive) =
        end_trace_id_exclusive_in_range(tables, block_range.from_block, block_range.to_block)
            .await?
    else {
        return Ok(None);
    };

    Ok(TraceIdRange::new(
        start,
        TraceId::new(end_exclusive.get().saturating_sub(1)),
    ))
}

async fn first_trace_id_in_range<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    from_block: u64,
    to_block: u64,
) -> Result<Option<TraceId>> {
    let mut block_num = from_block;
    while block_num <= to_block {
        let Some(window) = load_trace_block_window(tables, block_num).await? else {
            return Ok(None);
        };
        if window.count > 0 {
            return Ok(Some(window.first_trace_id));
        }
        block_num = block_num.saturating_add(1);
    }
    Ok(None)
}

async fn end_trace_id_exclusive_in_range<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    from_block: u64,
    to_block: u64,
) -> Result<Option<TraceId>> {
    let mut block_num = to_block;
    loop {
        let Some(window) = load_trace_block_window(tables, block_num).await? else {
            return Ok(None);
        };
        if window.count > 0 {
            return Ok(Some(TraceId::new(
                window
                    .first_trace_id
                    .get()
                    .saturating_add(window.count as u64),
            )));
        }
        if block_num == from_block {
            break;
        }
        block_num = block_num.saturating_sub(1);
    }
    Ok(None)
}
