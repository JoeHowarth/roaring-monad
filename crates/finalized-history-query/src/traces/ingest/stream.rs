use std::collections::{BTreeMap, BTreeSet};

use crate::core::ids::TraceId;
use crate::error::Result;
use crate::family::FinalizedBlock;
use crate::ingest::bitmap_pages::{self, StreamPageLayout};
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;
use crate::traces::keys::{
    TRACE_LOCAL_ID_MASK, TRACE_STREAM_PAGE_LOCAL_ID_SPAN, has_value_stream_id, stream_id,
};
use crate::traces::table_specs;
use crate::traces::types::StreamBitmapMeta;
use crate::traces::view::BlockTraceIter;

pub fn collect_trace_stream_appends(
    block: &FinalizedBlock,
    first_trace_id: u64,
) -> Result<BTreeMap<String, Vec<u32>>> {
    let mut out: BTreeMap<String, BTreeSet<u32>> = BTreeMap::new();

    for (index, iterated) in BlockTraceIter::new(&block.trace_rlp)?.enumerate() {
        let iterated = iterated?;
        let global_trace_id = TraceId::new(first_trace_id + index as u64);
        let shard = table_specs::trace_shard(global_trace_id);
        let local = table_specs::trace_local(global_trace_id).get();
        let view = iterated.view;

        out.entry(stream_id("from", view.from_addr()?, shard))
            .or_default()
            .insert(local);

        if let Some(to_addr) = view.to_addr()? {
            out.entry(stream_id("to", to_addr, shard))
                .or_default()
                .insert(local);
        }

        if let Some(selector) = view.selector()? {
            out.entry(stream_id("selector", selector, shard))
                .or_default()
                .insert(local);
        }

        if view.has_value()? {
            out.entry(has_value_stream_id(shard))
                .or_default()
                .insert(local);
        }
    }

    Ok(out
        .into_iter()
        .map(|(stream, values)| (stream, values.into_iter().collect()))
        .collect())
}

pub async fn persist_trace_stream_fragments<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    block: &FinalizedBlock,
    first_trace_id: u64,
) -> Result<Vec<(String, u32)>> {
    let grouped_values = collect_trace_stream_appends(block, first_trace_id)?
        .into_iter()
        .flat_map(|(stream, values)| values.into_iter().map(move |value| (stream.clone(), value)));
    bitmap_pages::persist_stream_fragments(
        block.block_num,
        grouped_values,
        TRACE_STREAM_PAGE_LOCAL_ID_SPAN,
        |stream, page_start, block_num, bytes| async move {
            tables
                .trace_bitmap_by_block()
                .put(&stream, page_start, block_num, bytes)
                .await
        },
    )
    .await
}

pub async fn compact_sealed_trace_stream_pages<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    touched_pages: &[(String, u32)],
    from_next_trace_id: u64,
    next_trace_id: u64,
) -> Result<()> {
    bitmap_pages::compact_sealed_touched_stream_pages(
        touched_pages,
        from_next_trace_id,
        next_trace_id,
        StreamPageLayout {
            page_span: TRACE_STREAM_PAGE_LOCAL_ID_SPAN,
            local_id_mask: TRACE_LOCAL_ID_MASK,
        },
        |stream_id, page_start| async move {
            compact_trace_stream_page(tables, &stream_id, page_start).await
        },
    )
    .await
}

pub async fn compact_trace_stream_page<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    stream_id: &str,
    page_start: u32,
) -> Result<bool> {
    bitmap_pages::compact_stream_page(
        stream_id,
        page_start,
        |stream, page_start| async move {
            tables
                .trace_bitmap_by_block()
                .load_page_fragments(&stream, page_start)
                .await
        },
        |stream, page_start, bytes| async move {
            tables
                .trace_bitmap_page_blobs()
                .put(&stream, page_start, bytes)
                .await
        },
        |stream, page_start, meta| async move {
            tables
                .trace_bitmap_page_meta()
                .put(&stream, page_start, &meta)
                .await
        },
        |count, min_local, max_local| StreamBitmapMeta {
            block_num: 0,
            count,
            min_local,
            max_local,
        },
    )
    .await
}
