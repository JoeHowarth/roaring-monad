use std::collections::{BTreeMap, BTreeSet};

use crate::core::ids::TraceId;
use crate::error::Result;
use crate::family::FinalizedBlock;
use crate::ingest::bitmap_pages;
use crate::kernel::sharded_streams::sharded_stream_id;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;
use crate::traces::keys::TRACE_STREAM_PAGE_LOCAL_ID_SPAN;
use crate::traces::view::BlockTraceIter;

pub fn collect_trace_stream_appends(
    block: &FinalizedBlock,
    first_trace_id: u64,
) -> Result<BTreeMap<String, Vec<u32>>> {
    let mut out: BTreeMap<String, BTreeSet<u32>> = BTreeMap::new();

    for (index, iterated) in BlockTraceIter::new(&block.trace_rlp)?.enumerate() {
        let iterated = iterated?;
        let global_trace_id = TraceId::new(first_trace_id + index as u64);
        let shard = global_trace_id.shard();
        let local = global_trace_id.local().get();
        let view = iterated.view;

        out.entry(sharded_stream_id("from", view.from_addr()?, shard.get()))
            .or_default()
            .insert(local);

        if let Some(to_addr) = view.to_addr()? {
            out.entry(sharded_stream_id("to", to_addr, shard.get()))
                .or_default()
                .insert(local);
        }

        if let Some(selector) = view.selector()? {
            out.entry(sharded_stream_id("selector", selector, shard.get()))
                .or_default()
                .insert(local);
        }

        if view.has_value()? {
            out.entry(sharded_stream_id("has_value", b"\x01", shard.get()))
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
        tables.trace_streams(),
        block.block_num,
        grouped_values,
        TRACE_STREAM_PAGE_LOCAL_ID_SPAN,
    )
    .await
}
