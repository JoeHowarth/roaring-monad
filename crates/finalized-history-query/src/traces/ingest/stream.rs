use std::collections::{BTreeMap, BTreeSet};

use roaring::RoaringBitmap;

use crate::core::ids::TraceId;
use crate::error::Result;
use crate::family::FinalizedBlock;
use crate::store::traits::{BlobStore, MetaStore};
use crate::streams::{BitmapBlob, encode_bitmap_blob};
use crate::tables::Tables;
use crate::traces::keys::{has_value_stream_id, stream_id};
use crate::traces::table_specs;
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
    let mut touched_pages = BTreeSet::<(String, u32)>::new();

    for (stream, values) in collect_trace_stream_appends(block, first_trace_id)? {
        let mut pages = BTreeMap::<u32, RoaringBitmap>::new();
        for value in values {
            let page_start = table_specs::stream_page_start_local(value);
            pages.entry(page_start).or_default().insert(value);
        }

        for (page_start, bitmap) in pages {
            let count = bitmap.len() as u32;
            let min_local = bitmap.min().unwrap_or(page_start);
            let max_local = bitmap.max().unwrap_or(page_start);
            let bitmap_blob = BitmapBlob {
                min_local,
                max_local,
                count,
                crc32: 0,
                bitmap,
            };

            tables
                .trace_bitmap_by_block()
                .put(
                    &stream,
                    page_start,
                    block.block_num,
                    encode_bitmap_blob(&bitmap_blob)?,
                )
                .await?;
            touched_pages.insert((stream.clone(), page_start));
        }
    }

    Ok(touched_pages.into_iter().collect())
}
