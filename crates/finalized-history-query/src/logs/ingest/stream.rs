use std::collections::{BTreeMap, BTreeSet};

use roaring::RoaringBitmap;

use crate::block::FinalizedBlock;
use crate::core::ids::LogId;
use crate::error::Result;
use crate::logs::table_specs;
use crate::logs::types::StreamBitmapMeta;
use crate::store::traits::{BlobStore, MetaStore};
use crate::streams::{BitmapBlob, decode_bitmap_blob, encode_bitmap_blob};
use crate::tables::Tables;

pub fn collect_stream_appends(
    block: &FinalizedBlock,
    first_log_id: u64,
) -> BTreeMap<String, Vec<u32>> {
    let mut out: BTreeMap<String, BTreeSet<u32>> = BTreeMap::new();

    for (index, log) in block.logs.iter().enumerate() {
        let global_log_id = LogId::new(first_log_id + index as u64);
        let shard = table_specs::log_shard(global_log_id);
        let local = table_specs::log_local(global_log_id).get();

        out.entry(table_specs::stream_id("addr", &log.address, shard))
            .or_default()
            .insert(local);

        if let Some(topic0) = log.topics.first() {
            out.entry(table_specs::stream_id("topic0", topic0, shard))
                .or_default()
                .insert(local);
        }

        for (topic_index, topic) in log.topics.iter().enumerate().skip(1).take(3) {
            let kind = match topic_index {
                1 => "topic1",
                2 => "topic2",
                3 => "topic3",
                _ => continue,
            };
            out.entry(table_specs::stream_id(kind, topic, shard))
                .or_default()
                .insert(local);
        }
    }

    out.into_iter()
        .map(|(stream, values)| (stream, values.into_iter().collect()))
        .collect()
}

pub async fn persist_stream_fragments<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    block: &FinalizedBlock,
    first_log_id: u64,
) -> Result<Vec<(String, u32)>> {
    let mut touched_pages = BTreeSet::<(String, u32)>::new();

    for (stream, values) in collect_stream_appends(block, first_log_id) {
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
                .bitmap_by_block()
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

pub async fn compact_sealed_stream_pages<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    sealed_pages: &[(String, u32)],
) -> Result<()> {
    if sealed_pages.is_empty() {
        return Ok(());
    }

    for (stream_id, page_start) in sealed_pages {
        compact_stream_page(tables, stream_id, *page_start).await?;
    }

    Ok(())
}

pub async fn compact_stream_page<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    stream_id: &str,
    page_start: u32,
) -> Result<bool> {
    let mut merged = RoaringBitmap::new();
    for bytes in tables
        .bitmap_by_block()
        .load_page_fragments(stream_id, page_start)
        .await?
    {
        merged |= &decode_bitmap_blob(&bytes)?.bitmap;
    }
    if merged.is_empty() {
        return Ok(false);
    }

    let count = merged.len() as u32;
    let min_local = merged.min().unwrap_or(page_start);
    let max_local = merged.max().unwrap_or(page_start);
    let meta = StreamBitmapMeta {
        block_num: 0,
        count,
        min_local,
        max_local,
    };
    let bitmap_blob = BitmapBlob {
        min_local,
        max_local,
        count,
        crc32: 0,
        bitmap: merged,
    };

    tables
        .bitmap_page_blobs()
        .put(stream_id, page_start, encode_bitmap_blob(&bitmap_blob)?)
        .await?;
    tables
        .bitmap_page_meta()
        .put(stream_id, page_start, &meta)
        .await?;
    Ok(true)
}
