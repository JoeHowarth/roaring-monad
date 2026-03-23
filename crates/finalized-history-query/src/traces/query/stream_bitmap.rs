use bytes::Bytes;
use futures::stream::{FuturesUnordered, StreamExt};
use roaring::RoaringBitmap;

use super::clause::PreparedShardClause;
use super::clause::is_full_shard_range;
use crate::error::Result;
use crate::store::traits::{BlobStore, MetaStore};
use crate::streams::decode_bitmap_blob;
use crate::tables::Tables;
use crate::traces::keys::TRACE_STREAM_PAGE_LOCAL_ID_SPAN;
use crate::traces::table_specs;

pub(in crate::traces) const STREAM_LOAD_CONCURRENCY: usize = 32;

pub(in crate::traces) async fn load_prepared_clause_bitmap<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    prepared_clause: &PreparedShardClause,
    local_from: u32,
    local_to: u32,
) -> Result<RoaringBitmap> {
    let mut out = RoaringBitmap::new();
    let mut in_flight = FuturesUnordered::new();

    for stream_id in &prepared_clause.stream_ids {
        in_flight.push(async move {
            load_stream_entries(tables, stream_id, local_from, local_to).await
        });
        if in_flight.len() >= STREAM_LOAD_CONCURRENCY
            && let Some(result) = in_flight.next().await
        {
            out |= &result?;
        }
    }

    while let Some(result) = in_flight.next().await {
        out |= &result?;
    }

    Ok(out)
}

pub(in crate::traces) async fn load_stream_entries<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    stream: &str,
    local_from: u32,
    local_to: u32,
) -> Result<RoaringBitmap> {
    let mut out = RoaringBitmap::new();
    let mut page_start = table_specs::stream_page_start_local(local_from);
    let last_page_start = table_specs::stream_page_start_local(local_to);

    loop {
        if let Some(meta) = load_trace_bitmap_page_meta(tables, stream, page_start).await? {
            if overlaps(meta.min_local, meta.max_local, local_from, local_to) {
                let loaded_page_blob = maybe_merge_cached_bitmap_blob(
                    tables, stream, page_start, &mut out, local_from, local_to,
                )
                .await?;
                if !loaded_page_blob {
                    load_bitmap_by_block_entries_for_page(
                        tables, stream, page_start, local_from, local_to, &mut out,
                    )
                    .await?;
                }
            }
        } else {
            load_bitmap_by_block_entries_for_page(
                tables, stream, page_start, local_from, local_to, &mut out,
            )
            .await?;
        }

        if page_start == last_page_start {
            break;
        }
        page_start = page_start.saturating_add(TRACE_STREAM_PAGE_LOCAL_ID_SPAN);
    }

    Ok(out)
}

async fn load_bitmap_by_block_entries_for_page<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    stream: &str,
    page_start: u32,
    local_from: u32,
    local_to: u32,
    out: &mut RoaringBitmap,
) -> Result<()> {
    for bytes in tables
        .trace_bitmap_by_block()
        .load_page_fragments(stream, page_start)
        .await?
    {
        let bitmap_blob = decode_bitmap_blob(&bytes)?;
        if !overlaps(
            bitmap_blob.min_local,
            bitmap_blob.max_local,
            local_from,
            local_to,
        ) {
            continue;
        }
        if is_full_shard_range(local_from, local_to)
            || (bitmap_blob.min_local >= local_from && bitmap_blob.max_local <= local_to)
        {
            *out |= &bitmap_blob.bitmap;
            continue;
        }
        for value in bitmap_blob.bitmap {
            if value >= local_from && value <= local_to {
                out.insert(value);
            }
        }
    }
    Ok(())
}

pub(in crate::traces) async fn load_trace_bitmap_page_meta<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    stream: &str,
    page_start: u32,
) -> Result<Option<crate::traces::types::StreamBitmapMeta>> {
    tables
        .trace_bitmap_page_meta()
        .get(stream, page_start)
        .await
}

async fn load_bitmap_page_blob<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    stream: &str,
    page_start: u32,
) -> Result<Option<Bytes>> {
    tables
        .trace_bitmap_page_blobs()
        .get_for_page(stream, page_start)
        .await
}

async fn maybe_merge_cached_bitmap_blob<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    stream: &str,
    page_start: u32,
    out: &mut RoaringBitmap,
    local_from: u32,
    local_to: u32,
) -> Result<bool> {
    let Some(bytes) = load_bitmap_page_blob(tables, stream, page_start).await? else {
        return Ok(false);
    };
    let bitmap_blob = decode_bitmap_blob(&bytes)?;
    if is_full_shard_range(local_from, local_to)
        || (bitmap_blob.min_local >= local_from && bitmap_blob.max_local <= local_to)
    {
        *out |= &bitmap_blob.bitmap;
        return Ok(true);
    }

    for value in bitmap_blob.bitmap {
        if value >= local_from && value <= local_to {
            out.insert(value);
        }
    }
    Ok(true)
}

pub(in crate::traces) fn overlaps(
    min_local: u32,
    max_local: u32,
    local_from: u32,
    local_to: u32,
) -> bool {
    min_local <= local_to && max_local >= local_from
}
