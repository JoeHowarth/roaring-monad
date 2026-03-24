use futures::stream::{FuturesUnordered, StreamExt};
use roaring::RoaringBitmap;

use crate::error::Result;
use crate::kernel::sharded_streams::merge_bitmap_bytes_into;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;

use super::planner::PreparedClause;
use super::stream_family::StreamIndexFamily;

pub(crate) const STREAM_LOAD_CONCURRENCY: usize = 32;

pub(crate) async fn load_prepared_clause_bitmap<
    M: MetaStore,
    B: BlobStore,
    K,
    F: StreamIndexFamily<ClauseKind = K>,
>(
    tables: &Tables<M, B>,
    prepared_clause: &PreparedClause<K>,
    local_from: u32,
    local_to: u32,
) -> Result<RoaringBitmap> {
    let mut out = RoaringBitmap::new();
    let mut in_flight = FuturesUnordered::new();

    for stream_id in &prepared_clause.stream_ids {
        in_flight.push(async move {
            load_stream_entries::<M, B, F>(tables, stream_id, local_from, local_to).await
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

pub(crate) async fn load_stream_entries<M: MetaStore, B: BlobStore, F: StreamIndexFamily>(
    tables: &Tables<M, B>,
    stream: &str,
    local_from: u32,
    local_to: u32,
) -> Result<RoaringBitmap> {
    let mut out = RoaringBitmap::new();
    let mut page_start = F::first_page_start(local_from);
    let last_page_start = F::last_page_start(local_to);
    let stream_tables = F::stream_tables(tables);

    loop {
        if let Some(meta) = stream_tables.get_page_meta(stream, page_start).await? {
            if F::meta_overlaps(&meta, local_from, local_to) {
                let loaded_page_blob = maybe_merge_cached_bitmap_blob::<M, B, F>(
                    tables, stream, page_start, &mut out, local_from, local_to,
                )
                .await?;
                if !loaded_page_blob {
                    load_bitmap_by_block_entries_for_page::<M, B, F>(
                        tables, stream, page_start, local_from, local_to, &mut out,
                    )
                    .await?;
                }
            }
        } else {
            load_bitmap_by_block_entries_for_page::<M, B, F>(
                tables, stream, page_start, local_from, local_to, &mut out,
            )
            .await?;
        }

        if page_start == last_page_start {
            break;
        }
        page_start = F::next_page_start(page_start);
    }

    Ok(out)
}

async fn load_bitmap_by_block_entries_for_page<M: MetaStore, B: BlobStore, F: StreamIndexFamily>(
    tables: &Tables<M, B>,
    stream: &str,
    page_start: u32,
    local_from: u32,
    local_to: u32,
    out: &mut RoaringBitmap,
) -> Result<()> {
    for bytes in F::stream_tables(tables)
        .load_page_fragments(stream, page_start)
        .await?
    {
        let _ = merge_bitmap_bytes_into(
            &bytes,
            out,
            local_from,
            local_to,
            F::is_full_shard_range(local_from, local_to),
        )?;
    }
    Ok(())
}

async fn maybe_merge_cached_bitmap_blob<M: MetaStore, B: BlobStore, F: StreamIndexFamily>(
    tables: &Tables<M, B>,
    stream: &str,
    page_start: u32,
    out: &mut RoaringBitmap,
    local_from: u32,
    local_to: u32,
) -> Result<bool> {
    let Some(bytes) = F::stream_tables(tables)
        .get_page_blob(stream, page_start)
        .await?
    else {
        return Ok(false);
    };
    merge_bitmap_bytes_into(
        &bytes,
        out,
        local_from,
        local_to,
        F::is_full_shard_range(local_from, local_to),
    )
}
