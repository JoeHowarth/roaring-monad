use bytes::Bytes;
use futures::stream::{FuturesUnordered, StreamExt};
use roaring::RoaringBitmap;

use crate::error::Result;
use crate::kernel::sharded_streams::merge_bitmap_bytes_into;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;

use super::planner::PreparedClause;

pub(crate) const STREAM_LOAD_CONCURRENCY: usize = 32;

pub(crate) trait StreamBitmapLoader {
    type BitmapMeta;

    fn is_full_shard_range(&self, local_from: u32, local_to: u32) -> bool;
    fn first_page_start(&self, local_from: u32) -> u32;
    fn last_page_start(&self, local_to: u32) -> u32;
    fn next_page_start(&self, page_start: u32) -> u32;
    fn meta_overlaps(&self, meta: &Self::BitmapMeta, local_from: u32, local_to: u32) -> bool;

    async fn load_bitmap_page_meta<M: MetaStore, B: BlobStore>(
        &self,
        tables: &Tables<M, B>,
        stream: &str,
        page_start: u32,
    ) -> Result<Option<Self::BitmapMeta>>;

    async fn load_bitmap_page_blob<M: MetaStore, B: BlobStore>(
        &self,
        tables: &Tables<M, B>,
        stream: &str,
        page_start: u32,
    ) -> Result<Option<Bytes>>;

    async fn load_page_fragments<M: MetaStore, B: BlobStore>(
        &self,
        tables: &Tables<M, B>,
        stream: &str,
        page_start: u32,
    ) -> Result<Vec<Bytes>>;
}

pub(crate) async fn load_prepared_clause_bitmap<
    M: MetaStore,
    B: BlobStore,
    K,
    L: StreamBitmapLoader,
>(
    tables: &Tables<M, B>,
    loader: &L,
    prepared_clause: &PreparedClause<K>,
    local_from: u32,
    local_to: u32,
) -> Result<RoaringBitmap> {
    let mut out = RoaringBitmap::new();
    let mut in_flight = FuturesUnordered::new();

    for stream_id in &prepared_clause.stream_ids {
        in_flight.push(async move {
            load_stream_entries(tables, loader, stream_id, local_from, local_to).await
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

pub(crate) async fn load_stream_entries<M: MetaStore, B: BlobStore, L: StreamBitmapLoader>(
    tables: &Tables<M, B>,
    loader: &L,
    stream: &str,
    local_from: u32,
    local_to: u32,
) -> Result<RoaringBitmap> {
    let mut out = RoaringBitmap::new();
    let mut page_start = loader.first_page_start(local_from);
    let last_page_start = loader.last_page_start(local_to);

    loop {
        if let Some(meta) = loader
            .load_bitmap_page_meta(tables, stream, page_start)
            .await?
        {
            if loader.meta_overlaps(&meta, local_from, local_to) {
                let loaded_page_blob = maybe_merge_cached_bitmap_blob(
                    tables, loader, stream, page_start, &mut out, local_from, local_to,
                )
                .await?;
                if !loaded_page_blob {
                    load_bitmap_by_block_entries_for_page(
                        tables, loader, stream, page_start, local_from, local_to, &mut out,
                    )
                    .await?;
                }
            }
        } else {
            load_bitmap_by_block_entries_for_page(
                tables, loader, stream, page_start, local_from, local_to, &mut out,
            )
            .await?;
        }

        if page_start == last_page_start {
            break;
        }
        page_start = loader.next_page_start(page_start);
    }

    Ok(out)
}

async fn load_bitmap_by_block_entries_for_page<
    M: MetaStore,
    B: BlobStore,
    L: StreamBitmapLoader,
>(
    tables: &Tables<M, B>,
    loader: &L,
    stream: &str,
    page_start: u32,
    local_from: u32,
    local_to: u32,
    out: &mut RoaringBitmap,
) -> Result<()> {
    for bytes in loader
        .load_page_fragments(tables, stream, page_start)
        .await?
    {
        let _ = merge_bitmap_bytes_into(
            &bytes,
            out,
            local_from,
            local_to,
            loader.is_full_shard_range(local_from, local_to),
        )?;
    }
    Ok(())
}

async fn maybe_merge_cached_bitmap_blob<M: MetaStore, B: BlobStore, L: StreamBitmapLoader>(
    tables: &Tables<M, B>,
    loader: &L,
    stream: &str,
    page_start: u32,
    out: &mut RoaringBitmap,
    local_from: u32,
    local_to: u32,
) -> Result<bool> {
    let Some(bytes) = loader
        .load_bitmap_page_blob(tables, stream, page_start)
        .await?
    else {
        return Ok(false);
    };
    merge_bitmap_bytes_into(
        &bytes,
        out,
        local_from,
        local_to,
        loader.is_full_shard_range(local_from, local_to),
    )
}
