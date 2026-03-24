use std::collections::BTreeSet;

use roaring::RoaringBitmap;

use crate::error::Result;
use crate::kernel::sharded_streams::{compacted_bitmap_blob, group_stream_values_into_pages};
use crate::store::traits::{BlobStore, MetaStore};
use crate::streams::{decode_bitmap_blob, encode_bitmap_blob};
use crate::tables::StreamTables;

#[derive(Clone, Copy)]
pub struct StreamPageLayout {
    pub page_span: u32,
    pub local_id_mask: u64,
}

pub async fn persist_stream_fragments<
    M: MetaStore,
    B: BlobStore,
    T: crate::kernel::codec::StorageCodec,
>(
    tables: &StreamTables<M, B, T>,
    block_num: u64,
    grouped_values: impl IntoIterator<Item = (String, u32)>,
    page_span: u32,
) -> Result<Vec<(String, u32)>> {
    let mut touched_pages = BTreeSet::<(String, u32)>::new();

    for (stream, pages) in group_stream_values_into_pages(grouped_values, page_span) {
        for (page_start, bitmap) in pages {
            let Some((_count, bitmap_blob)) = compacted_bitmap_blob(bitmap, page_start) else {
                continue;
            };

            tables
                .put_fragment(
                    &stream,
                    page_start,
                    block_num,
                    encode_bitmap_blob(&bitmap_blob)?,
                )
                .await?;
            touched_pages.insert((stream.clone(), page_start));
        }
    }

    Ok(touched_pages.into_iter().collect())
}

pub async fn compact_stream_page<
    M: MetaStore,
    B: BlobStore,
    T: crate::kernel::codec::StorageCodec,
>(
    tables: &StreamTables<M, B, T>,
    stream_id: &str,
    page_start: u32,
    make_meta: impl Fn(u32, u32, u32) -> T,
) -> Result<bool> {
    let mut merged = RoaringBitmap::new();
    for bytes in tables.load_page_fragments(stream_id, page_start).await? {
        merged |= &decode_bitmap_blob(&bytes)?.bitmap;
    }
    if merged.is_empty() {
        return Ok(false);
    }

    let Some((count, bitmap_blob)) = compacted_bitmap_blob(merged, page_start) else {
        return Ok(false);
    };
    let meta = make_meta(count, bitmap_blob.min_local, bitmap_blob.max_local);

    tables
        .put_page_blob(stream_id, page_start, encode_bitmap_blob(&bitmap_blob)?)
        .await?;
    tables.put_page_meta(stream_id, page_start, &meta).await?;
    Ok(true)
}

pub async fn compact_sealed_touched_stream_pages<
    M: MetaStore,
    B: BlobStore,
    T: crate::kernel::codec::StorageCodec,
>(
    tables: &StreamTables<M, B, T>,
    touched_pages: &[(String, u32)],
    from_next_primary_id: u64,
    next_primary_id: u64,
    layout: StreamPageLayout,
    make_meta: impl Fn(u32, u32, u32) -> T + Copy,
) -> Result<()> {
    for (stream_id, page_start) in touched_pages {
        if is_page_sealed(*page_start, from_next_primary_id, next_primary_id, layout) {
            let _ = compact_stream_page(tables, stream_id, *page_start, make_meta).await?;
        }
    }
    Ok(())
}

pub fn is_page_sealed(
    page_start_local: u32,
    from_next_primary_id: u64,
    next_primary_id: u64,
    layout: StreamPageLayout,
) -> bool {
    let page_end_local = page_start_local.saturating_add(layout.page_span);
    let from_local = (from_next_primary_id & layout.local_id_mask) as u32;
    let to_local = (next_primary_id & layout.local_id_mask) as u32;

    to_local >= page_end_local || to_local < from_local
}
