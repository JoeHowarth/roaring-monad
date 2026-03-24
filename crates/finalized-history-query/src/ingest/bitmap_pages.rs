use std::collections::BTreeSet;

use roaring::RoaringBitmap;

use crate::error::Result;
use crate::kernel::sharded_streams::{compacted_bitmap_blob, group_stream_values_into_pages};
use crate::store::traits::{BlobStore, MetaStore};
use crate::streams::{decode_bitmap_blob, encode_bitmap_blob};
use crate::tables::StreamTables;

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
