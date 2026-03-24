use std::collections::BTreeSet;
use std::future::Future;

use roaring::RoaringBitmap;

use crate::error::Result;
use crate::kernel::sharded_streams::{compacted_bitmap_blob, group_stream_values_into_pages};
use crate::streams::{decode_bitmap_blob, encode_bitmap_blob};

#[derive(Clone, Copy)]
pub struct StreamPageLayout {
    pub page_span: u32,
    pub local_id_mask: u64,
}

pub async fn persist_stream_fragments<PutFragment, PutFragmentFut>(
    block_num: u64,
    grouped_values: impl IntoIterator<Item = (String, u32)>,
    page_span: u32,
    mut put_fragment: PutFragment,
) -> Result<Vec<(String, u32)>>
where
    PutFragment: FnMut(String, u32, u64, bytes::Bytes) -> PutFragmentFut,
    PutFragmentFut: Future<Output = Result<()>>,
{
    let mut touched_pages = BTreeSet::<(String, u32)>::new();

    for (stream, pages) in group_stream_values_into_pages(grouped_values, page_span) {
        for (page_start, bitmap) in pages {
            let Some((_count, bitmap_blob)) = compacted_bitmap_blob(bitmap, page_start) else {
                continue;
            };

            put_fragment(
                stream.clone(),
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
    T,
    LoadFragments,
    LoadFragmentsFut,
    PutPageBlob,
    PutPageBlobFut,
    PutPageMeta,
    PutPageMetaFut,
>(
    stream_id: &str,
    page_start: u32,
    mut load_page_fragments: LoadFragments,
    mut put_page_blob: PutPageBlob,
    mut put_page_meta: PutPageMeta,
    make_meta: impl Fn(u32, u32, u32) -> T,
) -> Result<bool>
where
    LoadFragments: FnMut(String, u32) -> LoadFragmentsFut,
    LoadFragmentsFut: Future<Output = Result<Vec<bytes::Bytes>>>,
    PutPageBlob: FnMut(String, u32, bytes::Bytes) -> PutPageBlobFut,
    PutPageBlobFut: Future<Output = Result<()>>,
    PutPageMeta: FnMut(String, u32, T) -> PutPageMetaFut,
    PutPageMetaFut: Future<Output = Result<()>>,
{
    let mut merged = RoaringBitmap::new();
    let stream_id = stream_id.to_owned();
    for bytes in load_page_fragments(stream_id.clone(), page_start).await? {
        merged |= &decode_bitmap_blob(&bytes)?.bitmap;
    }
    if merged.is_empty() {
        return Ok(false);
    }

    let Some((count, bitmap_blob)) = compacted_bitmap_blob(merged, page_start) else {
        return Ok(false);
    };
    let meta = make_meta(count, bitmap_blob.min_local, bitmap_blob.max_local);

    put_page_blob(
        stream_id.clone(),
        page_start,
        encode_bitmap_blob(&bitmap_blob)?,
    )
    .await?;
    put_page_meta(stream_id, page_start, meta).await?;
    Ok(true)
}

pub async fn compact_sealed_touched_stream_pages<CompactPage, CompactPageFut>(
    touched_pages: &[(String, u32)],
    from_next_primary_id: u64,
    next_primary_id: u64,
    layout: StreamPageLayout,
    mut compact_page: CompactPage,
) -> Result<()>
where
    CompactPage: FnMut(String, u32) -> CompactPageFut,
    CompactPageFut: Future<Output = Result<bool>>,
{
    for (stream_id, page_start) in touched_pages {
        if is_page_sealed(*page_start, from_next_primary_id, next_primary_id, layout) {
            let _ = compact_page(stream_id.clone(), *page_start).await?;
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
