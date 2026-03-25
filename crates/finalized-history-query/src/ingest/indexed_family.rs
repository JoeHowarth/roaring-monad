use crate::error::Result;
use crate::ingest::bitmap_pages;
use crate::ingest::open_pages::{OpenBitmapPage, collect_newly_sealed_open_bitmap_pages};
use crate::ingest::primary_dir::compact_newly_sealed_primary_directory;
use crate::kernel::codec::StorageCodec;
use crate::kernel::sharded_streams::parse_stream_shard;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::{OpenBitmapPageTable, PrimaryDirTables, StreamTables};

pub async fn finalize_indexed_family_ingest<M, B, T>(
    dir: &PrimaryDirTables<M>,
    streams: &StreamTables<M, B, T>,
    open_bitmap_pages: &OpenBitmapPageTable<M>,
    block_num: u64,
    from_next_primary_id: u64,
    written_count: u32,
    touched_pages: Vec<(String, u32)>,
    stream_page_local_id_span: u32,
    make_meta: impl Fn(u32, u32, u32) -> T + Copy,
) -> Result<u64>
where
    M: MetaStore,
    B: BlobStore,
    T: StorageCodec,
{
    dir.persist_block_fragment(block_num, from_next_primary_id, written_count)
        .await?;

    let next_primary_id = from_next_primary_id.saturating_add(u64::from(written_count));
    let opened_during = touched_pages
        .into_iter()
        .filter_map(|(stream_id, page_start_local)| {
            parse_stream_shard(&stream_id).map(|shard| OpenBitmapPage {
                shard,
                page_start_local,
                stream_id,
            })
        })
        .collect::<Vec<_>>();

    for page in opened_during
        .iter()
        .filter(|page| !page.is_sealed_at(next_primary_id, stream_page_local_id_span))
    {
        open_bitmap_pages.mark_if_absent(page).await?;
    }

    compact_newly_sealed_primary_directory(dir, from_next_primary_id, next_primary_id).await?;

    for page in collect_newly_sealed_open_bitmap_pages(
        open_bitmap_pages,
        &opened_during,
        from_next_primary_id,
        next_primary_id,
        stream_page_local_id_span,
    )
    .await?
    {
        let _ = bitmap_pages::compact_stream_page(
            streams,
            &page.stream_id,
            page.page_start_local,
            make_meta,
        )
        .await?;
        open_bitmap_pages.delete(&page).await?;
    }

    Ok(next_primary_id)
}
