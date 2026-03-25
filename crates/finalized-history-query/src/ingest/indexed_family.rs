use std::collections::BTreeMap;

use crate::error::Result;
use crate::ingest::bitmap_pages;
use crate::ingest::open_pages::{OpenBitmapPage, collect_newly_sealed_open_bitmap_pages};
use crate::ingest::primary_dir::compact_newly_sealed_primary_directory;
use crate::kernel::codec::StorageCodec;
use crate::kernel::sharded_streams::group_stream_values;
use crate::kernel::sharded_streams::parse_stream_shard;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::{OpenBitmapPageTable, PrimaryDirTables, StreamTables};

pub fn primary_id_at_offset(first_primary_id: u64, offset: usize) -> u64 {
    first_primary_id + offset as u64
}

pub struct IndexedFamilyTables<'a, M: MetaStore, B: BlobStore, T> {
    pub dir: &'a PrimaryDirTables<M>,
    pub streams: &'a StreamTables<M, B, T>,
    pub open_bitmap_pages: &'a OpenBitmapPageTable<M>,
}

pub struct IndexedFamilyIngestArtifacts<T> {
    pub block_num: u64,
    pub from_next_primary_id: u64,
    pub written_count: u32,
    pub touched_pages: Vec<(String, u32)>,
    pub stream_page_local_id_span: u32,
    pub make_meta: fn(u32, u32, u32) -> T,
}

impl<T> IndexedFamilyIngestArtifacts<T> {
    pub fn next_primary_id(&self) -> u64 {
        self.from_next_primary_id
            .saturating_add(u64::from(self.written_count))
    }
}

pub struct IndexedFamilyFinalizeResult {
    pub next_primary_id: u64,
}

pub fn collect_grouped_stream_appends<Item, F>(
    first_primary_id: u64,
    items: impl IntoIterator<Item = Item>,
    mut stream_entries_for_item: F,
) -> Result<BTreeMap<String, Vec<u32>>>
where
    F: FnMut(&Item, u64) -> Result<Vec<(String, u32)>>,
{
    let mut stream_values = Vec::new();

    for (offset, item) in items.into_iter().enumerate() {
        let primary_id = primary_id_at_offset(first_primary_id, offset);
        stream_values.extend(stream_entries_for_item(&item, primary_id)?);
    }

    Ok(group_stream_values(stream_values))
}

pub fn iter_grouped_stream_appends(
    grouped_values: &BTreeMap<String, Vec<u32>>,
) -> impl Iterator<Item = (String, u32)> + '_ {
    grouped_values
        .iter()
        .flat_map(|(stream, values)| values.iter().copied().map(|value| (stream.clone(), value)))
}

pub async fn finalize_indexed_family_ingest<M, B, T>(
    tables: IndexedFamilyTables<'_, M, B, T>,
    artifacts: IndexedFamilyIngestArtifacts<T>,
) -> Result<IndexedFamilyFinalizeResult>
where
    M: MetaStore,
    B: BlobStore,
    T: StorageCodec,
{
    tables
        .dir
        .persist_block_fragment(
            artifacts.block_num,
            artifacts.from_next_primary_id,
            artifacts.written_count,
        )
        .await?;

    let next_primary_id = artifacts.next_primary_id();
    let opened_during = artifacts
        .touched_pages
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
        .filter(|page| !page.is_sealed_at(next_primary_id, artifacts.stream_page_local_id_span))
    {
        tables.open_bitmap_pages.mark_if_absent(page).await?;
    }

    compact_newly_sealed_primary_directory(
        tables.dir,
        artifacts.from_next_primary_id,
        next_primary_id,
    )
    .await?;

    for page in collect_newly_sealed_open_bitmap_pages(
        tables.open_bitmap_pages,
        &opened_during,
        artifacts.from_next_primary_id,
        next_primary_id,
        artifacts.stream_page_local_id_span,
    )
    .await?
    {
        let _ = bitmap_pages::compact_stream_page(
            tables.streams,
            &page.stream_id,
            page.page_start_local,
            artifacts.make_meta,
        )
        .await?;
        tables.open_bitmap_pages.delete(&page).await?;
    }

    Ok(IndexedFamilyFinalizeResult { next_primary_id })
}
