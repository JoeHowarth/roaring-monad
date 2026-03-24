use std::collections::BTreeSet;

use crate::core::ids::FamilyId;
use crate::core::layout::read_u64_be;
use crate::error::{Error, Result};
use crate::ingest::bitmap_pages;
use crate::kernel::sharded_streams::page_start_local;
use crate::logs::keys::STREAM_PAGE_LOCAL_ID_SPAN;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::{OpenBitmapPageTable, Tables};
use crate::traces::keys::TRACE_STREAM_PAGE_LOCAL_ID_SPAN;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct OpenBitmapPage {
    pub shard: u64,
    pub page_start_local: u32,
    pub stream_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FrontierPosition {
    shard: u64,
    page_start_local: u32,
}

impl FrontierPosition {
    fn from_next_primary_id(next_primary_id: u64, page_span: u32) -> Self {
        let id = FamilyId::new(next_primary_id);
        Self {
            shard: id.shard_raw(),
            page_start_local: page_start_local(id.local_raw(), page_span),
        }
    }
}

impl OpenBitmapPage {
    pub fn is_sealed_at(&self, next_primary_id: u64, page_span: u32) -> bool {
        let id = FamilyId::new(next_primary_id);
        let frontier_shard = id.shard_raw();
        let frontier_local = id.local_raw();
        let frontier_open_page = page_start_local(frontier_local, page_span);

        if self.shard < frontier_shard {
            return true;
        }
        if self.shard > frontier_shard {
            return false;
        }

        self.page_start_local < frontier_open_page
            || (next_primary_id > 0
                && self.page_start_local.saturating_add(page_span) <= frontier_local)
    }
}

pub async fn collect_newly_sealed_open_bitmap_pages<M: MetaStore>(
    table: &OpenBitmapPageTable<M>,
    opened_during: &[OpenBitmapPage],
    from_next_primary_id: u64,
    to_next_primary_id: u64,
    page_span: u32,
) -> Result<Vec<OpenBitmapPage>> {
    let mut sealed = opened_during
        .iter()
        .filter(|page| page.is_sealed_at(to_next_primary_id, page_span))
        .cloned()
        .collect::<BTreeSet<_>>();

    if to_next_primary_id <= from_next_primary_id {
        return Ok(sealed.into_iter().collect());
    }

    let from = FrontierPosition::from_next_primary_id(from_next_primary_id, page_span);
    let to = FrontierPosition::from_next_primary_id(to_next_primary_id, page_span);

    if from.shard == to.shard {
        let affected_pages =
            (to.page_start_local.saturating_sub(from.page_start_local)) / page_span;
        if affected_pages <= 32 {
            let mut ps = from.page_start_local;
            while ps < to.page_start_local {
                sealed.extend(table.list_for_shard_page(from.shard, ps).await?);
                ps = ps.saturating_add(page_span);
            }
        } else {
            sealed.extend(
                table
                    .list_for_shard(from.shard)
                    .await?
                    .into_iter()
                    .filter(|page| {
                        page.page_start_local >= from.page_start_local
                            && page.page_start_local < to.page_start_local
                    }),
            );
        }
    } else {
        sealed.extend(
            table
                .list_for_shard(from.shard)
                .await?
                .into_iter()
                .filter(|page| page.page_start_local >= from.page_start_local),
        );

        let mut shard_raw = from.shard.saturating_add(1);
        while shard_raw < to.shard {
            sealed.extend(table.list_for_shard(shard_raw).await?);
            shard_raw = shard_raw.saturating_add(1);
        }

        if to.page_start_local > 0 {
            sealed.extend(
                table
                    .list_for_shard(to.shard)
                    .await?
                    .into_iter()
                    .filter(|page| page.page_start_local < to.page_start_local),
            );
        }
    }

    Ok(sealed
        .into_iter()
        .filter(|page| page.is_sealed_at(to_next_primary_id, page_span))
        .collect())
}

/// Scan the tracking table for markers that are sealed at the current frontier
/// and return them. Used during ownership-transition recovery to repair stale
/// markers left by a crashed writer.
pub async fn collect_all_sealed_open_bitmap_pages<M: MetaStore>(
    table: &OpenBitmapPageTable<M>,
    next_primary_id: u64,
    page_span: u32,
) -> Result<Vec<OpenBitmapPage>> {
    let frontier = FrontierPosition::from_next_primary_id(next_primary_id, page_span);
    let mut sealed = Vec::new();

    for shard in 0..=frontier.shard {
        let pages = table.list_for_shard(shard).await?;
        sealed.extend(
            pages
                .into_iter()
                .filter(|page| page.is_sealed_at(next_primary_id, page_span)),
        );
    }

    Ok(sealed)
}

/// Repair stale open bitmap page markers after an ownership transition. For
/// each family, scan the tracking table for markers that are sealed at the
/// current frontier, compact them (idempotent), and delete the markers.
pub async fn repair_sealed_open_bitmap_pages<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    next_log_id: u64,
    next_trace_id: u64,
) -> Result<()> {
    for page in collect_all_sealed_open_bitmap_pages(
        &tables.log_open_bitmap_pages,
        next_log_id,
        STREAM_PAGE_LOCAL_ID_SPAN,
    )
    .await?
    {
        let _ = bitmap_pages::compact_stream_page(
            &tables.log_streams,
            &page.stream_id,
            page.page_start_local,
            |count, min_local, max_local| crate::logs::types::StreamBitmapMeta {
                count,
                min_local,
                max_local,
            },
        )
        .await?;
        tables.log_open_bitmap_pages.delete(&page).await?;
    }

    for page in collect_all_sealed_open_bitmap_pages(
        &tables.trace_open_bitmap_pages,
        next_trace_id,
        TRACE_STREAM_PAGE_LOCAL_ID_SPAN,
    )
    .await?
    {
        let _ = bitmap_pages::compact_stream_page(
            &tables.trace_streams,
            &page.stream_id,
            page.page_start_local,
            |count, min_local, max_local| crate::traces::types::StreamBitmapMeta {
                count,
                min_local,
                max_local,
            },
        )
        .await?;
        tables.trace_open_bitmap_pages.delete(&page).await?;
    }

    Ok(())
}

pub(crate) fn decode_open_bitmap_page_key(
    partition: &[u8],
    clustering: &[u8],
) -> Result<OpenBitmapPage> {
    if partition.len() != 8 {
        return Err(Error::Decode("invalid open_bitmap_page partition"));
    }
    let min_len = 8 + 1;
    if clustering.len() < min_len {
        return Err(Error::Decode("invalid open_bitmap_page clustering"));
    }
    if clustering[8] != b'/' {
        return Err(Error::Decode("invalid open_bitmap_page separator"));
    }

    let shard = read_u64_be(partition).ok_or(Error::Decode("invalid open_bitmap_page shard"))?;
    let page_start_local = read_u64_be(&clustering[..8])
        .and_then(|raw| u32::try_from(raw).ok())
        .ok_or(Error::Decode("invalid open_bitmap_page page"))?;
    let stream_id = String::from_utf8(clustering[9..].to_vec())
        .map_err(|_| Error::Decode("invalid open_bitmap_page stream id"))?;

    Ok(OpenBitmapPage {
        shard,
        page_start_local,
        stream_id,
    })
}
