use std::collections::BTreeSet;

use crate::core::ids::{LogId, LogShard};
use crate::core::layout::read_u64_be;
use crate::error::{Error, Result};
use crate::kernel::sharded_streams::page_start_local;
use crate::logs::keys::STREAM_PAGE_LOCAL_ID_SPAN;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct OpenBitmapPage {
    pub shard: LogShard,
    pub page_start_local: u32,
    pub stream_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FrontierPosition {
    shard: LogShard,
    page_start_local: u32,
}

impl FrontierPosition {
    fn from_next_log_id(next_log_id: u64) -> Self {
        let frontier_id = LogId::new(next_log_id);
        let shard = frontier_id.shard();
        let local = frontier_id.local().get();
        Self {
            shard,
            page_start_local: page_start_local(local, STREAM_PAGE_LOCAL_ID_SPAN),
        }
    }
}

impl OpenBitmapPage {
    pub fn is_sealed_at(&self, next_log_id: u64) -> bool {
        let frontier_id = LogId::new(next_log_id);
        let frontier_shard = frontier_id.shard();
        let frontier_local = frontier_id.local().get();
        let frontier_open_page = page_start_local(frontier_local, STREAM_PAGE_LOCAL_ID_SPAN);

        if self.shard < frontier_shard {
            return true;
        }
        if self.shard > frontier_shard {
            return false;
        }

        self.page_start_local < frontier_open_page
            || (next_log_id > 0
                && self
                    .page_start_local
                    .saturating_add(STREAM_PAGE_LOCAL_ID_SPAN)
                    <= frontier_local)
    }
}

pub async fn mark_open_bitmap_page_if_absent<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    page: &OpenBitmapPage,
) -> Result<()> {
    tables.open_bitmap_pages.mark_if_absent(page).await
}

pub async fn delete_open_bitmap_page<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    page: &OpenBitmapPage,
) -> Result<()> {
    tables.open_bitmap_pages.delete(page).await
}

pub async fn list_open_bitmap_pages_for_shard<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    shard: LogShard,
) -> Result<Vec<OpenBitmapPage>> {
    tables.open_bitmap_pages.list_for_shard(shard).await
}

pub async fn list_open_bitmap_pages_for_shard_page<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    shard: LogShard,
    page_start_local: u32,
) -> Result<Vec<OpenBitmapPage>> {
    tables
        .open_bitmap_pages
        .list_for_shard_page(shard, page_start_local)
        .await
}

pub async fn collect_newly_sealed_open_bitmap_pages<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    opened_during: &[OpenBitmapPage],
    from_next_log_id: u64,
    to_next_log_id: u64,
) -> Result<Vec<OpenBitmapPage>> {
    let mut sealed = opened_during
        .iter()
        .filter(|page| page.is_sealed_at(to_next_log_id))
        .cloned()
        .collect::<BTreeSet<_>>();

    if to_next_log_id <= from_next_log_id {
        return Ok(sealed.into_iter().collect());
    }

    let from = FrontierPosition::from_next_log_id(from_next_log_id);
    let to = FrontierPosition::from_next_log_id(to_next_log_id);

    if from.shard == to.shard {
        let affected_pages =
            (to.page_start_local.saturating_sub(from.page_start_local)) / STREAM_PAGE_LOCAL_ID_SPAN;
        if affected_pages <= 32 {
            let mut page_start = from.page_start_local;
            while page_start < to.page_start_local {
                sealed.extend(
                    list_open_bitmap_pages_for_shard_page(tables, from.shard, page_start).await?,
                );
                page_start = page_start.saturating_add(STREAM_PAGE_LOCAL_ID_SPAN);
            }
        } else {
            sealed.extend(
                list_open_bitmap_pages_for_shard(tables, from.shard)
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
            list_open_bitmap_pages_for_shard(tables, from.shard)
                .await?
                .into_iter()
                .filter(|page| page.page_start_local >= from.page_start_local),
        );

        let mut shard_raw = from.shard.get().saturating_add(1);
        while shard_raw < to.shard.get() {
            let shard =
                LogShard::new(shard_raw).map_err(|_| Error::Decode("invalid shard range"))?;
            sealed.extend(list_open_bitmap_pages_for_shard(tables, shard).await?);
            shard_raw = shard_raw.saturating_add(1);
        }

        if to.page_start_local > 0 {
            sealed.extend(
                list_open_bitmap_pages_for_shard(tables, to.shard)
                    .await?
                    .into_iter()
                    .filter(|page| page.page_start_local < to.page_start_local),
            );
        }
    }

    Ok(sealed
        .into_iter()
        .filter(|page| page.is_sealed_at(to_next_log_id))
        .collect())
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

    let shard = read_u64_be(partition)
        .and_then(|raw| LogShard::new(raw).ok())
        .ok_or(Error::Decode("invalid open_bitmap_page shard"))?;
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
