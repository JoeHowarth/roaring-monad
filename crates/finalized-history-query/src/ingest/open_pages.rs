use bytes::Bytes;
use std::collections::BTreeSet;

use crate::core::ids::{LogId, LogShard};
use crate::domain::keys::{
    OPEN_BITMAP_PAGE_FAMILY, STREAM_PAGE_LOCAL_ID_SPAN, log_local, log_shard,
    open_bitmap_page_clustering_key, open_bitmap_page_partition_key,
    open_bitmap_page_shard_page_prefix, open_bitmap_page_shard_prefix, read_u64_be,
    stream_page_start_local,
};
use crate::error::{Error, Result};
use crate::logs::ingest::compact_stream_page;
use crate::store::traits::BlobStore;
use crate::store::traits::{DelCond, MetaStore, PutCond};

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
        let shard = log_shard(frontier_id);
        let local = log_local(frontier_id).get();
        Self {
            shard,
            page_start_local: stream_page_start_local(local),
        }
    }
}

impl OpenBitmapPage {
    pub fn is_sealed_at(&self, next_log_id: u64) -> bool {
        let frontier_id = LogId::new(next_log_id);
        let frontier_shard = log_shard(frontier_id);
        let frontier_local = log_local(frontier_id).get();
        let frontier_open_page = stream_page_start_local(frontier_local);

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

pub async fn mark_open_bitmap_page_if_absent<M: MetaStore>(
    meta_store: &M,
    page: &OpenBitmapPage,
) -> Result<()> {
    let partition = open_bitmap_page_partition_key();
    let clustering =
        open_bitmap_page_clustering_key(page.shard, page.page_start_local, &page.stream_id);
    let _ = meta_store
        .scan_put(
            OPEN_BITMAP_PAGE_FAMILY,
            &partition,
            &clustering,
            Bytes::new(),
            PutCond::IfAbsent,
        )
        .await?;
    Ok(())
}

pub async fn delete_open_bitmap_page<M: MetaStore>(
    meta_store: &M,
    page: &OpenBitmapPage,
) -> Result<()> {
    let partition = open_bitmap_page_partition_key();
    let clustering =
        open_bitmap_page_clustering_key(page.shard, page.page_start_local, &page.stream_id);
    meta_store
        .scan_delete(
            OPEN_BITMAP_PAGE_FAMILY,
            &partition,
            &clustering,
            DelCond::Any,
        )
        .await
}

pub async fn list_all_open_bitmap_pages<M: MetaStore>(
    meta_store: &M,
) -> Result<Vec<OpenBitmapPage>> {
    list_open_bitmap_pages_in_partition(meta_store, b"").await
}

pub async fn list_open_bitmap_pages_for_shard<M: MetaStore>(
    meta_store: &M,
    shard: LogShard,
) -> Result<Vec<OpenBitmapPage>> {
    list_open_bitmap_pages_in_partition(meta_store, &open_bitmap_page_shard_prefix(shard)).await
}

pub async fn list_open_bitmap_pages_for_shard_page<M: MetaStore>(
    meta_store: &M,
    shard: LogShard,
    page_start_local: u32,
) -> Result<Vec<OpenBitmapPage>> {
    list_open_bitmap_pages_in_partition(
        meta_store,
        &open_bitmap_page_shard_page_prefix(shard, page_start_local),
    )
    .await
}

async fn list_open_bitmap_pages_in_partition<M: MetaStore>(
    meta_store: &M,
    prefix: &[u8],
) -> Result<Vec<OpenBitmapPage>> {
    let partition = open_bitmap_page_partition_key();
    let mut cursor = None;
    let mut out = Vec::new();
    loop {
        let page = meta_store
            .scan_list(
                OPEN_BITMAP_PAGE_FAMILY,
                &partition,
                prefix,
                cursor.take(),
                1_024,
            )
            .await?;
        for clustering in page.keys {
            out.push(decode_open_bitmap_page_key(&partition, &clustering)?);
        }
        if page.next_cursor.is_none() {
            break;
        }
        cursor = page.next_cursor;
    }
    Ok(out)
}

pub async fn collect_newly_sealed_open_bitmap_pages<M: MetaStore>(
    meta_store: &M,
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
                    list_open_bitmap_pages_for_shard_page(meta_store, from.shard, page_start)
                        .await?,
                );
                page_start = page_start.saturating_add(STREAM_PAGE_LOCAL_ID_SPAN);
            }
        } else {
            sealed.extend(
                list_open_bitmap_pages_for_shard(meta_store, from.shard)
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
            list_open_bitmap_pages_for_shard(meta_store, from.shard)
                .await?
                .into_iter()
                .filter(|page| page.page_start_local >= from.page_start_local),
        );

        let mut shard_raw = from.shard.get().saturating_add(1);
        while shard_raw < to.shard.get() {
            let shard =
                LogShard::new(shard_raw).map_err(|_| Error::Decode("invalid shard range"))?;
            sealed.extend(list_open_bitmap_pages_for_shard(meta_store, shard).await?);
            shard_raw = shard_raw.saturating_add(1);
        }

        if to.page_start_local > 0 {
            sealed.extend(
                list_open_bitmap_pages_for_shard(meta_store, to.shard)
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

pub async fn repair_open_bitmap_page_markers<M: MetaStore, B: BlobStore>(
    meta_store: &M,
    blob_store: &B,
    next_log_id: u64,
) -> Result<()> {
    for page in list_all_open_bitmap_pages(meta_store)
        .await?
        .into_iter()
        .filter(|page| page.is_sealed_at(next_log_id))
    {
        let _ = compact_stream_page(
            meta_store,
            blob_store,
            &page.stream_id,
            page.page_start_local,
        )
        .await?;
        delete_open_bitmap_page(meta_store, &page).await?;
    }
    Ok(())
}

pub fn decode_open_bitmap_page_key(partition: &[u8], clustering: &[u8]) -> Result<OpenBitmapPage> {
    if !partition.is_empty() {
        return Err(Error::Decode("invalid open_bitmap_page partition"));
    }
    let min_len = 8 + 1 + 8 + 1;
    if clustering.len() < min_len {
        return Err(Error::Decode("invalid open_bitmap_page clustering"));
    }
    if clustering[8] != b'/' || clustering[17] != b'/' {
        return Err(Error::Decode("invalid open_bitmap_page separators"));
    }

    let shard = read_u64_be(&clustering[..8])
        .and_then(|raw| LogShard::new(raw).ok())
        .ok_or(Error::Decode("invalid open_bitmap_page shard"))?;
    let page_start_local = read_u64_be(&clustering[9..17])
        .and_then(|raw| u32::try_from(raw).ok())
        .ok_or(Error::Decode("invalid open_bitmap_page page"))?;
    let stream_id = String::from_utf8(clustering[18..].to_vec())
        .map_err(|_| Error::Decode("invalid open_bitmap_page stream id"))?;

    Ok(OpenBitmapPage {
        shard,
        page_start_local,
        stream_id,
    })
}
