use bytes::Bytes;

use crate::core::ids::{LogId, LogShard};
use crate::domain::keys::{
    STREAM_PAGE_LOCAL_ID_SPAN, log_local, log_shard, open_stream_page_key, open_stream_page_prefix,
    read_u64_be, stream_page_start_local,
};
use crate::error::{Error, Result};
use crate::logs::ingest::compact_stream_page;
use crate::store::traits::BlobStore;
use crate::store::traits::{DelCond, FenceToken, MetaStore, PutCond};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct OpenStreamPage {
    pub shard: LogShard,
    pub page_start_local: u32,
    pub stream_id: String,
}

impl OpenStreamPage {
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

pub async fn mark_open_stream_page_if_absent<M: MetaStore>(
    meta_store: &M,
    page: &OpenStreamPage,
    writer_id: u64,
) -> Result<()> {
    let _ = meta_store
        .put(
            &open_stream_page_key(page.shard, page.page_start_local, &page.stream_id),
            Bytes::new(),
            PutCond::IfAbsent,
            FenceToken(writer_id),
        )
        .await?;
    Ok(())
}

pub async fn delete_open_stream_page<M: MetaStore>(
    meta_store: &M,
    page: &OpenStreamPage,
    writer_id: u64,
) -> Result<()> {
    meta_store
        .delete(
            &open_stream_page_key(page.shard, page.page_start_local, &page.stream_id),
            DelCond::Any,
            FenceToken(writer_id),
        )
        .await
}

pub async fn list_all_open_stream_pages<M: MetaStore>(
    meta_store: &M,
) -> Result<Vec<OpenStreamPage>> {
    let mut cursor = None;
    let mut out = Vec::new();
    loop {
        let page = meta_store
            .list_prefix(open_stream_page_prefix(), cursor.take(), 1_024)
            .await?;
        for key in page.keys {
            out.push(decode_open_stream_page_key(&key)?);
        }
        if page.next_cursor.is_none() {
            break;
        }
        cursor = page.next_cursor;
    }
    Ok(out)
}

pub async fn repair_open_stream_page_markers<M: MetaStore, B: BlobStore>(
    meta_store: &M,
    blob_store: &B,
    next_log_id: u64,
    writer_id: u64,
) -> Result<()> {
    for page in list_all_open_stream_pages(meta_store)
        .await?
        .into_iter()
        .filter(|page| page.is_sealed_at(next_log_id))
    {
        let _ = compact_stream_page(
            meta_store,
            blob_store,
            &page.stream_id,
            page.page_start_local,
            writer_id,
        )
        .await?;
        delete_open_stream_page(meta_store, &page, writer_id).await?;
    }
    Ok(())
}

pub fn decode_open_stream_page_key(key: &[u8]) -> Result<OpenStreamPage> {
    let prefix_len = open_stream_page_prefix().len();
    let min_len = prefix_len + 8 + 1 + 8 + 1;
    if key.len() < min_len || !key.starts_with(open_stream_page_prefix()) {
        return Err(Error::Decode("invalid open_stream_page key"));
    }
    if key[prefix_len + 8] != b'/' || key[prefix_len + 17] != b'/' {
        return Err(Error::Decode("invalid open_stream_page separators"));
    }

    let shard = read_u64_be(&key[prefix_len..prefix_len + 8])
        .and_then(|raw| LogShard::new(raw).ok())
        .ok_or(Error::Decode("invalid open_stream_page shard"))?;
    let page_start_local = read_u64_be(&key[prefix_len + 9..prefix_len + 17])
        .and_then(|raw| u32::try_from(raw).ok())
        .ok_or(Error::Decode("invalid open_stream_page page"))?;
    let stream_id = String::from_utf8(key[prefix_len + 18..].to_vec())
        .map_err(|_| Error::Decode("invalid open_stream_page stream id"))?;

    Ok(OpenStreamPage {
        shard,
        page_start_local,
        stream_id,
    })
}
