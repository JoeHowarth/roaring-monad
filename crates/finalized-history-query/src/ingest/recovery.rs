use crate::codec::finalized_state::decode_block_meta;
use crate::domain::keys::{
    block_hash_to_num_key, block_log_header_key, block_logs_blob_key, block_meta_key,
    log_directory_bucket_key, log_directory_sub_bucket_key, stream_page_blob_key,
    stream_page_meta_key,
};
use crate::domain::types::BlockMeta;
use crate::error::{Error, Result};
use crate::ingest::open_pages::OpenStreamPage;
use crate::logs::ingest::{
    newly_sealed_directory_bucket_starts, newly_sealed_directory_sub_bucket_starts,
    parse_stream_shard,
};
use crate::store::traits::{BlobStore, DelCond, FenceToken, MetaStore};

#[derive(Debug, Clone)]
pub struct BlockMetaAnchor {
    pub block_num: u64,
    pub meta: BlockMeta,
}

#[derive(Debug, Clone, Default)]
pub struct UnpublishedSummaryCleanup {
    pub stream_pages: Vec<OpenStreamPage>,
    pub directory_sub_buckets: Vec<u64>,
    pub directory_buckets: Vec<u64>,
}

pub async fn discover_unpublished_suffix<M: MetaStore>(
    meta_store: &M,
    published_head: u64,
) -> Result<Vec<BlockMetaAnchor>> {
    let mut next_block = published_head.saturating_add(1);
    let mut out = Vec::new();

    loop {
        let Some(record) = meta_store.get(&block_meta_key(next_block)).await? else {
            break;
        };
        out.push(BlockMetaAnchor {
            block_num: next_block,
            meta: decode_block_meta(&record.value)?,
        });
        next_block = next_block.saturating_add(1);
    }

    Ok(out)
}

pub async fn delete_unpublished_block<M: MetaStore, B: BlobStore>(
    meta_store: &M,
    blob_store: &B,
    anchor: &BlockMetaAnchor,
    writer_id: u64,
) -> Result<()> {
    for key in
        list_blob_keys_with_block_suffix(blob_store, b"stream_frag_blob/", anchor.block_num).await?
    {
        blob_store.delete_blob(&key).await?;
    }
    for key in
        list_keys_with_block_suffix(meta_store, b"stream_frag_meta/", anchor.block_num).await?
    {
        meta_store
            .delete(&key, DelCond::Any, FenceToken(writer_id))
            .await?;
    }
    blob_store
        .delete_blob(&block_logs_blob_key(anchor.block_num))
        .await?;
    meta_store
        .delete(
            &block_log_header_key(anchor.block_num),
            DelCond::Any,
            FenceToken(writer_id),
        )
        .await?;
    for key in list_keys_with_block_suffix(meta_store, b"log_dir_frag/", anchor.block_num).await? {
        meta_store
            .delete(&key, DelCond::Any, FenceToken(writer_id))
            .await?;
    }
    meta_store
        .delete(
            &block_hash_to_num_key(&anchor.meta.block_hash),
            DelCond::Any,
            FenceToken(writer_id),
        )
        .await?;
    meta_store
        .delete(
            &block_meta_key(anchor.block_num),
            DelCond::Any,
            FenceToken(writer_id),
        )
        .await?;
    Ok(())
}

pub async fn discover_unpublished_summaries_to_clean<M: MetaStore>(
    meta_store: &M,
    published_head: u64,
    anchors: &[BlockMetaAnchor],
) -> Result<UnpublishedSummaryCleanup> {
    let Some(last_anchor) = anchors.last() else {
        return Ok(UnpublishedSummaryCleanup::default());
    };

    let from_next_log_id = if published_head == 0 {
        0
    } else {
        let Some(record) = meta_store.get(&block_meta_key(published_head)).await? else {
            return Err(Error::NotFound);
        };
        let head_meta = decode_block_meta(&record.value)?;
        head_meta
            .first_log_id
            .saturating_add(u64::from(head_meta.count))
    };
    let to_next_log_id = last_anchor
        .meta
        .first_log_id
        .saturating_add(u64::from(last_anchor.meta.count));

    Ok(UnpublishedSummaryCleanup {
        stream_pages: discover_unpublished_stream_page_summaries_to_clean(
            meta_store,
            from_next_log_id,
            to_next_log_id,
        )
        .await?,
        directory_sub_buckets: newly_sealed_directory_sub_bucket_starts(
            from_next_log_id,
            to_next_log_id,
        ),
        directory_buckets: newly_sealed_directory_bucket_starts(from_next_log_id, to_next_log_id),
    })
}

pub async fn delete_unpublished_summaries<M: MetaStore, B: BlobStore>(
    meta_store: &M,
    blob_store: &B,
    summaries: &UnpublishedSummaryCleanup,
    writer_id: u64,
) -> Result<()> {
    for page in &summaries.stream_pages {
        blob_store
            .delete_blob(&stream_page_blob_key(
                &page.stream_id,
                page.page_start_local,
            ))
            .await?;
        meta_store
            .delete(
                &stream_page_meta_key(&page.stream_id, page.page_start_local),
                DelCond::Any,
                FenceToken(writer_id),
            )
            .await?;
    }

    for bucket_start in &summaries.directory_buckets {
        meta_store
            .delete(
                &log_directory_bucket_key(*bucket_start),
                DelCond::Any,
                FenceToken(writer_id),
            )
            .await?;
    }

    for sub_bucket_start in &summaries.directory_sub_buckets {
        meta_store
            .delete(
                &log_directory_sub_bucket_key(*sub_bucket_start),
                DelCond::Any,
                FenceToken(writer_id),
            )
            .await?;
    }

    Ok(())
}

pub async fn cleanup_unpublished_suffix<M: MetaStore, B: BlobStore>(
    meta_store: &M,
    blob_store: &B,
    published_head: u64,
    writer_id: u64,
) -> Result<Vec<BlockMetaAnchor>> {
    let anchors = discover_unpublished_suffix(meta_store, published_head).await?;
    let summaries =
        discover_unpublished_summaries_to_clean(meta_store, published_head, &anchors).await?;
    delete_unpublished_summaries(meta_store, blob_store, &summaries, writer_id).await?;
    for anchor in anchors.iter().rev() {
        delete_unpublished_block(meta_store, blob_store, anchor, writer_id).await?;
    }
    Ok(anchors)
}

async fn list_keys_with_block_suffix<M: MetaStore>(
    meta_store: &M,
    prefix: &[u8],
    block_num: u64,
) -> Result<Vec<Vec<u8>>> {
    let mut cursor = None;
    let mut out = Vec::new();
    loop {
        let page = meta_store.list_prefix(prefix, cursor.take(), 1_024).await?;
        for key in page.keys {
            if read_u64_suffix(&key)? == block_num {
                out.push(key);
            }
        }
        if page.next_cursor.is_none() {
            break;
        }
        cursor = page.next_cursor;
    }
    Ok(out)
}

async fn list_blob_keys_with_block_suffix<B: BlobStore>(
    blob_store: &B,
    prefix: &[u8],
    block_num: u64,
) -> Result<Vec<Vec<u8>>> {
    let mut cursor = None;
    let mut out = Vec::new();
    loop {
        let page = blob_store.list_prefix(prefix, cursor.take(), 1_024).await?;
        for key in page.keys {
            if read_u64_suffix(&key)? == block_num {
                out.push(key);
            }
        }
        if page.next_cursor.is_none() {
            break;
        }
        cursor = page.next_cursor;
    }
    Ok(out)
}

fn read_u64_suffix(key: &[u8]) -> Result<u64> {
    if key.len() < 8 {
        return Err(Error::Decode("short key suffix"));
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&key[key.len() - 8..]);
    Ok(u64::from_be_bytes(bytes))
}

async fn discover_unpublished_stream_page_summaries_to_clean<M: MetaStore>(
    meta_store: &M,
    from_next_log_id: u64,
    to_next_log_id: u64,
) -> Result<Vec<OpenStreamPage>> {
    if to_next_log_id <= from_next_log_id {
        return Ok(Vec::new());
    }

    let mut cursor = None;
    let mut out = Vec::new();
    loop {
        let page = meta_store
            .list_prefix(b"stream_page_meta/", cursor.take(), 1_024)
            .await?;
        for key in page.keys {
            if let Some(summary_page) = parse_stream_page_summary_key(&key)?
                && summary_page_is_in_newly_sealed_range(
                    &summary_page,
                    from_next_log_id,
                    to_next_log_id,
                )
            {
                out.push(summary_page);
            }
        }
        if page.next_cursor.is_none() {
            break;
        }
        cursor = page.next_cursor;
    }

    Ok(out)
}

fn parse_stream_page_summary_key(key: &[u8]) -> Result<Option<OpenStreamPage>> {
    let prefix = b"stream_page_meta/";
    if !key.starts_with(prefix) {
        return Ok(None);
    }
    if key.len() < prefix.len() + 1 + 8 {
        return Err(Error::Decode("short stream_page_meta key"));
    }
    let separator = key
        .len()
        .checked_sub(9)
        .ok_or(Error::Decode("short stream_page_meta key"))?;
    if key[separator] != b'/' {
        return Err(Error::Decode("invalid stream_page_meta separator"));
    }
    let page_start_local = read_u64_suffix(key).and_then(|raw| {
        u32::try_from(raw).map_err(|_| Error::Decode("stream page start overflow"))
    })?;
    let stream_id = String::from_utf8(key[prefix.len()..separator].to_vec())
        .map_err(|_| Error::Decode("invalid stream_page_meta stream id"))?;
    let Some(shard) = parse_stream_shard(&stream_id) else {
        return Ok(None);
    };

    Ok(Some(OpenStreamPage {
        shard,
        page_start_local,
        stream_id,
    }))
}

fn summary_page_is_in_newly_sealed_range(
    page: &OpenStreamPage,
    from_next_log_id: u64,
    to_next_log_id: u64,
) -> bool {
    if !page.is_sealed_at(to_next_log_id) || page.is_sealed_at(from_next_log_id) {
        return false;
    }
    true
}
