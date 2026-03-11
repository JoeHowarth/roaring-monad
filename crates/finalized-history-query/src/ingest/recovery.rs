use crate::codec::finalized_state::decode_block_meta;
use crate::domain::keys::{
    block_hash_to_num_key, block_log_header_key, block_logs_blob_key, block_meta_key,
};
use crate::domain::types::BlockMeta;
use crate::error::{Error, Result};
use crate::store::traits::{BlobStore, DelCond, FenceToken, MetaStore};

#[derive(Debug, Clone)]
pub struct BlockMetaAnchor {
    pub block_num: u64,
    pub meta: BlockMeta,
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

pub async fn cleanup_unpublished_suffix<M: MetaStore, B: BlobStore>(
    meta_store: &M,
    blob_store: &B,
    published_head: u64,
    writer_id: u64,
) -> Result<Vec<BlockMetaAnchor>> {
    let anchors = discover_unpublished_suffix(meta_store, published_head).await?;
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
