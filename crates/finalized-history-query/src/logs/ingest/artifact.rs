use bytes::Bytes;

use crate::codec::finalized_state::encode_u64;
use crate::config::Config;
use crate::core::ids::LogId;
use crate::domain::keys::{
    LOG_DIRECTORY_SUB_BUCKET_SIZE, block_hash_index_key, block_log_blob_key, block_log_header_key,
    block_record_key, log_dir_by_block_key, log_dir_sub_bucket_start,
};
use crate::domain::types::{BlockLogHeader, BlockRecord, DirByBlock, Log};
use crate::error::{Error, Result};
use crate::logs::types::Block;
use crate::store::traits::{BlobStore, MetaStore, PutCond};

pub(in crate::logs) async fn put_artifact_meta<M: MetaStore>(
    meta_store: &M,
    key: &[u8],
    value: Bytes,
) -> Result<()> {
    let _ = meta_store.put(key, value, PutCond::Any).await?;
    Ok(())
}

pub(in crate::logs) async fn put_artifact_blob<B: BlobStore>(
    blob_store: &B,
    key: &[u8],
    value: Bytes,
) -> Result<()> {
    blob_store.put_blob(key, value).await
}

pub async fn persist_log_artifacts<M: MetaStore, B: BlobStore>(
    _config: &Config,
    meta_store: &M,
    blob_store: &B,
    block_num: u64,
    logs: &[Log],
    _first_log_id: u64,
) -> Result<()> {
    if logs.is_empty() {
        return Ok(());
    }

    let (block_blob, header) = encode_block_log_blob(logs)?;
    put_artifact_blob(blob_store, &block_log_blob_key(block_num), block_blob).await?;
    put_artifact_meta(
        meta_store,
        &block_log_header_key(block_num),
        header.encode(),
    )
    .await?;
    Ok(())
}

pub async fn persist_log_block_record<M: MetaStore>(
    meta_store: &M,
    block: &Block,
    first_log_id: u64,
) -> Result<()> {
    let block_record = BlockRecord {
        block_hash: block.block_hash,
        parent_hash: block.parent_hash,
        first_log_id,
        count: block.logs.len() as u32,
    };

    put_artifact_meta(
        meta_store,
        &block_record_key(block.block_num),
        block_record.encode(),
    )
    .await?;

    put_artifact_meta(
        meta_store,
        &block_hash_index_key(&block.block_hash),
        encode_u64(block.block_num),
    )
    .await?;

    Ok(())
}

pub async fn persist_log_dir_by_block<M: MetaStore>(
    meta_store: &M,
    block_num: u64,
    first_log_id: u64,
    count: u32,
) -> Result<()> {
    let fragment = DirByBlock {
        block_num,
        first_log_id,
        end_log_id_exclusive: first_log_id.saturating_add(u64::from(count)),
    };

    let mut sub_bucket_start = log_dir_sub_bucket_start(LogId::new(first_log_id));
    let last_sub_bucket_start = if count == 0 {
        sub_bucket_start
    } else {
        log_dir_sub_bucket_start(LogId::new(fragment.end_log_id_exclusive.saturating_sub(1)))
    };
    let encoded_fragment = fragment.encode();

    loop {
        put_artifact_meta(
            meta_store,
            &log_dir_by_block_key(sub_bucket_start, block_num),
            encoded_fragment.clone(),
        )
        .await?;
        if sub_bucket_start == last_sub_bucket_start {
            break;
        }
        sub_bucket_start = sub_bucket_start.saturating_add(LOG_DIRECTORY_SUB_BUCKET_SIZE);
    }

    Ok(())
}

fn encode_block_log_blob(logs: &[Log]) -> Result<(Bytes, BlockLogHeader)> {
    let mut out = Vec::<u8>::new();
    let mut offsets = Vec::with_capacity(logs.len() + 1);
    for log in logs {
        offsets.push(
            u32::try_from(out.len()).map_err(|_| Error::Decode("block log offset overflow"))?,
        );
        out.extend_from_slice(&log.encode());
    }
    offsets.push(u32::try_from(out.len()).map_err(|_| Error::Decode("block log size overflow"))?);
    Ok((Bytes::from(out), BlockLogHeader { offsets }))
}

pub fn parse_stream_shard(stream_id: &str) -> Option<crate::core::ids::LogShard> {
    let (_, shard_hex) = stream_id.rsplit_once('/')?;
    let raw = u64::from_str_radix(shard_hex, 16).ok()?;
    crate::core::ids::LogShard::new(raw).ok()
}

pub(in crate::logs) fn read_u64_suffix(key: &[u8]) -> Result<u64> {
    if key.len() < 8 {
        return Err(Error::Decode("short key suffix"));
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&key[key.len() - 8..]);
    Ok(u64::from_be_bytes(bytes))
}
