use bytes::Bytes;

use crate::codec::finalized_state::{encode_block_meta, encode_u64};
use crate::codec::log::{encode_block_log_header, encode_log, encode_log_dir_fragment};
use crate::config::Config;
use crate::core::ids::LogId;
use crate::domain::keys::{
    LOG_DIRECTORY_SUB_BUCKET_SIZE, block_hash_to_num_key, block_log_header_key,
    block_logs_blob_key, block_meta_key, log_directory_fragment_key,
    log_directory_sub_bucket_start,
};
use crate::domain::types::{BlockLogHeader, BlockMeta, Log, LogDirFragment};
use crate::error::{Error, Result};
use crate::logs::types::Block;
use crate::store::traits::{BlobStore, CreateOutcome, FenceToken, MetaStore, PutCond};

#[derive(Clone, Copy)]
pub(in crate::logs) enum ImmutableClass {
    Artifact,
    Summary,
}

pub(in crate::logs) async fn put_immutable_meta<M: MetaStore>(
    meta_store: &M,
    key: &[u8],
    value: Bytes,
    epoch: u64,
    class: ImmutableClass,
) -> Result<()> {
    let result = meta_store
        .put(key, value.clone(), PutCond::IfAbsent, FenceToken(epoch))
        .await?;
    if result.applied {
        return Ok(());
    }

    let Some(existing) = meta_store.get(key).await? else {
        return Err(Error::Backend(
            "immutable metadata create reported existing row but row is missing".to_string(),
        ));
    };
    if existing.value == value {
        return Ok(());
    }

    match class {
        ImmutableClass::Artifact => Err(Error::ArtifactConflict),
        ImmutableClass::Summary => Err(Error::SummaryConflict),
    }
}

pub(in crate::logs) async fn put_immutable_blob<B: BlobStore>(
    blob_store: &B,
    key: &[u8],
    value: Bytes,
    class: ImmutableClass,
) -> Result<()> {
    match blob_store.put_blob_if_absent(key, value.clone()).await? {
        CreateOutcome::Created => Ok(()),
        CreateOutcome::AlreadyExists => {
            let Some(existing) = blob_store.get_blob(key).await? else {
                return Err(Error::Backend(
                    "immutable blob create reported existing object but object is missing"
                        .to_string(),
                ));
            };
            if existing == value {
                Ok(())
            } else {
                match class {
                    ImmutableClass::Artifact => Err(Error::ArtifactConflict),
                    ImmutableClass::Summary => Err(Error::SummaryConflict),
                }
            }
        }
    }
}

pub async fn persist_log_artifacts<M: MetaStore, B: BlobStore>(
    _config: &Config,
    meta_store: &M,
    blob_store: &B,
    block_num: u64,
    logs: &[Log],
    _first_log_id: u64,
    epoch: u64,
) -> Result<()> {
    if logs.is_empty() {
        return Ok(());
    }

    let (block_blob, header) = encode_block_logs(logs)?;
    put_immutable_blob(
        blob_store,
        &block_logs_blob_key(block_num),
        block_blob,
        ImmutableClass::Artifact,
    )
    .await?;
    put_immutable_meta(
        meta_store,
        &block_log_header_key(block_num),
        encode_block_log_header(&header),
        epoch,
        ImmutableClass::Artifact,
    )
    .await?;
    Ok(())
}

pub async fn persist_log_block_metadata<M: MetaStore>(
    meta_store: &M,
    block: &Block,
    first_log_id: u64,
    epoch: u64,
) -> Result<()> {
    let block_meta = BlockMeta {
        block_hash: block.block_hash,
        parent_hash: block.parent_hash,
        first_log_id,
        count: block.logs.len() as u32,
    };

    put_immutable_meta(
        meta_store,
        &block_meta_key(block.block_num),
        encode_block_meta(&block_meta),
        epoch,
        ImmutableClass::Artifact,
    )
    .await?;

    put_immutable_meta(
        meta_store,
        &block_hash_to_num_key(&block.block_hash),
        encode_u64(block.block_num),
        epoch,
        ImmutableClass::Artifact,
    )
    .await?;

    Ok(())
}

pub async fn persist_log_directory_fragments<M: MetaStore>(
    meta_store: &M,
    block_num: u64,
    first_log_id: u64,
    count: u32,
    epoch: u64,
) -> Result<()> {
    let fragment = LogDirFragment {
        block_num,
        first_log_id,
        end_log_id_exclusive: first_log_id.saturating_add(u64::from(count)),
    };

    let mut sub_bucket_start = log_directory_sub_bucket_start(LogId::new(first_log_id));
    let last_sub_bucket_start = if count == 0 {
        sub_bucket_start
    } else {
        log_directory_sub_bucket_start(LogId::new(fragment.end_log_id_exclusive.saturating_sub(1)))
    };
    let encoded_fragment = encode_log_dir_fragment(&fragment);

    loop {
        put_immutable_meta(
            meta_store,
            &log_directory_fragment_key(sub_bucket_start, block_num),
            encoded_fragment.clone(),
            epoch,
            ImmutableClass::Artifact,
        )
        .await?;
        if sub_bucket_start == last_sub_bucket_start {
            break;
        }
        sub_bucket_start = sub_bucket_start.saturating_add(LOG_DIRECTORY_SUB_BUCKET_SIZE);
    }

    Ok(())
}

fn encode_block_logs(logs: &[Log]) -> Result<(Bytes, BlockLogHeader)> {
    let mut out = Vec::<u8>::new();
    let mut offsets = Vec::with_capacity(logs.len() + 1);
    for log in logs {
        offsets.push(
            u32::try_from(out.len()).map_err(|_| Error::Decode("block log offset overflow"))?,
        );
        out.extend_from_slice(&encode_log(log));
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
