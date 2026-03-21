use bytes::Bytes;

use crate::codec::finalized_state::encode_u64;
use crate::config::Config;
use crate::domain::table_specs::{BlobTableSpec, PointTableSpec, ScannableTableSpec};
use crate::error::{Error, Result};
use crate::logs::keys::LOG_DIRECTORY_SUB_BUCKET_SIZE;
use crate::logs::table_specs::{
    BlockHashIndexSpec, BlockLogBlobSpec, BlockLogHeaderSpec, BlockRecordSpec, LogDirByBlockSpec,
    LogDirSubBucketSpec,
};
use crate::logs::types::{Block, BlockLogHeader, BlockRecord, DirByBlock, Log};
use crate::store::traits::{BlobStore, BlobTableId, MetaStore, PutCond, ScannableTableId, TableId};

pub(in crate::logs) async fn put_artifact_meta<M: MetaStore>(
    meta_store: &M,
    family: TableId,
    key: &[u8],
    value: Bytes,
) -> Result<()> {
    let _ = meta_store.put(family, key, value, PutCond::Any).await?;
    Ok(())
}

pub(in crate::logs) async fn put_scannable_artifact_meta<M: MetaStore>(
    meta_store: &M,
    family: ScannableTableId,
    partition: &[u8],
    clustering: &[u8],
    value: Bytes,
) -> Result<()> {
    let _ = meta_store
        .scan_put(family, partition, clustering, value, PutCond::Any)
        .await?;
    Ok(())
}

pub(in crate::logs) async fn put_artifact_blob<B: BlobStore>(
    blob_store: &B,
    table: BlobTableId,
    key: &[u8],
    value: Bytes,
) -> Result<()> {
    blob_store.put_blob(table, key, value).await
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
    put_artifact_blob(
        blob_store,
        BlockLogBlobSpec::TABLE,
        &BlockLogBlobSpec::key(block_num),
        block_blob,
    )
    .await?;
    put_artifact_meta(
        meta_store,
        BlockLogHeaderSpec::TABLE,
        &BlockLogHeaderSpec::key(block_num),
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
        BlockRecordSpec::TABLE,
        &BlockRecordSpec::key(block.block_num),
        block_record.encode(),
    )
    .await?;

    put_artifact_meta(
        meta_store,
        BlockHashIndexSpec::TABLE,
        &BlockHashIndexSpec::key(&block.block_hash),
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

    let mut sub_bucket_start = LogDirSubBucketSpec::sub_bucket_start(first_log_id);
    let last_sub_bucket_start = if count == 0 {
        sub_bucket_start
    } else {
        LogDirSubBucketSpec::sub_bucket_start(fragment.end_log_id_exclusive.saturating_sub(1))
    };
    let encoded_fragment = fragment.encode();

    loop {
        let partition = LogDirByBlockSpec::partition(sub_bucket_start);
        let clustering = LogDirByBlockSpec::clustering(block_num);
        put_scannable_artifact_meta(
            meta_store,
            LogDirByBlockSpec::TABLE,
            &partition,
            &clustering,
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
