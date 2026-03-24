use bytes::Bytes;

use crate::config::Config;
use crate::error::{Error, Result};
use crate::kernel::codec::StorageCodec;
use crate::logs::keys::LOG_DIRECTORY_SUB_BUCKET_SIZE;
use crate::logs::table_specs::{LogDirByBlockSpec, LogDirSubBucketSpec};
use crate::logs::types::{BlockLogHeader, Log};
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::{PrimaryDirFragmentLayout, Tables};

pub async fn persist_log_artifacts<M: MetaStore, B: BlobStore>(
    _config: &Config,
    tables: &Tables<M, B>,
    block_num: u64,
    logs: &[Log],
    _first_log_id: u64,
) -> Result<()> {
    let (block_blob, header) = encode_block_log_blob(logs)?;
    tables
        .point_log_payloads
        .put_block(block_num, block_blob, &header)
        .await
}

pub async fn persist_log_dir_by_block<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    block_num: u64,
    first_log_id: u64,
    count: u32,
) -> Result<()> {
    tables
        .log_dir
        .persist_block_fragment(
            block_num,
            first_log_id,
            count,
            PrimaryDirFragmentLayout {
                sub_bucket_start: LogDirSubBucketSpec::sub_bucket_start,
                sub_bucket_span: LOG_DIRECTORY_SUB_BUCKET_SIZE,
                partition: LogDirByBlockSpec::partition,
                clustering: LogDirByBlockSpec::clustering,
            },
        )
        .await
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
