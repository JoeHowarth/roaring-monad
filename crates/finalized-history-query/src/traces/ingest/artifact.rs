use bytes::Bytes;

use crate::core::offsets::BucketedOffsets;
use crate::error::{Error, Result};
use crate::family::FinalizedBlock;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;
use crate::traces::keys::TRACE_DIRECTORY_SUB_BUCKET_SIZE;
use crate::traces::table_specs::TraceDirSubBucketSpec;
use crate::traces::types::{BlockTraceHeader, DirByBlock, TraceBlockRecord};
use alloy_rlp::{Header, PayloadView};

pub const TRACE_ENCODING_VERSION: u32 = 1;

pub async fn persist_trace_artifacts<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    block_num: u64,
    trace_rlp: &[u8],
) -> Result<usize> {
    let (header, trace_count) = build_block_trace_header(trace_rlp)?;
    let block_blob = if trace_count == 0 {
        Bytes::new()
    } else {
        Bytes::copy_from_slice(trace_rlp)
    };
    tables
        .block_trace_blobs()
        .put_block(block_num, block_blob, &header)
        .await?;
    Ok(trace_count)
}

pub async fn persist_trace_block_record<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    block: &FinalizedBlock,
    first_trace_id: u64,
    count: u32,
) -> Result<()> {
    let block_record = TraceBlockRecord {
        block_hash: block.block_hash,
        parent_hash: block.parent_hash,
        first_trace_id,
        count,
    };

    tables
        .trace_block_records()
        .put(block.block_num, &block_record)
        .await
}

pub async fn persist_trace_dir_by_block<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    block_num: u64,
    first_trace_id: u64,
    count: u32,
) -> Result<()> {
    let fragment = DirByBlock {
        block_num,
        first_primary_id: first_trace_id,
        end_primary_id_exclusive: first_trace_id.saturating_add(u64::from(count)),
    };

    let mut sub_bucket_start = TraceDirSubBucketSpec::sub_bucket_start(first_trace_id);
    let last_sub_bucket_start = if count == 0 {
        sub_bucket_start
    } else {
        TraceDirSubBucketSpec::sub_bucket_start(fragment.end_primary_id_exclusive.saturating_sub(1))
    };

    loop {
        tables
            .trace_directory_fragments()
            .put(sub_bucket_start, block_num, &fragment)
            .await?;
        if sub_bucket_start == last_sub_bucket_start {
            break;
        }
        sub_bucket_start = sub_bucket_start.saturating_add(TRACE_DIRECTORY_SUB_BUCKET_SIZE);
    }

    Ok(())
}

fn build_block_trace_header(trace_rlp: &[u8]) -> Result<(BlockTraceHeader, usize)> {
    if trace_rlp.is_empty() {
        return Ok((empty_trace_header(), 0));
    }

    let mut offsets = BucketedOffsets::new();
    let mut tx_starts = Vec::new();
    let mut trace_count = 0usize;
    let mut buf = trace_rlp;
    let txs =
        match Header::decode_raw(&mut buf).map_err(|_| Error::Decode("invalid block trace rlp"))? {
            PayloadView::List(items) => items,
            PayloadView::String(_) => {
                return Err(Error::Decode("block trace blob must be an rlp list"));
            }
        };
    if !buf.is_empty() {
        return Err(Error::Decode("block trace blob has trailing bytes"));
    }

    for tx_bytes in txs {
        tx_starts
            .push(u32::try_from(trace_count).map_err(|_| Error::Decode("trace count overflow"))?);
        let mut tx_buf = tx_bytes;
        let frames = match Header::decode_raw(&mut tx_buf)
            .map_err(|_| Error::Decode("invalid transaction trace list"))?
        {
            PayloadView::List(items) => items,
            PayloadView::String(_) => {
                return Err(Error::Decode("transaction traces must be an rlp list"));
            }
        };
        if !tx_buf.is_empty() {
            return Err(Error::Decode("transaction trace list has trailing bytes"));
        }
        for frame in frames {
            offsets.push(offset_in(trace_rlp, frame))?;
            trace_count = trace_count.saturating_add(1);
        }
    }

    if trace_count == 0 {
        return Ok((empty_trace_header(), 0));
    }

    Ok((
        BlockTraceHeader {
            encoding_version: TRACE_ENCODING_VERSION,
            offsets,
            tx_starts,
        },
        trace_count,
    ))
}

fn empty_trace_header() -> BlockTraceHeader {
    BlockTraceHeader {
        encoding_version: TRACE_ENCODING_VERSION,
        offsets: BucketedOffsets::new(),
        tx_starts: Vec::new(),
    }
}

fn offset_in(root: &[u8], slice: &[u8]) -> u64 {
    crate::core::offsets::byte_offset_in(root, slice)
}
