use bytes::Bytes;

use crate::core::offsets::BucketedOffsets;
use crate::error::{Error, Result};
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;
use crate::traces::types::BlockTraceHeader;
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
        .block_trace_blobs
        .put_block(block_num, block_blob, &header)
        .await?;
    Ok(trace_count)
}

pub async fn persist_trace_dir_by_block<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    block_num: u64,
    first_trace_id: u64,
    count: u32,
) -> Result<()> {
    tables
        .trace_dir
        .persist_block_fragment(block_num, first_trace_id, count)
        .await
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
            offsets.push(crate::core::offsets::byte_offset_in(trace_rlp, frame))?;
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

#[cfg(test)]
mod tests {
    use futures::executor::block_on;

    use super::persist_trace_dir_by_block;
    use crate::kernel::codec::StorageCodec;
    use crate::kernel::table_specs::u64_key;
    use crate::store::blob::InMemoryBlobStore;
    use crate::store::meta::InMemoryMetaStore;
    use crate::traces::keys::TRACE_DIRECTORY_SUB_BUCKET_SIZE;
    use crate::traces::types::DirByBlock;
    use crate::{store::traits::MetaStore, tables::Tables};

    #[test]
    fn persist_trace_dir_by_block_writes_each_spanned_sub_bucket() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let tables = Tables::without_cache(meta.clone(), InMemoryBlobStore::default());
            let first_trace_id = TRACE_DIRECTORY_SUB_BUCKET_SIZE - 3;
            let count = 7u32;

            persist_trace_dir_by_block(&tables, 700, first_trace_id, count)
                .await
                .expect("persist fragments");

            for sub_bucket_start in [0, TRACE_DIRECTORY_SUB_BUCKET_SIZE] {
                let fragment = meta
                    .scan_get(
                        crate::traces::keys::TRACE_DIR_BY_BLOCK_TABLE,
                        &u64_key(sub_bucket_start),
                        &u64_key(700),
                    )
                    .await
                    .expect("load directory fragment")
                    .expect("directory fragment present");
                let fragment = DirByBlock::decode(&fragment.value).expect("decode directory");
                assert_eq!(fragment.block_num, 700);
                assert_eq!(fragment.first_primary_id, first_trace_id);
                assert_eq!(
                    fragment.end_primary_id_exclusive,
                    first_trace_id + u64::from(count)
                );
            }
        });
    }
}
