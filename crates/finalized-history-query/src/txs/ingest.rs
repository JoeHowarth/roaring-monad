use std::collections::BTreeMap;

use bytes::Bytes;

use crate::config::Config;
use crate::core::offsets::BucketedOffsets;
use crate::error::{Error, Result};
use crate::family::FinalizedBlock;
use crate::kernel::codec::StorageCodec;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;
use crate::txs::types::{BlockTxHeader, StoredTxEnvelope, TxLocation};

pub fn collect_stream_appends(
    _block: &FinalizedBlock,
    _first_tx_id: u64,
) -> BTreeMap<String, Vec<u32>> {
    todo!("tx stream fanout planning is not implemented")
}

pub async fn persist_stream_fragments<M: MetaStore, B: BlobStore>(
    _tables: &Tables<M, B>,
    _block: &FinalizedBlock,
    _first_tx_id: u64,
) -> Result<Vec<(String, u32)>> {
    todo!("tx stream-fragment persistence is not implemented")
}

pub async fn persist_tx_artifacts<M: MetaStore, B: BlobStore>(
    _config: &Config,
    tables: &Tables<M, B>,
    block: &FinalizedBlock,
    _first_tx_id: u64,
) -> Result<usize> {
    let mut offsets = BucketedOffsets::new();
    let mut out = Vec::<u8>::new();

    for (expected_tx_idx, tx) in block.txs.iter().enumerate() {
        let expected_tx_idx =
            u32::try_from(expected_tx_idx).map_err(|_| Error::Decode("tx_idx overflow"))?;
        if tx.tx_idx != expected_tx_idx {
            return Err(Error::InvalidParams(
                "tx_idx must match tx position within block",
            ));
        }
        offsets.push(
            u64::try_from(out.len()).map_err(|_| Error::Decode("block tx offset overflow"))?,
        )?;
        out.extend_from_slice(
            &StoredTxEnvelope {
                tx_hash: tx.tx_hash,
                sender: tx.sender,
                signed_tx_bytes: tx.signed_tx_bytes.clone(),
            }
            .encode(),
        );
        tables
            .tx_hash_index
            .put(
                &tx.tx_hash,
                &TxLocation {
                    block_num: block.block_num,
                    tx_idx: tx.tx_idx,
                },
            )
            .await?;
    }

    offsets.push(u64::try_from(out.len()).map_err(|_| Error::Decode("block tx size overflow"))?)?;

    tables
        .block_tx_blobs
        .put_block(
            block.block_num,
            Bytes::from(out),
            &BlockTxHeader { offsets },
        )
        .await?;

    Ok(block.txs.len())
}
