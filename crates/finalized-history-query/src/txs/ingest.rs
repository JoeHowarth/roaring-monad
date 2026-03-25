use std::collections::BTreeMap;

use bytes::Bytes;

use crate::config::Config;
use crate::core::ids::TxId;
use crate::core::offsets::BucketedOffsets;
use crate::error::{Error, Result};
use crate::family::FinalizedBlock;
use crate::ingest::bitmap_pages;
use crate::kernel::codec::StorageCodec;
use crate::kernel::sharded_streams::{group_stream_values, sharded_stream_id};
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;
use crate::txs::TX_STREAM_PAGE_LOCAL_ID_SPAN;
use crate::txs::codec::validate_tx;
use crate::txs::types::{BlockTxHeader, StoredTxEnvelope, TxLocation};
use crate::txs::view::TxView;

pub fn collect_stream_appends(
    block: &FinalizedBlock,
    first_tx_id: u64,
) -> Result<BTreeMap<String, Vec<u32>>> {
    let mut values = Vec::new();

    for (index, tx) in block.txs.iter().enumerate() {
        let global_tx_id = TxId::new(first_tx_id.saturating_add(index as u64));
        let shard = global_tx_id.shard().get();
        let local = global_tx_id.local().get();
        let signed_tx =
            TxView::decode(&tx.signed_tx_bytes).map_err(|_| Error::Decode("invalid signed tx"))?;

        values.push((sharded_stream_id("from", &tx.sender, shard), local));
        if let Some(to_addr) = signed_tx.to_addr()? {
            values.push((sharded_stream_id("to", &to_addr, shard), local));
        }
        if let Some(selector) = signed_tx.selector()? {
            values.push((sharded_stream_id("selector", &selector, shard), local));
        }
    }

    Ok(group_stream_values(values))
}

pub async fn persist_stream_fragments<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    block: &FinalizedBlock,
    first_tx_id: u64,
) -> Result<Vec<(String, u32)>> {
    let grouped_values = collect_stream_appends(block, first_tx_id)?
        .into_iter()
        .flat_map(|(stream, values)| values.into_iter().map(move |value| (stream.clone(), value)));
    bitmap_pages::persist_stream_fragments(
        &tables.tx_streams,
        block.block_num,
        grouped_values,
        TX_STREAM_PAGE_LOCAL_ID_SPAN,
    )
    .await
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
        if !validate_tx(&tx.signed_tx_bytes) {
            return Err(Error::InvalidParams("invalid signed tx bytes"));
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
