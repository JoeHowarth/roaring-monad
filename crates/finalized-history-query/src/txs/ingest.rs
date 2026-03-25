use std::collections::BTreeMap;

use bytes::Bytes;

use crate::config::Config;
use crate::core::ids::TxId;
use crate::core::offsets::BucketedOffsets;
use crate::error::{Error, Result};
use crate::family::FinalizedBlock;
use crate::ingest::bitmap_pages;
use crate::ingest::indexed_family::primary_id_at_offset;
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
        let global_tx_id = TxId::new(primary_id_at_offset(first_tx_id, index));
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

#[cfg(test)]
mod tests {
    use alloy_rlp::{Encodable, Header};
    use futures::executor::block_on;

    use crate::config::Config;
    use crate::core::ids::TxId;
    use crate::error::Error;
    use crate::family::FinalizedBlock;
    use crate::kernel::codec::StorageCodec;
    use crate::kernel::sharded_streams::sharded_stream_id;
    use crate::kernel::table_specs::{BlobTableSpec, PointTableSpec};
    use crate::store::blob::InMemoryBlobStore;
    use crate::store::meta::InMemoryMetaStore;
    use crate::store::traits::{BlobStore, MetaStore};
    use crate::tables::Tables;
    use crate::txs::table_specs::{BlockTxBlobSpec, BlockTxHeaderSpec, TxHashIndexSpec};
    use crate::txs::types::{BlockTxHeader, IngestTx, StoredTxEnvelope, TxLocation};

    use super::{collect_stream_appends, persist_tx_artifacts};

    fn encode_field<T: Encodable>(value: T) -> Vec<u8> {
        let mut out = Vec::new();
        value.encode(&mut out);
        out
    }

    fn encode_bytes(value: &[u8]) -> Vec<u8> {
        encode_field(value)
    }

    fn encode_tx_list(fields: Vec<Vec<u8>>) -> Vec<u8> {
        let mut out = Vec::new();
        Header {
            list: true,
            payload_length: fields.iter().map(Vec::len).sum(),
        }
        .encode(&mut out);
        for field in fields {
            out.extend_from_slice(&field);
        }
        out
    }

    fn encode_legacy_tx(to: Option<[u8; 20]>, input: &[u8]) -> Vec<u8> {
        encode_tx_list(vec![
            encode_field(1u64),
            encode_field(2u64),
            encode_field(21_000u64),
            encode_bytes(to.as_ref().map(<[u8; 20]>::as_slice).unwrap_or(&[])),
            encode_field(3u64),
            encode_bytes(input),
            encode_field(27u8),
            encode_field(1u8),
            encode_field(2u8),
        ])
    }

    fn sample_tx(tx_idx: u32, seed: u8, to: Option<[u8; 20]>, input: &[u8]) -> IngestTx {
        IngestTx {
            tx_idx,
            tx_hash: [seed; 32],
            sender: [seed.wrapping_add(1); 20],
            signed_tx_bytes: encode_legacy_tx(to, input),
        }
    }

    fn sample_block(block_num: u64, txs: Vec<IngestTx>) -> FinalizedBlock {
        let block_hash = [7u8; 32];
        let parent_hash = [8u8; 32];
        FinalizedBlock {
            block_num,
            block_hash,
            parent_hash,
            header: crate::core::header::EvmBlockHeader::minimal(
                block_num,
                block_hash,
                parent_hash,
            ),
            logs: Vec::new(),
            txs,
            trace_rlp: Vec::new(),
        }
    }

    #[test]
    fn persist_tx_artifacts_writes_block_blob_and_hash_index() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let blob = InMemoryBlobStore::default();
            let tables = Tables::without_cache(meta.clone(), blob.clone());
            let config = Config::default();
            let block = sample_block(
                7,
                vec![
                    sample_tx(0, 1, Some([3u8; 20]), &[0xaa, 0xbb, 0xcc, 0xdd, 1]),
                    sample_tx(1, 2, None, &[0xee, 0xff]),
                ],
            );

            let count = persist_tx_artifacts(&config, &tables, &block, 0)
                .await
                .expect("persist tx artifacts");

            assert_eq!(count, 2);

            let header = meta
                .get(BlockTxHeaderSpec::TABLE, &BlockTxHeaderSpec::key(7))
                .await
                .expect("read tx header")
                .expect("tx header present");
            let header = BlockTxHeader::decode(&header.value).expect("decode tx header");
            assert_eq!(header.tx_count(), 2);

            let blob_bytes = blob
                .get_blob(BlockTxBlobSpec::TABLE, &BlockTxBlobSpec::key(7))
                .await
                .expect("read tx blob")
                .expect("tx blob present");
            let first =
                StoredTxEnvelope::decode(&blob_bytes[..header.offset(1).expect("offset") as usize])
                    .expect("decode first tx");
            assert_eq!(first.tx_hash, block.txs[0].tx_hash);

            let location = meta
                .get(
                    TxHashIndexSpec::TABLE,
                    &TxHashIndexSpec::key(&block.txs[1].tx_hash),
                )
                .await
                .expect("read tx hash index")
                .expect("tx hash index present");
            let location = TxLocation::decode(&location.value).expect("decode tx location");
            assert_eq!(
                location,
                TxLocation {
                    block_num: 7,
                    tx_idx: 1,
                }
            );
        });
    }

    #[test]
    fn persist_tx_artifacts_rejects_invalid_signed_tx_bytes() {
        block_on(async {
            let tables =
                Tables::without_cache(InMemoryMetaStore::default(), InMemoryBlobStore::default());
            let config = Config::default();
            let mut block = sample_block(7, vec![sample_tx(0, 1, Some([3u8; 20]), &[0xaa])]);
            block.txs[0].signed_tx_bytes = vec![0x01];

            let err = persist_tx_artifacts(&config, &tables, &block, 0)
                .await
                .expect_err("invalid signed tx should fail");

            assert!(matches!(
                err,
                Error::InvalidParams("invalid signed tx bytes")
            ));
        });
    }

    #[test]
    fn persist_tx_artifacts_rejects_non_canonical_tx_idx() {
        block_on(async {
            let tables =
                Tables::without_cache(InMemoryMetaStore::default(), InMemoryBlobStore::default());
            let config = Config::default();
            let block = sample_block(
                7,
                vec![
                    sample_tx(0, 1, Some([3u8; 20]), &[0xaa]),
                    sample_tx(3, 2, Some([4u8; 20]), &[0xbb]),
                ],
            );

            let err = persist_tx_artifacts(&config, &tables, &block, 0)
                .await
                .expect_err("invalid tx order should fail");

            assert!(matches!(
                err,
                Error::InvalidParams("tx_idx must match tx position within block")
            ));
        });
    }

    #[test]
    fn persist_tx_artifacts_persists_empty_block_blob_and_header() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let blob = InMemoryBlobStore::default();
            let tables = Tables::without_cache(meta.clone(), blob.clone());
            let config = Config::default();
            let block = sample_block(7, Vec::new());

            let count = persist_tx_artifacts(&config, &tables, &block, 0)
                .await
                .expect("persist empty tx block");

            assert_eq!(count, 0);

            let header = meta
                .get(BlockTxHeaderSpec::TABLE, &BlockTxHeaderSpec::key(7))
                .await
                .expect("read tx header")
                .expect("tx header present");
            let header = BlockTxHeader::decode(&header.value).expect("decode tx header");
            let blob_bytes = blob
                .get_blob(BlockTxBlobSpec::TABLE, &BlockTxBlobSpec::key(7))
                .await
                .expect("read tx blob")
                .expect("tx blob present");

            assert_eq!(header.tx_count(), 0);
            assert_eq!(header.offset(0).expect("sentinel offset"), 0);
            assert!(blob_bytes.is_empty());
        });
    }

    #[test]
    fn collect_stream_appends_skips_create_and_missing_selector_streams() {
        let block = sample_block(
            7,
            vec![
                sample_tx(0, 1, Some([3u8; 20]), &[0xaa, 0xbb, 0xcc, 0xdd, 1]),
                sample_tx(1, 2, None, &[0xee, 0xff, 0x00, 0x11]),
                sample_tx(2, 3, Some([4u8; 20]), &[0x99, 0x88, 0x77]),
            ],
        );

        let appends = collect_stream_appends(&block, 0).expect("collect appends");
        let first_shard = TxId::new(0).shard().get();
        let third_shard = TxId::new(2).shard().get();

        assert!(appends.contains_key(&sharded_stream_id("to", &[3u8; 20], first_shard)));
        assert!(appends.contains_key(&sharded_stream_id("to", &[4u8; 20], third_shard)));
        assert!(appends.contains_key(&sharded_stream_id(
            "selector",
            &[0xaa, 0xbb, 0xcc, 0xdd],
            first_shard,
        )));
        assert_eq!(
            appends
                .keys()
                .filter(|stream| stream.starts_with("selector/"))
                .count(),
            1
        );
    }
}
