use crate::core::directory_resolver::{ResolvedPrimaryLocation, resolve_primary_id};
use crate::core::ids::TxId;
use crate::core::refs::BlockRef;
use crate::error::{Error, Result};
use crate::query::runner::{MaterializerCaches, QueryMaterializer, cached_parent_block_ref};
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;
use crate::txs::filter::{TxFilter, exact_match};
use crate::txs::types::DirByBlock;
use crate::txs::view::TxRef;

pub struct TxMaterializer<'a, M: MetaStore, B: BlobStore> {
    tables: &'a Tables<M, B>,
    caches: MaterializerCaches<DirByBlock>,
}

impl<'a, M: MetaStore, B: BlobStore> TxMaterializer<'a, M, B> {
    pub fn new(tables: &'a Tables<M, B>) -> Self {
        Self {
            tables,
            caches: MaterializerCaches::default(),
        }
    }
}

impl<M: MetaStore, B: BlobStore> QueryMaterializer for TxMaterializer<'_, M, B> {
    type Id = TxId;
    type Item = TxRef;
    type Filter = TxFilter;
    type Output = TxRef;

    async fn resolve_id(&mut self, id: Self::Id) -> Result<Option<ResolvedPrimaryLocation>> {
        Ok(resolve_primary_id::<M, TxId>(
            &self.tables.tx_dir,
            &mut self.caches.directory_fragment_cache,
            id,
        )
        .await?
        .map(|location| ResolvedPrimaryLocation {
            block_num: location.block_num,
            local_ordinal: location.local_ordinal,
        }))
    }

    async fn load_run(
        &mut self,
        run: &[(Self::Id, ResolvedPrimaryLocation)],
    ) -> Result<Vec<(Self::Id, Self::Item)>> {
        let mut items = Vec::with_capacity(run.len());
        for (id, location) in run.iter().copied() {
            let tx_idx = u32::try_from(location.local_ordinal)
                .map_err(|_| Error::Decode("tx local ordinal overflow"))?;
            let Some(item) = self
                .tables
                .block_tx_blobs
                .load_tx_at(location.block_num, tx_idx)
                .await?
            else {
                continue;
            };
            items.push((id, item));
        }
        Ok(items)
    }

    async fn block_ref_for(&mut self, item: &Self::Item) -> Result<BlockRef> {
        cached_parent_block_ref(
            &mut self.caches.block_ref_cache,
            self.tables,
            item.block_num(),
            *item.block_hash(),
        )
        .await
    }

    fn exact_match(&self, item: &Self::Item, filter: &Self::Filter) -> bool {
        exact_match(item, filter)
    }

    fn into_output(item: Self::Item) -> Self::Output {
        item
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use bytes::Bytes;
    use futures::executor::block_on;

    use crate::core::ids::TxId;
    use crate::core::layout::DIRECTORY_SUB_BUCKET_SIZE;
    use crate::core::state::{BlockRecord, PrimaryWindowRecord};
    use crate::kernel::codec::StorageCodec;
    use crate::kernel::table_specs::{BlobTableSpec, ScannableTableSpec};
    use crate::store::blob::InMemoryBlobStore;
    use crate::store::meta::InMemoryMetaStore;
    use crate::store::traits::{BlobStore, BlobTableId, MetaStore, Page, PutCond};
    use crate::tables::{BytesCacheConfig, Tables};
    use crate::txs::table_specs::{BlockTxBlobSpec, TxDirByBlockSpec, TxDirSubBucketSpec};
    use crate::txs::types::{BlockTxHeader, DirByBlock, StoredTxEnvelope};

    use super::*;

    #[derive(Clone)]
    struct CountingBlobStore {
        inner: InMemoryBlobStore,
        get_blob_count: Arc<AtomicU64>,
        read_range_count: Arc<AtomicU64>,
    }

    impl BlobStore for CountingBlobStore {
        async fn put_blob(
            &self,
            table: BlobTableId,
            key: &[u8],
            value: Bytes,
        ) -> crate::Result<()> {
            self.inner.put_blob(table, key, value).await
        }

        async fn get_blob(&self, table: BlobTableId, key: &[u8]) -> crate::Result<Option<Bytes>> {
            if table == BlockTxBlobSpec::TABLE {
                self.get_blob_count.fetch_add(1, Ordering::Relaxed);
            }
            self.inner.get_blob(table, key).await
        }

        async fn read_range(
            &self,
            table: BlobTableId,
            key: &[u8],
            start: u64,
            end_exclusive: u64,
        ) -> crate::Result<Option<Bytes>> {
            if table == BlockTxBlobSpec::TABLE {
                self.read_range_count.fetch_add(1, Ordering::Relaxed);
            }
            self.inner
                .read_range(table, key, start, end_exclusive)
                .await
        }

        async fn delete_blob(&self, table: BlobTableId, key: &[u8]) -> crate::Result<()> {
            self.inner.delete_blob(table, key).await
        }

        async fn list_prefix(
            &self,
            table: BlobTableId,
            prefix: &[u8],
            cursor: Option<Vec<u8>>,
            limit: usize,
        ) -> crate::Result<Page> {
            self.inner.list_prefix(table, prefix, cursor, limit).await
        }
    }

    #[test]
    fn load_tx_at_reads_single_envelope_via_range_read() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let blob = CountingBlobStore {
                inner: InMemoryBlobStore::default(),
                get_blob_count: Arc::new(AtomicU64::new(0)),
                read_range_count: Arc::new(AtomicU64::new(0)),
            };
            let get_blob_count = Arc::clone(&blob.get_blob_count);
            let read_range_count = Arc::clone(&blob.read_range_count);
            let tables = Tables::new(meta, blob, BytesCacheConfig::disabled());

            tables
                .block_records
                .put(
                    7,
                    &BlockRecord {
                        block_hash: [9u8; 32],
                        parent_hash: [8u8; 32],
                        logs: None,
                        txs: Some(PrimaryWindowRecord {
                            first_primary_id: 0,
                            count: 2,
                        }),
                        traces: None,
                    },
                )
                .await
                .expect("block record");

            let first = StoredTxEnvelope {
                tx_hash: [1u8; 32],
                sender: [2u8; 20],
                signed_tx_bytes: vec![3, 4, 5],
            }
            .encode();
            let second = StoredTxEnvelope {
                tx_hash: [6u8; 32],
                sender: [7u8; 20],
                signed_tx_bytes: vec![8, 9],
            }
            .encode();
            let mut blob_bytes = Vec::new();
            blob_bytes.extend_from_slice(&first);
            blob_bytes.extend_from_slice(&second);
            let mut offsets = crate::core::offsets::BucketedOffsets::new();
            offsets.push(0).expect("offset");
            offsets
                .push(u64::try_from(first.len()).expect("first len"))
                .expect("offset");
            offsets
                .push(u64::try_from(blob_bytes.len()).expect("blob len"))
                .expect("offset");
            tables
                .block_tx_blobs
                .put_block(7, Bytes::from(blob_bytes), &BlockTxHeader { offsets })
                .await
                .expect("tx blob");

            let tx = tables
                .block_tx_blobs
                .load_tx_at(7, 1)
                .await
                .expect("load tx")
                .expect("tx exists");

            assert_eq!(tx.block_num(), 7);
            assert_eq!(tx.block_hash(), &[9u8; 32]);
            assert_eq!(tx.tx_idx(), 1);
            assert_eq!(tx.tx_hash().expect("tx hash"), &[6u8; 32]);
            assert_eq!(tx.sender().expect("sender"), &[7u8; 20]);
            assert_eq!(tx.signed_tx_bytes().expect("signed bytes"), &[8, 9]);
            assert_eq!(read_range_count.load(Ordering::Relaxed), 1);
            assert_eq!(get_blob_count.load(Ordering::Relaxed), 0);
        });
    }

    #[test]
    fn load_by_id_resolves_tx_dir_and_materializes_tx_view() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let blob = InMemoryBlobStore::default();
            let tables = Tables::without_cache(meta.clone(), blob);

            tables
                .block_records
                .put(
                    11,
                    &BlockRecord {
                        block_hash: [4u8; 32],
                        parent_hash: [5u8; 32],
                        logs: None,
                        txs: Some(PrimaryWindowRecord {
                            first_primary_id: DIRECTORY_SUB_BUCKET_SIZE,
                            count: 3,
                        }),
                        traces: None,
                    },
                )
                .await
                .expect("block record");

            meta.scan_put(
                TxDirByBlockSpec::TABLE,
                &TxDirByBlockSpec::partition(TxDirSubBucketSpec::sub_bucket_start(
                    DIRECTORY_SUB_BUCKET_SIZE + 1,
                )),
                &TxDirByBlockSpec::clustering(11),
                DirByBlock {
                    block_num: 11,
                    first_primary_id: DIRECTORY_SUB_BUCKET_SIZE,
                    end_primary_id_exclusive: DIRECTORY_SUB_BUCKET_SIZE + 3,
                }
                .encode(),
                PutCond::Any,
            )
            .await
            .expect("dir fragment");

            let envelopes = [
                StoredTxEnvelope {
                    tx_hash: [1u8; 32],
                    sender: [2u8; 20],
                    signed_tx_bytes: vec![3],
                }
                .encode(),
                StoredTxEnvelope {
                    tx_hash: [6u8; 32],
                    sender: [7u8; 20],
                    signed_tx_bytes: vec![8, 9],
                }
                .encode(),
                StoredTxEnvelope {
                    tx_hash: [10u8; 32],
                    sender: [11u8; 20],
                    signed_tx_bytes: vec![12],
                }
                .encode(),
            ];
            let mut blob_bytes = Vec::new();
            let mut offsets = crate::core::offsets::BucketedOffsets::new();
            offsets.push(0).expect("offset");
            for envelope in &envelopes {
                blob_bytes.extend_from_slice(envelope);
                offsets
                    .push(u64::try_from(blob_bytes.len()).expect("blob len"))
                    .expect("offset");
            }
            tables
                .block_tx_blobs
                .put_block(11, Bytes::from(blob_bytes), &BlockTxHeader { offsets })
                .await
                .expect("tx blob");

            let mut materializer = TxMaterializer::new(&tables);
            let tx = QueryMaterializer::load_by_id(
                &mut materializer,
                TxId::new(DIRECTORY_SUB_BUCKET_SIZE + 1),
            )
            .await
            .expect("load by id")
            .expect("tx exists");

            assert_eq!(tx.block_num(), 11);
            assert_eq!(tx.block_hash(), &[4u8; 32]);
            assert_eq!(tx.tx_idx(), 1);
            assert_eq!(tx.tx_hash().expect("tx hash"), &[6u8; 32]);
            assert_eq!(tx.sender().expect("sender"), &[7u8; 20]);
        });
    }
}
