use crate::core::directory_resolver::{ResolvedPrimaryLocation, resolve_primary_id};
use crate::core::ids::TraceId;
use crate::core::refs::BlockRef;
use crate::error::Result;
use crate::query::runner::{MaterializerCaches, QueryMaterializer, cached_parent_block_ref};
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;
use crate::traces::filter::{TraceFilter, exact_match};
use crate::traces::view::TraceRef;

pub struct TraceMaterializer<'a, M: MetaStore, B: BlobStore> {
    tables: &'a Tables<M, B>,
    caches: MaterializerCaches<crate::traces::types::DirByBlock>,
}

impl<'a, M: MetaStore, B: BlobStore> TraceMaterializer<'a, M, B> {
    pub fn new(tables: &'a Tables<M, B>) -> Self {
        Self {
            tables,
            caches: MaterializerCaches::default(),
        }
    }
}

impl<M: MetaStore, B: BlobStore> QueryMaterializer for TraceMaterializer<'_, M, B> {
    type Id = TraceId;
    type Item = TraceRef;
    type Filter = TraceFilter;
    type Output = TraceRef;

    async fn resolve_id(&mut self, id: Self::Id) -> Result<Option<ResolvedPrimaryLocation>> {
        Ok(resolve_primary_id::<M, TraceId>(
            &self.tables.trace_dir,
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
        let Some((_, first)) = run.first().copied() else {
            return Ok(Vec::new());
        };
        let last = run.last().expect("run must be non-empty").1;
        let run_items = self
            .tables
            .block_trace_blobs
            .load_contiguous_run(first.block_num, first.local_ordinal, last.local_ordinal)
            .await?;
        if run_items.len() != run.len() {
            return Err(crate::error::Error::NotFound);
        }
        Ok(run
            .iter()
            .copied()
            .map(|(id, _)| id)
            .zip(run_items)
            .collect())
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

    use alloy_rlp::Encodable;
    use bytes::Bytes;
    use futures::executor::block_on;

    use crate::core::offsets::BucketedOffsets;
    use crate::core::state::BlockRecord;
    use crate::kernel::codec::StorageCodec;
    use crate::kernel::table_specs::{BlobTableSpec, PointTableSpec};
    use crate::store::blob::InMemoryBlobStore;
    use crate::store::meta::InMemoryMetaStore;
    use crate::store::traits::{BlobStore, BlobTableId, MetaStore, Page, PutCond};
    use crate::tables::{BytesCacheConfig, TableCacheConfig, Tables};
    use crate::traces::table_specs::{BlockTraceBlobSpec, BlockTraceHeaderSpec};
    use crate::traces::types::BlockTraceHeader;

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
            if table == BlockTraceBlobSpec::TABLE {
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
            if table == BlockTraceBlobSpec::TABLE {
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

    fn encode_field<T: Encodable>(value: T) -> Vec<u8> {
        let mut out = Vec::new();
        value.encode(&mut out);
        out
    }

    fn encode_bytes(value: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        value.encode(&mut out);
        out
    }

    fn encode_frame(from: [u8; 20], to: Option<[u8; 20]>, input: &[u8], depth: u64) -> Vec<u8> {
        let fields = vec![
            encode_field(0u8),
            encode_field(0u64),
            encode_bytes(&from),
            encode_bytes(to.as_ref().map(<[u8; 20]>::as_slice).unwrap_or(&[])),
            encode_bytes(&[]),
            encode_field(100u64),
            encode_field(90u64),
            encode_bytes(input),
            encode_bytes(&[]),
            encode_field(1u8),
            encode_field(depth),
        ];
        let mut out = Vec::new();
        alloy_rlp::Header {
            list: true,
            payload_length: fields.iter().map(Vec::len).sum(),
        }
        .encode(&mut out);
        for field in fields {
            out.extend_from_slice(&field);
        }
        out
    }

    #[test]
    fn load_contiguous_run_reads_one_range_and_then_hits_point_cache() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let blob = CountingBlobStore {
                inner: InMemoryBlobStore::default(),
                get_blob_count: Arc::new(AtomicU64::new(0)),
                read_range_count: Arc::new(AtomicU64::new(0)),
            };
            let blob_writer = blob.clone();
            let get_blob_count = Arc::clone(&blob.get_blob_count);
            let read_range_count = Arc::clone(&blob.read_range_count);
            let tables = Tables::new(
                meta.clone(),
                blob,
                BytesCacheConfig {
                    block_trace_blobs: TableCacheConfig {
                        max_bytes: 4 * 1024,
                    },
                    ..BytesCacheConfig::disabled()
                },
            );

            tables
                .block_records
                .put(
                    7,
                    &BlockRecord {
                        block_hash: [9u8; 32],
                        parent_hash: [8u8; 32],
                        logs: None,
                        txs: None,
                        traces: None,
                    },
                )
                .await
                .expect("block record");

            let frame0 = encode_frame([1u8; 20], Some([2u8; 20]), &[0xaa, 0xbb, 0xcc, 0xdd], 0);
            let frame1 = encode_frame([3u8; 20], Some([4u8; 20]), &[0x11, 0x22, 0x33, 0x44], 1);
            let mut blob_bytes = Vec::new();
            blob_bytes.extend_from_slice(&frame0);
            blob_bytes.extend_from_slice(&frame1);
            let mut offsets = BucketedOffsets::new();
            offsets.push(0).expect("offset");
            offsets
                .push(u64::try_from(frame0.len()).expect("frame0 len"))
                .expect("offset");
            offsets
                .push(u64::try_from(blob_bytes.len()).expect("blob len"))
                .expect("offset");
            let header = BlockTraceHeader {
                encoding_version: 1,
                offsets,
                tx_starts: vec![0, 1],
            };

            let _ = meta
                .put(
                    BlockTraceHeaderSpec::TABLE,
                    &BlockTraceHeaderSpec::key(7),
                    header.encode(),
                    PutCond::Any,
                )
                .await
                .expect("trace header");
            blob_writer
                .put_blob(
                    BlockTraceBlobSpec::TABLE,
                    &BlockTraceBlobSpec::key(7),
                    Bytes::from(blob_bytes),
                )
                .await
                .expect("trace blob");

            let first_run = tables
                .block_trace_blobs
                .load_contiguous_run(7, 0, 1)
                .await
                .expect("load first run");
            assert_eq!(first_run.len(), 2);
            assert_eq!(read_range_count.load(Ordering::Relaxed), 1);
            assert_eq!(get_blob_count.load(Ordering::Relaxed), 0);

            let second_run = tables
                .block_trace_blobs
                .load_contiguous_run(7, 0, 1)
                .await
                .expect("load second run");
            assert_eq!(second_run.len(), 2);
            assert_eq!(read_range_count.load(Ordering::Relaxed), 1);
            assert_eq!(get_blob_count.load(Ordering::Relaxed), 0);

            let metrics = tables.metrics_snapshot().block_trace_blobs;
            assert_eq!(metrics.inserts, 2);
            assert!(metrics.hits >= 2);
        });
    }

    #[test]
    fn put_block_warms_the_same_trace_cache_used_by_reads() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let blob = CountingBlobStore {
                inner: InMemoryBlobStore::default(),
                get_blob_count: Arc::new(AtomicU64::new(0)),
                read_range_count: Arc::new(AtomicU64::new(0)),
            };
            let get_blob_count = Arc::clone(&blob.get_blob_count);
            let read_range_count = Arc::clone(&blob.read_range_count);
            let tables = Tables::new(
                meta,
                blob,
                BytesCacheConfig {
                    block_trace_blobs: TableCacheConfig {
                        max_bytes: 4 * 1024,
                    },
                    ..BytesCacheConfig::disabled()
                },
            );

            tables
                .block_records
                .put(
                    7,
                    &BlockRecord {
                        block_hash: [9u8; 32],
                        parent_hash: [8u8; 32],
                        logs: None,
                        txs: None,
                        traces: None,
                    },
                )
                .await
                .expect("block record");

            let frame0 = encode_frame([1u8; 20], Some([2u8; 20]), &[0xaa, 0xbb, 0xcc, 0xdd], 0);
            let frame1 = encode_frame([3u8; 20], Some([4u8; 20]), &[0x11, 0x22, 0x33, 0x44], 1);
            let mut blob_bytes = Vec::new();
            blob_bytes.extend_from_slice(&frame0);
            blob_bytes.extend_from_slice(&frame1);
            let mut offsets = BucketedOffsets::new();
            offsets.push(0).expect("offset");
            offsets
                .push(u64::try_from(frame0.len()).expect("frame0 len"))
                .expect("offset");
            offsets
                .push(u64::try_from(blob_bytes.len()).expect("blob len"))
                .expect("offset");
            let header = BlockTraceHeader {
                encoding_version: 1,
                offsets,
                tx_starts: vec![0, 1],
            };

            tables
                .block_trace_blobs
                .put_block(7, Bytes::from(blob_bytes), &header)
                .await
                .expect("put trace block");

            let run = tables
                .block_trace_blobs
                .load_contiguous_run(7, 0, 1)
                .await
                .expect("load run");

            assert_eq!(run.len(), 2);
            assert_eq!(read_range_count.load(Ordering::Relaxed), 0);
            assert_eq!(get_blob_count.load(Ordering::Relaxed), 0);
        });
    }
}
