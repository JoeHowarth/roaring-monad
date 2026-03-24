use futures::stream::{FuturesUnordered, StreamExt};
use roaring::RoaringBitmap;

use crate::error::Result;
use crate::kernel::sharded_streams::merge_bitmap_bytes_into;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;

use super::planner::PreparedClause;
use super::stream_family::StreamIndexFamily;

pub(crate) const STREAM_LOAD_CONCURRENCY: usize = 32;

pub(crate) async fn load_prepared_clause_bitmap<
    M: MetaStore,
    B: BlobStore,
    K,
    F: StreamIndexFamily<ClauseKind = K>,
>(
    tables: &Tables<M, B>,
    prepared_clause: &PreparedClause<K>,
    local_from: u32,
    local_to: u32,
) -> Result<RoaringBitmap> {
    let mut out = RoaringBitmap::new();
    let mut in_flight = FuturesUnordered::new();

    for stream_id in &prepared_clause.stream_ids {
        in_flight.push(async move {
            load_stream_entries::<M, B, F>(tables, stream_id, local_from, local_to).await
        });
        if in_flight.len() >= STREAM_LOAD_CONCURRENCY
            && let Some(result) = in_flight.next().await
        {
            out |= &result?;
        }
    }

    while let Some(result) = in_flight.next().await {
        out |= &result?;
    }

    Ok(out)
}

pub(crate) async fn load_stream_entries<M: MetaStore, B: BlobStore, F: StreamIndexFamily>(
    tables: &Tables<M, B>,
    stream: &str,
    local_from: u32,
    local_to: u32,
) -> Result<RoaringBitmap> {
    let mut out = RoaringBitmap::new();
    let mut page_start = F::first_page_start(local_from);
    let last_page_start = F::last_page_start(local_to);
    let stream_tables = F::stream_tables(tables);

    loop {
        if let Some(meta) = stream_tables.get_page_meta(stream, page_start).await? {
            if F::meta_overlaps(&meta, local_from, local_to) {
                let loaded_page_blob = maybe_merge_cached_bitmap_blob::<M, B, F>(
                    tables, stream, page_start, &mut out, local_from, local_to,
                )
                .await?;
                if !loaded_page_blob {
                    load_bitmap_by_block_entries_for_page::<M, B, F>(
                        tables, stream, page_start, local_from, local_to, &mut out,
                    )
                    .await?;
                }
            }
        } else {
            load_bitmap_by_block_entries_for_page::<M, B, F>(
                tables, stream, page_start, local_from, local_to, &mut out,
            )
            .await?;
        }

        if page_start == last_page_start {
            break;
        }
        page_start = F::next_page_start(page_start);
    }

    Ok(out)
}

async fn load_bitmap_by_block_entries_for_page<M: MetaStore, B: BlobStore, F: StreamIndexFamily>(
    tables: &Tables<M, B>,
    stream: &str,
    page_start: u32,
    local_from: u32,
    local_to: u32,
    out: &mut RoaringBitmap,
) -> Result<()> {
    for bytes in F::stream_tables(tables)
        .load_page_fragments(stream, page_start)
        .await?
    {
        let _ = merge_bitmap_bytes_into(
            &bytes,
            out,
            local_from,
            local_to,
            F::is_full_shard_range(local_from, local_to),
        )?;
    }
    Ok(())
}

async fn maybe_merge_cached_bitmap_blob<M: MetaStore, B: BlobStore, F: StreamIndexFamily>(
    tables: &Tables<M, B>,
    stream: &str,
    page_start: u32,
    out: &mut RoaringBitmap,
    local_from: u32,
    local_to: u32,
) -> Result<bool> {
    let Some(bytes) = F::stream_tables(tables)
        .get_page_blob(stream, page_start)
        .await?
    else {
        return Ok(false);
    };
    merge_bitmap_bytes_into(
        &bytes,
        out,
        local_from,
        local_to,
        F::is_full_shard_range(local_from, local_to),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use bytes::Bytes;
    use futures::executor::block_on;
    use roaring::RoaringBitmap;

    use super::load_stream_entries;
    use crate::kernel::codec::StorageCodec;
    use crate::logs::keys::{BITMAP_BY_BLOCK_TABLE, BITMAP_PAGE_META_TABLE};
    use crate::logs::query::LogsStreamFamily;
    use crate::logs::table_specs::{
        BitmapByBlockSpec, BitmapPageBlobSpec, BitmapPageMetaSpec, BlobTableSpec,
    };
    use crate::logs::types::StreamBitmapMeta;
    use crate::store::blob::InMemoryBlobStore;
    use crate::store::meta::InMemoryMetaStore;
    use crate::store::traits::{
        BlobStore, BlobTableId, MetaStore, Page, PutCond, PutResult, Record, ScannableTableId,
        TableId,
    };
    use crate::streams::{BitmapBlob, decode_bitmap_blob, encode_bitmap_blob};
    use crate::tables::{BytesCacheConfig, TableCacheConfig, Tables};

    #[derive(Clone)]
    struct CountingMetaStore {
        inner: InMemoryMetaStore,
        target_family: TableId,
        target_key: Vec<u8>,
        get_count: Arc<AtomicU64>,
    }

    impl MetaStore for CountingMetaStore {
        async fn get(
            &self,
            family: crate::store::traits::TableId,
            key: &[u8],
        ) -> crate::Result<Option<Record>> {
            if family == self.target_family && key == self.target_key.as_slice() {
                self.get_count.fetch_add(1, Ordering::Relaxed);
            }
            self.inner.get(family, key).await
        }

        async fn put(
            &self,
            family: crate::store::traits::TableId,
            key: &[u8],
            value: Bytes,
            cond: PutCond,
        ) -> crate::Result<PutResult> {
            self.inner.put(family, key, value, cond).await
        }

        async fn delete(
            &self,
            family: crate::store::traits::TableId,
            key: &[u8],
            cond: crate::store::traits::DelCond,
        ) -> crate::Result<()> {
            self.inner.delete(family, key, cond).await
        }

        async fn scan_get(
            &self,
            family: ScannableTableId,
            partition: &[u8],
            clustering: &[u8],
        ) -> crate::Result<Option<Record>> {
            self.inner.scan_get(family, partition, clustering).await
        }

        async fn scan_put(
            &self,
            family: ScannableTableId,
            partition: &[u8],
            clustering: &[u8],
            value: Bytes,
            cond: PutCond,
        ) -> crate::Result<PutResult> {
            self.inner
                .scan_put(family, partition, clustering, value, cond)
                .await
        }

        async fn scan_delete(
            &self,
            family: ScannableTableId,
            partition: &[u8],
            clustering: &[u8],
            cond: crate::store::traits::DelCond,
        ) -> crate::Result<()> {
            self.inner
                .scan_delete(family, partition, clustering, cond)
                .await
        }

        async fn scan_list(
            &self,
            family: ScannableTableId,
            partition: &[u8],
            prefix: &[u8],
            cursor: Option<Vec<u8>>,
            limit: usize,
        ) -> crate::Result<Page> {
            self.inner
                .scan_list(family, partition, prefix, cursor, limit)
                .await
        }
    }

    #[derive(Clone)]
    struct CountingBlobStore {
        inner: InMemoryBlobStore,
        target_key: Vec<u8>,
        get_blob_count: Arc<AtomicU64>,
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
            if table == BitmapPageBlobSpec::TABLE && key == self.target_key.as_slice() {
                self.get_blob_count.fetch_add(1, Ordering::Relaxed);
            }
            self.inner.get_blob(table, key).await
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
    fn load_stream_entries_falls_back_to_fragments_when_page_blob_is_missing() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let blob = InMemoryBlobStore::default();
            let stream = "addr/test/00000000";
            let page_start = 0u32;
            let block_num = 7u64;

            let mut fragment_bitmap = RoaringBitmap::new();
            fragment_bitmap.insert(11);
            let fragment_bitmap_blob = BitmapBlob {
                min_local: 11,
                max_local: 11,
                count: 1,
                crc32: 0,
                bitmap: fragment_bitmap,
            };
            let partition = BitmapByBlockSpec::partition(stream, page_start);

            meta.scan_put(
                BITMAP_BY_BLOCK_TABLE,
                &partition,
                &BitmapByBlockSpec::clustering(block_num),
                encode_bitmap_blob(&fragment_bitmap_blob).expect("encode fragment bitmap blob"),
                PutCond::Any,
            )
            .await
            .expect("write stream fragment");

            meta.put(
                BITMAP_PAGE_META_TABLE,
                &BitmapPageMetaSpec::key(stream, page_start),
                StreamBitmapMeta {
                    count: 1,
                    min_local: 11,
                    max_local: 11,
                }
                .encode(),
                PutCond::Any,
            )
            .await
            .expect("write stream page meta");

            let tables = Tables::without_cache(meta, blob);
            let entries = load_stream_entries::<_, _, LogsStreamFamily>(&tables, stream, 0, 20)
                .await
                .expect("load stream entries");

            assert!(entries.contains(11));
            assert_eq!(entries.len(), 1);
        });
    }

    #[test]
    fn load_stream_entries_reuses_cached_bitmap_page_meta_and_blob() {
        block_on(async {
            let stream = "addr/test/00000000";
            let page_start = 0u32;
            let meta_key = BitmapPageMetaSpec::key(stream, page_start);
            let blob_key = BitmapPageBlobSpec::key(stream, page_start);

            let inner_meta = InMemoryMetaStore::default();
            let inner_blob = InMemoryBlobStore::default();
            let meta_gets = Arc::new(AtomicU64::new(0));
            let blob_gets = Arc::new(AtomicU64::new(0));

            let mut bitmap = RoaringBitmap::new();
            bitmap.insert(11);
            let bitmap_blob = BitmapBlob {
                min_local: 11,
                max_local: 11,
                count: 1,
                crc32: 0,
                bitmap,
            };

            inner_meta
                .put(
                    BITMAP_PAGE_META_TABLE,
                    &meta_key,
                    StreamBitmapMeta {
                        count: 1,
                        min_local: 11,
                        max_local: 11,
                    }
                    .encode(),
                    PutCond::Any,
                )
                .await
                .expect("write stream page meta");
            inner_blob
                .put_blob(
                    BitmapPageBlobSpec::TABLE,
                    &blob_key,
                    encode_bitmap_blob(&bitmap_blob).expect("encode stream page bitmap blob"),
                )
                .await
                .expect("write stream page blob");

            let meta = CountingMetaStore {
                inner: inner_meta,
                target_family: BITMAP_PAGE_META_TABLE,
                target_key: meta_key.clone(),
                get_count: meta_gets.clone(),
            };
            let blob = CountingBlobStore {
                inner: inner_blob,
                target_key: blob_key.clone(),
                get_blob_count: blob_gets.clone(),
            };
            let tables = Tables::new(
                meta,
                blob,
                BytesCacheConfig {
                    bitmap_page_meta: TableCacheConfig {
                        max_bytes: 4 * 1024,
                    },
                    bitmap_page_blobs: TableCacheConfig {
                        max_bytes: 4 * 1024,
                    },
                    ..BytesCacheConfig::disabled()
                },
            );

            let first = load_stream_entries::<_, _, LogsStreamFamily>(&tables, stream, 0, 20)
                .await
                .expect("first load");
            let second = load_stream_entries::<_, _, LogsStreamFamily>(&tables, stream, 0, 20)
                .await
                .expect("second load");

            assert_eq!(first, second);
            assert_eq!(meta_gets.load(Ordering::Relaxed), 1);
            assert_eq!(blob_gets.load(Ordering::Relaxed), 1);
        });
    }

    #[test]
    fn load_stream_entries_can_cache_meta_without_caching_blobs() {
        block_on(async {
            let stream = "addr/test/00000000";
            let page_start = 0u32;
            let meta_key = BitmapPageMetaSpec::key(stream, page_start);
            let blob_key = BitmapPageBlobSpec::key(stream, page_start);

            let inner_meta = InMemoryMetaStore::default();
            let inner_blob = InMemoryBlobStore::default();
            let meta_gets = Arc::new(AtomicU64::new(0));
            let blob_gets = Arc::new(AtomicU64::new(0));

            let mut bitmap = RoaringBitmap::new();
            bitmap.insert(11);
            let bitmap_blob = BitmapBlob {
                min_local: 11,
                max_local: 11,
                count: 1,
                crc32: 0,
                bitmap,
            };

            inner_meta
                .put(
                    BITMAP_PAGE_META_TABLE,
                    &meta_key,
                    StreamBitmapMeta {
                        count: 1,
                        min_local: 11,
                        max_local: 11,
                    }
                    .encode(),
                    PutCond::Any,
                )
                .await
                .expect("write stream page meta");
            inner_blob
                .put_blob(
                    BitmapPageBlobSpec::TABLE,
                    &blob_key,
                    encode_bitmap_blob(&bitmap_blob).expect("encode stream page bitmap blob"),
                )
                .await
                .expect("write stream page blob");

            let meta = CountingMetaStore {
                inner: inner_meta,
                target_family: BITMAP_PAGE_META_TABLE,
                target_key: meta_key.clone(),
                get_count: meta_gets.clone(),
            };
            let blob = CountingBlobStore {
                inner: inner_blob,
                target_key: blob_key.clone(),
                get_blob_count: blob_gets.clone(),
            };
            let tables = Tables::new(
                meta,
                blob,
                BytesCacheConfig {
                    bitmap_page_meta: TableCacheConfig {
                        max_bytes: 4 * 1024,
                    },
                    ..BytesCacheConfig::disabled()
                },
            );

            let first = load_stream_entries::<_, _, LogsStreamFamily>(&tables, stream, 0, 20)
                .await
                .expect("first load");
            let second = load_stream_entries::<_, _, LogsStreamFamily>(&tables, stream, 0, 20)
                .await
                .expect("second load");

            assert_eq!(first, second);
            assert_eq!(meta_gets.load(Ordering::Relaxed), 1);
            assert_eq!(blob_gets.load(Ordering::Relaxed), 2);
        });
    }

    #[test]
    fn load_page_fragments_preserves_full_partition_coverage() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let blob = InMemoryBlobStore::default();
            let stream = "addr/test/00000000";
            let page_start = 0u32;

            for (block_num, local) in [(9u64, 9u32), (7u64, 7u32), (8u64, 8u32)] {
                let mut bitmap = RoaringBitmap::new();
                bitmap.insert(local);
                let blob = BitmapBlob {
                    min_local: local,
                    max_local: local,
                    count: 1,
                    crc32: 0,
                    bitmap,
                };
                meta.scan_put(
                    BITMAP_BY_BLOCK_TABLE,
                    &BitmapByBlockSpec::partition(stream, page_start),
                    &BitmapByBlockSpec::clustering(block_num),
                    encode_bitmap_blob(&blob).expect("encode fragment bitmap blob"),
                    PutCond::Any,
                )
                .await
                .expect("write page fragment");
            }

            let tables = Tables::without_cache(meta, blob);
            let fragments = tables
                .log_streams
                .load_page_fragments(stream, page_start)
                .await
                .expect("load page fragments");

            let decoded = fragments
                .into_iter()
                .map(|bytes| decode_bitmap_blob(&bytes).expect("decode bitmap fragment"))
                .collect::<Vec<_>>();
            assert_eq!(decoded.len(), 3);
            for local in [7u32, 8u32, 9u32] {
                assert!(decoded.iter().any(|blob| blob.bitmap.contains(local)));
            }
        });
    }
}
