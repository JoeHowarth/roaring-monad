use bytes::Bytes;
use futures::stream::{FuturesUnordered, StreamExt};
use roaring::RoaringBitmap;

use crate::domain::keys::STREAM_PAGE_LOCAL_ID_SPAN;
use crate::domain::table_specs;
use crate::error::{Error, Result};
use crate::logs::index_spec::is_full_shard_range;
use crate::store::traits::{BlobStore, MetaStore};
use crate::streams::bitmap_blob::decode_bitmap_blob;
use crate::tables::Tables;

use super::clause::PreparedShardClause;

pub(in crate::logs) const STREAM_LOAD_CONCURRENCY: usize = 32;

pub(in crate::logs) async fn load_prepared_clause_bitmap<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    prepared_clause: &PreparedShardClause,
    local_from: u32,
    local_to: u32,
) -> Result<RoaringBitmap> {
    let mut out = RoaringBitmap::new();
    let mut in_flight = FuturesUnordered::new();

    for stream_id in &prepared_clause.stream_ids {
        in_flight.push(async move {
            load_stream_entries(tables, stream_id, local_from, local_to).await
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

pub(in crate::logs) async fn fetch_union_log_level_with_cache<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    kind: &str,
    values: &[Vec<u8>],
    from_log_id: crate::core::ids::LogId,
    to_log_id_inclusive: crate::core::ids::LogId,
) -> Result<crate::core::execution::ShardBitmapSet> {
    use std::collections::BTreeMap;

    use crate::core::ids::LogShard;
    let mut out = BTreeMap::new();
    let from_shard = table_specs::log_shard(from_log_id);
    let to_shard = table_specs::log_shard(to_log_id_inclusive);
    let mut in_flight = FuturesUnordered::new();

    for value in values {
        for shard_raw in from_shard.get()..=to_shard.get() {
            let shard = LogShard::new(shard_raw).expect("shard derived from LogId range");
            let stream = table_specs::stream_id(kind, value, shard);
            let (local_from, local_to) =
                table_specs::local_range_for_shard(from_log_id, to_log_id_inclusive, shard);
            in_flight.push(async move {
                let entries =
                    load_stream_entries(tables, &stream, local_from.get(), local_to.get()).await?;
                Ok::<(LogShard, RoaringBitmap), Error>((shard, entries))
            });
            if in_flight.len() >= STREAM_LOAD_CONCURRENCY
                && let Some(result) = in_flight.next().await
            {
                merge_union_entries(&mut out, result?);
            }
        }
    }

    while let Some(result) = in_flight.next().await {
        merge_union_entries(&mut out, result?);
    }

    Ok(out)
}

fn merge_union_entries(
    out: &mut crate::core::execution::ShardBitmapSet,
    (shard, entries): (crate::core::ids::LogShard, RoaringBitmap),
) {
    if entries.is_empty() {
        return;
    }
    out.entry(shard)
        .and_modify(|existing| *existing |= &entries)
        .or_insert(entries);
}

pub(in crate::logs) async fn load_stream_entries<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    stream: &str,
    local_from: u32,
    local_to: u32,
) -> Result<RoaringBitmap> {
    let mut out = RoaringBitmap::new();
    let mut page_start = table_specs::stream_page_start_local(local_from);
    let last_page_start = table_specs::stream_page_start_local(local_to);

    loop {
        if let Some(meta) = load_bitmap_page_meta(tables, stream, page_start).await? {
            if overlaps(meta.min_local, meta.max_local, local_from, local_to) {
                let loaded_page_blob = maybe_merge_cached_bitmap_blob(
                    tables, stream, page_start, &mut out, local_from, local_to,
                )
                .await?;
                if !loaded_page_blob {
                    load_bitmap_by_block_entries_for_page(
                        tables, stream, page_start, local_from, local_to, &mut out,
                    )
                    .await?;
                }
            }
        } else {
            load_bitmap_by_block_entries_for_page(
                tables, stream, page_start, local_from, local_to, &mut out,
            )
            .await?;
        }

        if page_start == last_page_start {
            break;
        }
        page_start = page_start.saturating_add(STREAM_PAGE_LOCAL_ID_SPAN);
    }

    Ok(out)
}

async fn load_bitmap_by_block_entries_for_page<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    stream: &str,
    page_start: u32,
    local_from: u32,
    local_to: u32,
    out: &mut RoaringBitmap,
) -> Result<()> {
    for bytes in tables
        .bitmap_by_block()
        .load_page_fragments(stream, page_start)
        .await?
    {
        let bitmap_blob = decode_bitmap_blob(&bytes)?;
        if !overlaps(
            bitmap_blob.min_local,
            bitmap_blob.max_local,
            local_from,
            local_to,
        ) {
            continue;
        }
        if is_full_shard_range(local_from, local_to)
            || (bitmap_blob.min_local >= local_from && bitmap_blob.max_local <= local_to)
        {
            *out |= &bitmap_blob.bitmap;
            continue;
        }
        for value in bitmap_blob.bitmap {
            if value >= local_from && value <= local_to {
                out.insert(value);
            }
        }
    }
    Ok(())
}

pub(in crate::logs) async fn load_bitmap_page_meta<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    stream: &str,
    page_start: u32,
) -> Result<Option<crate::domain::types::StreamBitmapMeta>> {
    tables.bitmap_page_meta().get(stream, page_start).await
}

async fn load_bitmap_page_blob<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    stream: &str,
    page_start: u32,
) -> Result<Option<Bytes>> {
    tables
        .bitmap_page_blobs()
        .get_for_page(stream, page_start)
        .await
}

async fn maybe_merge_cached_bitmap_blob<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    stream: &str,
    page_start: u32,
    out: &mut RoaringBitmap,
    local_from: u32,
    local_to: u32,
) -> Result<bool> {
    let Some(bytes) = load_bitmap_page_blob(tables, stream, page_start).await? else {
        return Ok(false);
    };
    let bitmap_blob = decode_bitmap_blob(&bytes)?;
    if is_full_shard_range(local_from, local_to)
        || (bitmap_blob.min_local >= local_from && bitmap_blob.max_local <= local_to)
    {
        *out |= &bitmap_blob.bitmap;
        return Ok(true);
    }

    for value in bitmap_blob.bitmap {
        if value >= local_from && value <= local_to {
            out.insert(value);
        }
    }
    Ok(true)
}

pub(in crate::logs) fn overlaps(
    min_local: u32,
    max_local: u32,
    local_from: u32,
    local_to: u32,
) -> bool {
    min_local <= local_to && max_local >= local_from
}

#[cfg(test)]
mod tests {
    use super::load_stream_entries;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use bytes::Bytes;

    use crate::domain::keys::{BITMAP_BY_BLOCK_TABLE, BITMAP_PAGE_META_TABLE};
    use crate::domain::table_specs::{
        BitmapByBlockSpec, BitmapPageBlobSpec, BitmapPageMetaSpec, BlobTableSpec,
    };
    use crate::domain::types::StreamBitmapMeta;
    use crate::store::blob::InMemoryBlobStore;
    use crate::store::meta::InMemoryMetaStore;
    use crate::store::traits::{
        BlobStore, BlobTableId, MetaStore, Page, PutCond, PutResult, Record, ScannableTableId,
        TableId,
    };
    use crate::streams::bitmap_blob::{BitmapBlob, encode_bitmap_blob};
    use crate::tables::{BytesCacheConfig, TableCacheConfig, Tables};
    use futures::executor::block_on;
    use roaring::RoaringBitmap;

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
                    block_num: 0,
                    count: 1,
                    min_local: 11,
                    max_local: 11,
                }
                .encode(),
                PutCond::Any,
            )
            .await
            .expect("write stream page meta");

            let tables = Tables::without_cache(Arc::new(meta), Arc::new(blob));
            let entries = load_stream_entries(&tables, stream, 0, 20)
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
                        block_num: 7,
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
                Arc::new(meta),
                Arc::new(blob),
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

            let first = load_stream_entries(&tables, stream, 0, 20)
                .await
                .expect("first load");
            let second = load_stream_entries(&tables, stream, 0, 20)
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
                        block_num: 7,
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
                Arc::new(meta),
                Arc::new(blob),
                BytesCacheConfig {
                    bitmap_page_meta: TableCacheConfig {
                        max_bytes: 4 * 1024,
                    },
                    ..BytesCacheConfig::disabled()
                },
            );

            let first = load_stream_entries(&tables, stream, 0, 20)
                .await
                .expect("first load");
            let second = load_stream_entries(&tables, stream, 0, 20)
                .await
                .expect("second load");

            assert_eq!(first, second);
            assert_eq!(meta_gets.load(Ordering::Relaxed), 1);
            assert_eq!(blob_gets.load(Ordering::Relaxed), 2);
        });
    }
}
