#[allow(dead_code, unused_imports)]
mod helpers;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use finalized_history_query::api::FinalizedHistoryService;
use finalized_history_query::cache::{BytesCacheConfig, TableCacheConfig};
use finalized_history_query::config::Config;
use finalized_history_query::domain::keys::block_log_blob_key;
use finalized_history_query::store::blob::InMemoryBlobStore;
use finalized_history_query::store::meta::InMemoryMetaStore;
use futures::executor::block_on;

use helpers::*;

#[test]
fn service_reuses_cached_point_log_payloads_across_queries() {
    block_on(async {
        let meta = InMemoryMetaStore::default();
        let target_key = block_log_blob_key(1);
        let get_blob_calls = Arc::new(AtomicU64::new(0));
        let read_range_calls = Arc::new(AtomicU64::new(0));
        let blob = CountingBlobStore {
            inner: Arc::new(InMemoryBlobStore::default()),
            target_key: target_key.clone(),
            get_blob_calls: get_blob_calls.clone(),
            read_range_calls: read_range_calls.clone(),
            read_range_bytes: Arc::new(AtomicU64::new(0)),
        };
        let svc = FinalizedHistoryService::new_reader_writer(
            Config {
                bytes_cache: BytesCacheConfig {
                    point_log_payloads: TableCacheConfig {
                        max_bytes: 1024 * 1024,
                    },
                    ..BytesCacheConfig::disabled()
                },
                ..lease_writer_config()
            },
            meta,
            blob,
            1,
        );

        svc.ingest_finalized_block(mk_block(1, [0; 32], vec![mk_log(7, 10, 20, 1, 0, 0)]))
            .await
            .expect("ingest block");

        let first = query_page(&svc, 1, 1, indexed_address_filter(7), 10, None)
            .await
            .expect("first query");
        let second = query_page(&svc, 1, 1, indexed_address_filter(7), 10, None)
            .await
            .expect("second query");

        assert_eq!(first.items, second.items);
        assert_eq!(get_blob_calls.load(Ordering::Relaxed), 0);
        assert_eq!(read_range_calls.load(Ordering::Relaxed), 1);

        let metrics = svc.cache_metrics();
        assert_eq!(metrics.point_log_payloads.misses, 1);
        assert_eq!(metrics.point_log_payloads.hits, 1);
        assert_eq!(metrics.point_log_payloads.inserts, 1);
        assert_eq!(metrics.point_log_payloads.evictions, 0);
        assert!(metrics.point_log_payloads.bytes_used > 0);
    });
}

#[test]
fn service_coalesces_contiguous_same_block_log_blob_into_one_range_read() {
    block_on(async {
        let meta = InMemoryMetaStore::default();
        let target_key = block_log_blob_key(1);
        let get_blob_calls = Arc::new(AtomicU64::new(0));
        let read_range_calls = Arc::new(AtomicU64::new(0));
        let blob = CountingBlobStore {
            inner: Arc::new(InMemoryBlobStore::default()),
            target_key: target_key.clone(),
            get_blob_calls: get_blob_calls.clone(),
            read_range_calls: read_range_calls.clone(),
            read_range_bytes: Arc::new(AtomicU64::new(0)),
        };
        let svc = FinalizedHistoryService::new_reader_writer(
            Config {
                bytes_cache: BytesCacheConfig {
                    point_log_payloads: TableCacheConfig {
                        max_bytes: 1024 * 1024,
                    },
                    ..BytesCacheConfig::disabled()
                },
                ..lease_writer_config()
            },
            meta,
            blob,
            1,
        );

        svc.ingest_finalized_block(mk_block(
            1,
            [0; 32],
            vec![
                mk_log(7, 10, 20, 1, 0, 0),
                mk_log(7, 10, 21, 1, 0, 1),
                mk_log(7, 10, 22, 1, 0, 2),
            ],
        ))
        .await
        .expect("ingest block");

        let page = query_page(&svc, 1, 1, indexed_address_filter(7), 10, None)
            .await
            .expect("query page");

        assert_eq!(page.items.len(), 3);
        assert_eq!(get_blob_calls.load(Ordering::Relaxed), 0);
        assert_eq!(read_range_calls.load(Ordering::Relaxed), 1);

        let metrics = svc.cache_metrics();
        assert_eq!(metrics.point_log_payloads.inserts, 3);
        assert_eq!(metrics.point_log_payloads.misses, 3);
    });
}

#[test]
fn query_limit_one_does_not_need_full_contiguous_run_bytes() {
    block_on(async {
        let meta = InMemoryMetaStore::default();
        let target_key = block_log_blob_key(1);
        let get_blob_calls = Arc::new(AtomicU64::new(0));
        let read_range_calls = Arc::new(AtomicU64::new(0));
        let read_range_bytes = Arc::new(AtomicU64::new(0));
        let blob = CountingBlobStore {
            inner: Arc::new(InMemoryBlobStore::default()),
            target_key: target_key.clone(),
            get_blob_calls: get_blob_calls.clone(),
            read_range_calls: read_range_calls.clone(),
            read_range_bytes: read_range_bytes.clone(),
        };
        let svc = FinalizedHistoryService::new_reader_writer(
            Config {
                bytes_cache: BytesCacheConfig {
                    point_log_payloads: TableCacheConfig {
                        max_bytes: 1024 * 1024,
                    },
                    ..BytesCacheConfig::disabled()
                },
                ..lease_writer_config()
            },
            meta,
            blob,
            1,
        );

        let logs = vec![
            mk_log(7, 10, 20, 1, 0, 0),
            mk_log(7, 10, 21, 1, 0, 1),
            mk_log(7, 10, 22, 1, 0, 2),
        ];
        let needed_log_bytes = (logs[0].encode().len() + logs[1].encode().len()) as u64;

        svc.ingest_finalized_block(mk_block(1, [0; 32], logs))
            .await
            .expect("ingest block");

        let page = query_page(&svc, 1, 1, indexed_address_filter(7), 1, None)
            .await
            .expect("query page");

        assert_eq!(page.items.len(), 1);
        assert_eq!(get_blob_calls.load(Ordering::Relaxed), 0);
        assert_eq!(read_range_calls.load(Ordering::Relaxed), 1);
        assert_eq!(
            read_range_bytes.load(Ordering::Relaxed),
            needed_log_bytes,
            "query limit 1 should only read the bytes needed for limit + 1 pagination"
        );

        let metrics = svc.cache_metrics();
        assert_eq!(metrics.point_log_payloads.inserts, 2);
        assert_eq!(metrics.point_log_payloads.misses, 2);
    });
}
