use std::fs;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use finalized_log_index::api::{FinalizedIndexService, FinalizedLogIndex};
use finalized_log_index::codec::chunk::{ChunkBlob, encode_chunk};
use finalized_log_index::codec::log::{encode_block_meta, encode_meta_state};
use finalized_log_index::codec::manifest::{Manifest, decode_manifest, encode_manifest};
use finalized_log_index::config::{BroadQueryPolicy, Config, GuardrailAction, IngestMode};
use finalized_log_index::domain::filter::{Clause, LogFilter, QueryOptions};
use finalized_log_index::domain::keys::{
    MAX_LOCAL_ID, META_STATE_KEY, block_hash_to_num_key, compose_global_log_id, log_shard,
    manifest_key, stream_id,
};
use finalized_log_index::domain::types::{Block, BlockMeta, Log, MetaState};
use finalized_log_index::error::Error;
use finalized_log_index::store::blob::InMemoryBlobStore;
use finalized_log_index::store::fs::{FsBlobStore, FsMetaStore};
use finalized_log_index::store::meta::InMemoryMetaStore;
use finalized_log_index::store::traits::{
    BlobStore, DelCond, FenceToken, MetaStore, Page, PutCond, PutResult, Record,
};
use futures::executor::block_on;
use roaring::RoaringBitmap;

fn mk_log(address: u8, topic0: u8, topic1: u8, block_num: u64, tx_idx: u32, log_idx: u32) -> Log {
    Log {
        address: [address; 20],
        topics: vec![[topic0; 32], [topic1; 32]],
        data: vec![address, topic0, topic1],
        block_num,
        tx_idx,
        log_idx,
        block_hash: [block_num as u8; 32],
    }
}

fn mk_block(block_num: u64, parent_hash: [u8; 32], logs: Vec<Log>) -> Block {
    Block {
        block_num,
        block_hash: [block_num as u8; 32],
        parent_hash,
        logs,
    }
}

struct FlakyMetaStore {
    inner: InMemoryMetaStore,
    fail_get_remaining: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl MetaStore for FlakyMetaStore {
    async fn get(
        &self,
        key: &[u8],
    ) -> finalized_log_index::error::Result<Option<finalized_log_index::store::traits::Record>>
    {
        if self.fail_get_remaining.load(Ordering::Relaxed) > 0 {
            self.fail_get_remaining.fetch_sub(1, Ordering::Relaxed);
            return Err(Error::Backend("injected backend get failure".to_string()));
        }
        self.inner.get(key).await
    }

    async fn put(
        &self,
        key: &[u8],
        value: bytes::Bytes,
        cond: finalized_log_index::store::traits::PutCond,
        fence: FenceToken,
    ) -> finalized_log_index::error::Result<finalized_log_index::store::traits::PutResult> {
        self.inner.put(key, value, cond, fence).await
    }

    async fn delete(
        &self,
        key: &[u8],
        cond: finalized_log_index::store::traits::DelCond,
        fence: FenceToken,
    ) -> finalized_log_index::error::Result<()> {
        self.inner.delete(key, cond, fence).await
    }

    async fn list_prefix(
        &self,
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> finalized_log_index::error::Result<finalized_log_index::store::traits::Page> {
        self.inner.list_prefix(prefix, cursor, limit).await
    }
}

#[derive(Default)]
struct RecordingMetaStore {
    inner: InMemoryMetaStore,
    puts: Mutex<Vec<(Vec<u8>, PutCond)>>,
}

impl RecordingMetaStore {
    fn count_puts_with_prefix<F>(&self, prefix: &[u8], mut pred: F) -> usize
    where
        F: FnMut(PutCond) -> bool,
    {
        self.puts
            .lock()
            .map(|puts| {
                puts.iter()
                    .filter(|(k, cond)| k.starts_with(prefix) && pred(*cond))
                    .count()
            })
            .unwrap_or(0)
    }
}

#[derive(Default)]
struct RecordingBlobStore {
    inner: InMemoryBlobStore,
    gets: Mutex<Vec<Vec<u8>>>,
}

impl RecordingBlobStore {
    fn count_gets_with_prefix(&self, prefix: &[u8]) -> usize {
        self.gets
            .lock()
            .map(|gets| gets.iter().filter(|key| key.starts_with(prefix)).count())
            .unwrap_or(0)
    }
}

#[async_trait::async_trait]
impl BlobStore for RecordingBlobStore {
    async fn put_blob(
        &self,
        key: &[u8],
        value: bytes::Bytes,
    ) -> finalized_log_index::error::Result<()> {
        self.inner.put_blob(key, value).await
    }

    async fn get_blob(
        &self,
        key: &[u8],
    ) -> finalized_log_index::error::Result<Option<bytes::Bytes>> {
        if let Ok(mut gets) = self.gets.lock() {
            gets.push(key.to_vec());
        }
        self.inner.get_blob(key).await
    }

    async fn delete_blob(&self, key: &[u8]) -> finalized_log_index::error::Result<()> {
        self.inner.delete_blob(key).await
    }

    async fn list_prefix(
        &self,
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> finalized_log_index::error::Result<Page> {
        self.inner.list_prefix(prefix, cursor, limit).await
    }
}

#[async_trait::async_trait]
impl MetaStore for RecordingMetaStore {
    async fn get(&self, key: &[u8]) -> finalized_log_index::error::Result<Option<Record>> {
        self.inner.get(key).await
    }

    async fn put(
        &self,
        key: &[u8],
        value: bytes::Bytes,
        cond: PutCond,
        fence: FenceToken,
    ) -> finalized_log_index::error::Result<PutResult> {
        if let Ok(mut puts) = self.puts.lock() {
            puts.push((key.to_vec(), cond));
        }
        self.inner.put(key, value, cond, fence).await
    }

    async fn delete(
        &self,
        key: &[u8],
        cond: DelCond,
        fence: FenceToken,
    ) -> finalized_log_index::error::Result<()> {
        self.inner.delete(key, cond, fence).await
    }

    async fn list_prefix(
        &self,
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> finalized_log_index::error::Result<Page> {
        self.inner.list_prefix(prefix, cursor, limit).await
    }
}

#[test]
fn ingest_and_query_with_limits() {
    block_on(async {
        let config = Config {
            target_entries_per_chunk: 2,
            planner_max_or_terms: 8,
            ..Config::default()
        };
        let svc = FinalizedIndexService::new(
            config,
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        let b1 = mk_block(
            1,
            [0; 32],
            vec![mk_log(1, 10, 20, 1, 0, 0), mk_log(2, 11, 21, 1, 0, 1)],
        );
        let b2 = mk_block(
            2,
            b1.block_hash,
            vec![mk_log(1, 10, 22, 2, 0, 0), mk_log(3, 12, 23, 2, 0, 1)],
        );

        svc.ingest_finalized_block(b1).await.expect("ingest b1");
        svc.ingest_finalized_block(b2).await.expect("ingest b2");

        let filter = LogFilter {
            from_block: Some(1),
            to_block: Some(2),
            block_hash: None,
            address: Some(Clause::One([1; 20])),
            topic0: Some(Clause::One([10; 32])),
            topic1: None,
            topic2: None,
            topic3: None,
        };

        let all = svc
            .query_finalized(filter.clone(), QueryOptions { max_results: None })
            .await
            .expect("query all");
        assert_eq!(all.len(), 2);

        let limited = svc
            .query_finalized(
                filter,
                QueryOptions {
                    max_results: Some(1),
                },
            )
            .await
            .expect("query limited");
        assert_eq!(limited.len(), 1);
    });
}

#[test]
fn block_hash_mode_and_invalid_params() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config::default(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        let b1 = mk_block(1, [0; 32], vec![mk_log(1, 10, 20, 1, 0, 0)]);
        svc.ingest_finalized_block(b1.clone())
            .await
            .expect("ingest b1");

        let ok = svc
            .query_finalized(
                LogFilter {
                    from_block: None,
                    to_block: None,
                    block_hash: Some(b1.block_hash),
                    address: None,
                    topic0: Some(Clause::One([10; 32])),
                    topic1: None,
                    topic2: None,
                    topic3: None,
                },
                QueryOptions::default(),
            )
            .await
            .expect("block hash query");
        assert_eq!(ok.len(), 1);

        let err = svc
            .query_finalized(
                LogFilter {
                    from_block: Some(1),
                    to_block: Some(1),
                    block_hash: Some(b1.block_hash),
                    address: None,
                    topic0: None,
                    topic1: None,
                    topic2: None,
                    topic3: None,
                },
                QueryOptions::default(),
            )
            .await
            .expect_err("invalid params expected");
        assert!(matches!(err, Error::InvalidParams(_)));
    });
}

#[test]
fn or_guardrail_is_enforced() {
    block_on(async {
        let config = Config {
            planner_max_or_terms: 2,
            ..Config::default()
        };
        let svc = FinalizedIndexService::new(
            config,
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        let b1 = mk_block(1, [0; 32], vec![mk_log(1, 10, 20, 1, 0, 0)]);
        svc.ingest_finalized_block(b1).await.expect("ingest b1");

        let err = svc
            .query_finalized(
                LogFilter {
                    from_block: Some(1),
                    to_block: Some(1),
                    block_hash: None,
                    address: Some(Clause::Or(vec![[1; 20], [2; 20], [3; 20]])),
                    topic0: None,
                    topic1: None,
                    topic2: None,
                    topic3: None,
                },
                QueryOptions::default(),
            )
            .await
            .expect_err("too broad");

        assert!(matches!(err, Error::QueryTooBroad { actual: 3, max: 2 }));
    });
}

#[test]
fn broad_query_can_fallback_to_block_scan() {
    block_on(async {
        let config = Config {
            planner_max_or_terms: 1,
            planner_broad_query_policy: BroadQueryPolicy::BlockScan,
            ..Config::default()
        };
        let svc = FinalizedIndexService::new(
            config,
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        let b1 = mk_block(
            1,
            [0; 32],
            vec![mk_log(1, 10, 20, 1, 0, 0), mk_log(2, 10, 21, 1, 0, 1)],
        );
        svc.ingest_finalized_block(b1).await.expect("ingest b1");

        // Two OR terms exceed max_or_terms=1, but policy says block-scan fallback.
        let got = svc
            .query_finalized(
                LogFilter {
                    from_block: Some(1),
                    to_block: Some(1),
                    block_hash: None,
                    address: Some(Clause::Or(vec![[1; 20], [2; 20]])),
                    topic0: Some(Clause::One([10; 32])),
                    topic1: None,
                    topic2: None,
                    topic3: None,
                },
                QueryOptions::default(),
            )
            .await
            .expect("fallback block scan query");

        assert_eq!(got.len(), 2);
    });
}

#[test]
fn fs_store_adapter_roundtrip() {
    block_on(async {
        let root = std::env::temp_dir().join(format!(
            "finalized-index-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        fs::create_dir_all(&root).expect("mkdir");

        let meta = FsMetaStore::new(&root, 1).expect("meta store");
        let blob = FsBlobStore::new(&root).expect("blob store");
        let config = Config {
            target_entries_per_chunk: 1,
            ..Config::default()
        };
        let svc = FinalizedIndexService::new(config, meta, blob, 1);

        let b1 = mk_block(1, [0; 32], vec![mk_log(9, 5, 4, 1, 0, 0)]);
        svc.ingest_finalized_block(b1).await.expect("ingest");

        let got = svc
            .query_finalized(
                LogFilter {
                    from_block: Some(1),
                    to_block: Some(1),
                    block_hash: None,
                    address: Some(Clause::One([9; 20])),
                    topic0: Some(Clause::One([5; 32])),
                    topic1: None,
                    topic2: None,
                    topic3: None,
                },
                QueryOptions::default(),
            )
            .await
            .expect("query");
        assert_eq!(got.len(), 1);

        let _ = fs::remove_dir_all(root);
    });
}

#[test]
fn ingest_always_writes_topic0_log_for_cold_signature() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config {
                target_entries_per_chunk: 1,
                ..Config::default()
            },
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        let sig = [0x55; 32];
        let mut parent = [0u8; 32];
        for b in 1..=3001u64 {
            let logs = if b == 1 || b == 3001 {
                vec![mk_log(1, sig[0], 20, b, 0, 0)]
            } else {
                Vec::new()
            };
            let block = mk_block(b, parent, logs);
            parent = block.block_hash;
            svc.ingest_finalized_block(block).await.expect("ingest");
        }

        let sid = stream_id("topic0_log", &sig, log_shard(0));
        let rec = svc
            .ingest
            .meta_store
            .get(&manifest_key(&sid))
            .await
            .expect("manifest get")
            .expect("manifest exists");
        let manifest = decode_manifest(&rec.value).expect("decode manifest");
        assert_eq!(manifest.approx_count, 2);
    });
}

#[test]
fn seals_by_chunk_bytes_threshold() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config {
                target_entries_per_chunk: 10_000,
                target_chunk_bytes: 1,
                maintenance_seal_seconds: 60_000,
                ..Config::default()
            },
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        let b1 = mk_block(1, [0; 32], vec![mk_log(9, 9, 9, 1, 0, 0)]);
        svc.ingest_finalized_block(b1).await.expect("ingest");

        let sid = stream_id("addr", &[9; 20], 0);
        let rec = svc
            .ingest
            .meta_store
            .get(&manifest_key(&sid))
            .await
            .expect("manifest get")
            .expect("manifest");
        let manifest = decode_manifest(&rec.value).expect("decode manifest");
        assert_eq!(manifest.chunk_refs.len(), 1);
    });
}

#[test]
fn periodic_maintenance_seals_old_tails() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config {
                target_entries_per_chunk: 10_000,
                target_chunk_bytes: usize::MAX / 2,
                maintenance_seal_seconds: 1,
                ..Config::default()
            },
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        let b1 = mk_block(1, [0; 32], vec![mk_log(7, 7, 7, 1, 0, 0)]);
        svc.ingest_finalized_block(b1).await.expect("ingest");

        let sid = stream_id("addr", &[7; 20], 0);
        let mkey = manifest_key(&sid);
        let mut manifest = svc
            .ingest
            .meta_store
            .get(&mkey)
            .await
            .expect("manifest get")
            .map(|rec| decode_manifest(&rec.value).expect("decode manifest"))
            .unwrap_or_else(Manifest::default);
        assert_eq!(manifest.chunk_refs.len(), 0);

        manifest.last_seal_unix_sec = 1;
        let _ = svc
            .ingest
            .meta_store
            .put(
                &mkey,
                encode_manifest(&manifest),
                PutCond::Any,
                FenceToken(1),
            )
            .await
            .expect("seed old seal time");

        let stats = svc
            .ingest
            .run_periodic_maintenance(1)
            .await
            .expect("maintenance");
        assert!(stats.flushed_streams >= 1);
        assert!(stats.sealed_streams >= 1);

        let rec = svc
            .ingest
            .meta_store
            .get(&mkey)
            .await
            .expect("manifest get")
            .expect("manifest");
        let manifest = decode_manifest(&rec.value).expect("decode manifest");
        assert_eq!(manifest.chunk_refs.len(), 1);
    });
}

#[test]
fn query_only_loads_overlapping_address_chunks() {
    block_on(async {
        let blob = RecordingBlobStore::default();
        let svc = FinalizedIndexService::new(
            Config {
                target_entries_per_chunk: 1,
                planner_max_or_terms: 8,
                ..Config::default()
            },
            InMemoryMetaStore::default(),
            blob,
            1,
        );

        let b1 = mk_block(1, [0; 32], vec![mk_log(9, 1, 1, 1, 0, 0)]);
        let b2 = mk_block(2, b1.block_hash, vec![mk_log(9, 2, 2, 2, 0, 0)]);
        let b3 = mk_block(3, b2.block_hash, vec![mk_log(9, 3, 3, 3, 0, 0)]);
        svc.ingest_finalized_block(b1).await.expect("ingest b1");
        svc.ingest_finalized_block(b2).await.expect("ingest b2");
        svc.ingest_finalized_block(b3).await.expect("ingest b3");

        let got = svc
            .query_finalized(
                LogFilter {
                    from_block: Some(2),
                    to_block: Some(2),
                    block_hash: None,
                    address: Some(Clause::One([9; 20])),
                    topic0: None,
                    topic1: None,
                    topic2: None,
                    topic3: None,
                },
                QueryOptions::default(),
            )
            .await
            .expect("query");
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].block_num, 2);

        let addr_prefix = b"chunks/addr/";
        let log_pack_prefix = b"log_packs/";
        assert_eq!(svc.ingest.blob_store.count_gets_with_prefix(addr_prefix), 1);
        assert_eq!(
            svc.ingest
                .blob_store
                .count_gets_with_prefix(log_pack_prefix),
            1
        );
    });
}

#[test]
fn topic0_queries_use_only_topic0_log_chunks() {
    block_on(async {
        let blob = RecordingBlobStore::default();
        let svc = FinalizedIndexService::new(
            Config {
                target_entries_per_chunk: 1,
                planner_max_or_terms: 8,
                ..Config::default()
            },
            InMemoryMetaStore::default(),
            blob,
            1,
        );

        let b1 = mk_block(1, [0; 32], vec![mk_log(1, 7, 1, 1, 0, 0)]);
        let b2 = mk_block(2, b1.block_hash, vec![mk_log(2, 7, 2, 2, 0, 0)]);
        let b3 = mk_block(3, b2.block_hash, vec![mk_log(3, 7, 3, 3, 0, 0)]);
        svc.ingest_finalized_block(b1).await.expect("ingest b1");
        svc.ingest_finalized_block(b2).await.expect("ingest b2");
        svc.ingest_finalized_block(b3).await.expect("ingest b3");

        let got = svc
            .query_finalized(
                LogFilter {
                    from_block: Some(2),
                    to_block: Some(2),
                    block_hash: None,
                    address: None,
                    topic0: Some(Clause::One([7; 32])),
                    topic1: None,
                    topic2: None,
                    topic3: None,
                },
                QueryOptions::default(),
            )
            .await
            .expect("query");
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].block_num, 2);

        let topic0_log_prefix = b"chunks/topic0_log/";
        let log_pack_prefix = b"log_packs/";
        assert_eq!(
            svc.ingest
                .blob_store
                .count_gets_with_prefix(topic0_log_prefix),
            1
        );
        assert_eq!(
            svc.ingest
                .blob_store
                .count_gets_with_prefix(log_pack_prefix),
            1
        );
    });
}

#[test]
fn ingest_and_query_across_24_bit_log_shard_boundary() {
    block_on(async {
        let meta = InMemoryMetaStore::default();
        let blob = InMemoryBlobStore::default();
        let svc = FinalizedIndexService::new(
            Config {
                target_entries_per_chunk: 1,
                planner_max_or_terms: 8,
                ..Config::default()
            },
            meta,
            blob,
            1,
        );

        let previous = BlockMeta {
            block_hash: [1; 32],
            parent_hash: [0; 32],
            first_log_id: 0,
            count: 0,
        };
        let _ = svc
            .ingest
            .meta_store
            .put(
                META_STATE_KEY,
                encode_meta_state(&MetaState {
                    indexed_finalized_head: 1,
                    next_log_id: u64::from(MAX_LOCAL_ID),
                    writer_epoch: 1,
                }),
                PutCond::Any,
                FenceToken(1),
            )
            .await
            .expect("seed meta state");
        let _ = svc
            .ingest
            .meta_store
            .put(
                &finalized_log_index::domain::keys::block_meta_key(1),
                encode_block_meta(&previous),
                PutCond::Any,
                FenceToken(1),
            )
            .await
            .expect("seed previous block meta");

        let b2 = mk_block(2, previous.block_hash, vec![mk_log(9, 1, 1, 2, 0, 0)]);
        let b3 = mk_block(3, b2.block_hash, vec![mk_log(9, 2, 2, 3, 0, 0)]);
        svc.ingest_finalized_block(b2).await.expect("ingest b2");
        svc.ingest_finalized_block(b3).await.expect("ingest b3");

        let first_id = u64::from(MAX_LOCAL_ID);
        let second_id = compose_global_log_id(log_shard(first_id) + 1, 0);
        let first_sid = stream_id("addr", &[9; 20], log_shard(first_id));
        let second_sid = stream_id("addr", &[9; 20], log_shard(second_id));

        let first_manifest = svc
            .ingest
            .meta_store
            .get(&manifest_key(&first_sid))
            .await
            .expect("first manifest get")
            .expect("first manifest");
        let second_manifest = svc
            .ingest
            .meta_store
            .get(&manifest_key(&second_sid))
            .await
            .expect("second manifest get")
            .expect("second manifest");
        assert_eq!(
            decode_manifest(&first_manifest.value)
                .expect("decode first manifest")
                .chunk_refs
                .len(),
            1
        );
        assert_eq!(
            decode_manifest(&second_manifest.value)
                .expect("decode second manifest")
                .chunk_refs
                .len(),
            1
        );

        let got = svc
            .query_finalized(
                LogFilter {
                    from_block: Some(2),
                    to_block: Some(3),
                    block_hash: None,
                    address: Some(Clause::One([9; 20])),
                    topic0: None,
                    topic1: None,
                    topic2: None,
                    topic3: None,
                },
                QueryOptions::default(),
            )
            .await
            .expect("query");
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].block_num, 2);
        assert_eq!(got[1].block_num, 3);
    });
}

#[test]
fn invalid_parent_puts_service_in_degraded_mode() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config::default(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        let b1 = mk_block(1, [0; 32], vec![mk_log(1, 1, 1, 1, 0, 0)]);
        svc.ingest_finalized_block(b1).await.expect("ingest b1");

        let bad_b2 = mk_block(2, [9; 32], vec![mk_log(1, 1, 1, 2, 0, 0)]);
        let err = svc
            .ingest_finalized_block(bad_b2)
            .await
            .expect_err("invalid parent");
        assert!(matches!(err, Error::InvalidParent));

        let health = svc.health().await;
        assert!(health.degraded);
        assert!(health.message.contains("degraded"));

        let qerr = svc
            .query_finalized(
                LogFilter {
                    from_block: Some(1),
                    to_block: Some(1),
                    block_hash: None,
                    address: None,
                    topic0: None,
                    topic1: None,
                    topic2: None,
                    topic3: None,
                },
                QueryOptions::default(),
            )
            .await
            .expect_err("degraded query");
        assert!(matches!(qerr, Error::Degraded(_)));
    });
}

#[test]
fn gc_guardrail_fail_closed_sets_degraded() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config {
                max_orphan_chunk_bytes: 1,
                gc_guardrail_action: GuardrailAction::FailClosed,
                ..Config::default()
            },
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        let mut bm = RoaringBitmap::new();
        bm.insert(1);
        let orphan = ChunkBlob {
            min_local: 1,
            max_local: 1,
            count: 1,
            crc32: 0,
            bitmap: bm,
        };
        svc.ingest
            .blob_store
            .put_blob(
                b"chunks/orphan/0000000000000001",
                encode_chunk(&orphan).expect("encode chunk"),
            )
            .await
            .expect("put orphan");

        let stats = svc.run_gc_once().await.expect("gc");
        assert!(stats.exceeded_guardrail);
        assert!(svc.health().await.degraded);
    });
}

#[test]
fn gc_guardrail_throttle_blocks_ingest() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config {
                max_orphan_chunk_bytes: 1,
                gc_guardrail_action: GuardrailAction::Throttle,
                ..Config::default()
            },
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        let mut bm = RoaringBitmap::new();
        bm.insert(2);
        let orphan = ChunkBlob {
            min_local: 2,
            max_local: 2,
            count: 1,
            crc32: 0,
            bitmap: bm,
        };
        svc.ingest
            .blob_store
            .put_blob(
                b"chunks/orphan/0000000000000002",
                encode_chunk(&orphan).expect("encode chunk"),
            )
            .await
            .expect("put orphan");

        let _ = svc.run_gc_once().await.expect("gc");
        let err = svc
            .ingest_finalized_block(mk_block(1, [0; 32], vec![mk_log(1, 1, 1, 1, 0, 0)]))
            .await
            .expect_err("throttled ingest");
        assert!(matches!(err, Error::Throttled(_)));
        assert!(!svc.health().await.degraded);
    });
}

#[test]
fn prune_block_hash_index_hook() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config::default(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        let b1 = mk_block(1, [0; 32], vec![mk_log(1, 1, 1, 1, 0, 0)]);
        let b2 = mk_block(2, b1.block_hash, vec![mk_log(2, 2, 2, 2, 0, 0)]);
        svc.ingest_finalized_block(b1.clone())
            .await
            .expect("ingest b1");
        svc.ingest_finalized_block(b2.clone())
            .await
            .expect("ingest b2");

        let removed = svc.prune_block_hash_index_below(2).await.expect("prune");
        assert!(removed >= 1);
        assert!(
            svc.ingest
                .meta_store
                .get(&block_hash_to_num_key(&b1.block_hash))
                .await
                .expect("get b1 map")
                .is_none()
        );
        assert!(
            svc.ingest
                .meta_store
                .get(&block_hash_to_num_key(&b2.block_hash))
                .await
                .expect("get b2 map")
                .is_some()
        );
    });
}

#[test]
fn backend_failures_throttle_then_degrade() {
    block_on(async {
        let fail_counter = Arc::new(AtomicUsize::new(3));
        let svc = FinalizedIndexService::new(
            Config {
                backend_error_throttle_after: 2,
                backend_error_degraded_after: 3,
                ..Config::default()
            },
            FlakyMetaStore {
                inner: InMemoryMetaStore::default(),
                fail_get_remaining: fail_counter,
            },
            InMemoryBlobStore::default(),
            1,
        );
        let b1 = mk_block(1, [0; 32], vec![mk_log(1, 1, 1, 1, 0, 0)]);

        let e1 = svc
            .ingest_finalized_block(b1.clone())
            .await
            .expect_err("err1");
        assert!(matches!(e1, Error::Backend(_)));
        assert!(!svc.health().await.degraded);

        let e2 = svc
            .ingest_finalized_block(b1.clone())
            .await
            .expect_err("err2");
        assert!(matches!(e2, Error::Backend(_)));
        let h2 = svc.health().await;
        assert!(!h2.degraded);
        assert!(h2.message.contains("throttled"));

        let e3 = svc.ingest_finalized_block(b1).await.expect_err("err3");
        assert!(matches!(e3, Error::Throttled(_)));

        let qerr = svc
            .query_finalized(
                LogFilter {
                    from_block: Some(1),
                    to_block: Some(1),
                    block_hash: None,
                    address: None,
                    topic0: None,
                    topic1: None,
                    topic2: None,
                    topic3: None,
                },
                QueryOptions::default(),
            )
            .await
            .expect_err("backend query error");
        assert!(matches!(qerr, Error::Backend(_)));
        assert!(svc.health().await.degraded);
    });
}

#[test]
fn backend_success_clears_throttle() {
    block_on(async {
        let fail_counter = Arc::new(AtomicUsize::new(2));
        let svc = FinalizedIndexService::new(
            Config {
                backend_error_throttle_after: 2,
                backend_error_degraded_after: 10,
                ..Config::default()
            },
            FlakyMetaStore {
                inner: InMemoryMetaStore::default(),
                fail_get_remaining: fail_counter,
            },
            InMemoryBlobStore::default(),
            1,
        );
        let b1 = mk_block(1, [0; 32], vec![mk_log(1, 1, 1, 1, 0, 0)]);

        let _ = svc
            .ingest_finalized_block(b1.clone())
            .await
            .expect_err("err1");
        let _ = svc
            .ingest_finalized_block(b1.clone())
            .await
            .expect_err("err2");
        assert!(svc.health().await.message.contains("throttled"));

        let _ = svc.indexed_finalized_head().await.expect("head probe");
        svc.ingest_finalized_block(b1)
            .await
            .expect("eventual success");
        let health = svc.health().await;
        assert!(!health.degraded);
        assert_eq!(health.message, "ok");
    });
}

#[test]
fn strict_mode_keeps_cas_for_state_and_manifest_writes() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config {
                ingest_mode: IngestMode::StrictCas,
                target_entries_per_chunk: 1,
                ..Config::default()
            },
            RecordingMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        let b1 = mk_block(1, [0; 32], vec![mk_log(9, 5, 4, 1, 0, 0)]);
        svc.ingest_finalized_block(b1).await.expect("ingest b1");

        assert!(
            svc.ingest
                .meta_store
                .count_puts_with_prefix(META_STATE_KEY, |c| {
                    matches!(c, PutCond::IfAbsent | PutCond::IfVersion(_))
                })
                >= 1
        );
        assert_eq!(
            svc.ingest
                .meta_store
                .count_puts_with_prefix(META_STATE_KEY, |c| matches!(c, PutCond::Any)),
            0
        );

        assert!(
            svc.ingest
                .meta_store
                .count_puts_with_prefix(b"manifests/", |c| {
                    matches!(c, PutCond::IfAbsent | PutCond::IfVersion(_))
                })
                >= 1
        );
        assert_eq!(
            svc.ingest
                .meta_store
                .count_puts_with_prefix(b"manifests/", |c| matches!(c, PutCond::Any)),
            0
        );
    });
}

#[test]
fn fast_mode_uses_unconditional_writes_for_state_and_manifest() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config {
                ingest_mode: IngestMode::SingleWriterFast,
                target_entries_per_chunk: 1,
                ..Config::default()
            },
            RecordingMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        let b1 = mk_block(1, [0; 32], vec![mk_log(9, 5, 4, 1, 0, 0)]);
        svc.ingest_finalized_block(b1).await.expect("ingest b1");

        assert!(
            svc.ingest
                .meta_store
                .count_puts_with_prefix(META_STATE_KEY, |c| matches!(c, PutCond::Any))
                >= 1
        );
        assert_eq!(
            svc.ingest
                .meta_store
                .count_puts_with_prefix(META_STATE_KEY, |c| {
                    matches!(c, PutCond::IfAbsent | PutCond::IfVersion(_))
                }),
            0
        );

        assert!(
            svc.ingest
                .meta_store
                .count_puts_with_prefix(b"manifests/", |c| matches!(c, PutCond::Any))
                >= 1
        );
        assert_eq!(
            svc.ingest
                .meta_store
                .count_puts_with_prefix(b"manifests/", |c| {
                    matches!(c, PutCond::IfAbsent | PutCond::IfVersion(_))
                }),
            0
        );
    });
}
