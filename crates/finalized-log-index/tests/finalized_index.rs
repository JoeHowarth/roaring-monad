use std::fs;

use finalized_log_index::api::{FinalizedIndexService, FinalizedLogIndex};
use finalized_log_index::codec::log::{decode_topic0_mode, encode_topic0_mode};
use finalized_log_index::codec::manifest::{decode_manifest, encode_manifest};
use finalized_log_index::config::{BroadQueryPolicy, Config};
use finalized_log_index::domain::filter::{Clause, LogFilter, QueryOptions};
use finalized_log_index::domain::keys::{manifest_key, stream_id, topic0_mode_key};
use finalized_log_index::domain::types::{Block, Log, Topic0Mode};
use finalized_log_index::error::Error;
use finalized_log_index::store::blob::InMemoryBlobStore;
use finalized_log_index::store::fs::{FsBlobStore, FsMetaStore};
use finalized_log_index::store::meta::InMemoryMetaStore;
use finalized_log_index::store::traits::{FenceToken, MetaStore, PutCond};
use futures::executor::block_on;

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
fn topic0_mode_enables_for_cold_signature() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config::default(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        let sig = [0x55; 32];
        let mut parent = [0u8; 32];
        for b in 1..=1200u64 {
            let logs = if b == 1 {
                vec![mk_log(1, sig[0], 20, b, 0, 0)]
            } else {
                Vec::new()
            };
            let block = mk_block(b, parent, logs);
            parent = block.block_hash;
            svc.ingest_finalized_block(block).await.expect("ingest");
        }

        let mode_key = topic0_mode_key(&sig);
        let rec = svc
            .ingest
            .meta_store
            .get(&mode_key)
            .await
            .expect("mode get")
            .expect("mode exists");
        let mode = decode_topic0_mode(&rec.value).expect("decode mode");
        assert!(mode.log_enabled);
        assert!(mode.enabled_from_block >= 1000);
    });
}

#[test]
fn topic0_mode_disables_for_hot_signature() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config::default(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        let sig = [0x44; 32];
        let mode_key = topic0_mode_key(&sig);
        let _ = svc
            .ingest
            .meta_store
            .put(
                &mode_key,
                encode_topic0_mode(&Topic0Mode {
                    log_enabled: true,
                    enabled_from_block: 0,
                }),
                PutCond::Any,
                FenceToken(1),
            )
            .await
            .expect("seed mode");

        let b1 = mk_block(1, [0; 32], vec![mk_log(1, sig[0], 20, 1, 0, 0)]);
        svc.ingest_finalized_block(b1).await.expect("ingest");

        let rec = svc
            .ingest
            .meta_store
            .get(&mode_key)
            .await
            .expect("mode get")
            .expect("mode exists");
        let mode = decode_topic0_mode(&rec.value).expect("decode mode");
        assert!(!mode.log_enabled);
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
        let rec = svc
            .ingest
            .meta_store
            .get(&mkey)
            .await
            .expect("manifest get")
            .expect("manifest");
        let mut manifest = decode_manifest(&rec.value).expect("decode manifest");
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
