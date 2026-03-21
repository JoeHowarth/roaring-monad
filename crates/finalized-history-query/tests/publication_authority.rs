#[allow(dead_code, unused_imports)]
mod helpers;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use finalized_history_query::api::FinalizedHistoryService;
use finalized_history_query::config::Config;
use finalized_history_query::domain::keys::{
    BITMAP_BY_BLOCK_TABLE, BLOCK_RECORD_TABLE, LOG_DIR_BY_BLOCK_TABLE, PUBLICATION_STATE_SUFFIX,
    PUBLICATION_STATE_TABLE,
};
use finalized_history_query::domain::table_specs::{
    self, BitmapByBlockSpec, BlobTableSpec, BlockLogBlobSpec, BlockRecordSpec, LogDirByBlockSpec,
};
use finalized_history_query::domain::types::BlockRecord;
use finalized_history_query::ingest::authority::lease::LeaseAuthority;
use finalized_history_query::ingest::engine::IngestEngine;
use finalized_history_query::store::blob::InMemoryBlobStore;
use finalized_history_query::store::meta::InMemoryMetaStore;
use finalized_history_query::store::publication::{MetaPublicationStore, PublicationStore};
use finalized_history_query::store::traits::{BlobStore, MetaStore, PutCond};
use finalized_history_query::{Error, WriteAuthority};
use futures::executor::block_on;

use helpers::*;

#[test]
fn ingest_publishes_publication_state_and_immutable_frontier_artifacts() {
    block_on(async {
        let meta = InMemoryMetaStore::default();
        let blob = InMemoryBlobStore::default();
        let config = lease_writer_config();
        let expected_lease_valid_through_block = static_observed_finalized_block()
            .expect("static observed finalized block")
            .saturating_add(config.publication_lease_blocks - 1);
        let svc = FinalizedHistoryService::new_reader_writer(config, meta, blob, 1);
        let block = mk_block(
            1,
            [0; 32],
            vec![mk_log(1, 10, 20, 1, 0, 0), mk_log(1, 10, 21, 1, 0, 1)],
        );

        svc.ingest_finalized_block(block.clone())
            .await
            .expect("ingest");

        assert_eq!(svc.indexed_finalized_head().await.expect("head"), 1);
        let publication_state = svc
            .ingest
            .meta_store
            .get(PUBLICATION_STATE_TABLE, PUBLICATION_STATE_SUFFIX)
            .await
            .expect("publication state get")
            .expect("publication state");
        let publication_state = finalized_history_query::domain::types::PublicationState::decode(
            &publication_state.value,
        )
        .expect("decode publication state");
        assert_eq!(publication_state.owner_id, 1);
        assert_eq!(publication_state.indexed_finalized_head, 1);
        assert_eq!(
            publication_state.lease_valid_through_block,
            expected_lease_valid_through_block
        );
        assert!(
            svc.ingest
                .meta_store
                .scan_get(
                    LOG_DIR_BY_BLOCK_TABLE,
                    &LogDirByBlockSpec::partition(0),
                    &LogDirByBlockSpec::clustering(1),
                )
                .await
                .expect("directory fragment get")
                .is_some()
        );

        let sid = table_specs::stream_id(
            "addr",
            &[1; 20],
            finalized_history_query::core::ids::LogShard::new(0).unwrap(),
        );
        let page_start = table_specs::stream_page_start_local(0);
        assert!(
            svc.ingest
                .meta_store
                .scan_get(
                    BITMAP_BY_BLOCK_TABLE,
                    &BitmapByBlockSpec::partition(&sid, page_start),
                    &BitmapByBlockSpec::clustering(1),
                )
                .await
                .expect("stream fragment get")
                .is_some()
        );
    });
}

#[test]
fn acquire_publication_bootstraps_and_takeover_switches_session() {
    block_on(async {
        let meta = InMemoryMetaStore::default();

        let first_head = acquire_lease(
            MetaPublicationStore::new(Arc::new(meta.clone())),
            7,
            100,
            50,
        )
        .await
        .expect("bootstrap");
        let first_state = MetaPublicationStore::new(Arc::new(meta.clone()))
            .load()
            .await
            .expect("load publication state")
            .expect("publication state");
        assert_eq!(first_state.owner_id, 7);
        assert_eq!(first_head, 0);

        let second_head = acquire_lease(
            MetaPublicationStore::new(Arc::new(meta.clone())),
            9,
            151,
            50,
        )
        .await
        .expect("takeover after expiry");
        let second_state = MetaPublicationStore::new(Arc::new(meta.clone()))
            .load()
            .await
            .expect("load publication state")
            .expect("publication state");
        assert_eq!(second_state.owner_id, 9);
        assert_eq!(second_head, 0);
        assert_ne!(second_state.session_id, first_state.session_id);
    });
}

#[test]
fn standby_writer_does_not_take_over_while_primary_lease_is_fresh() {
    block_on(async {
        let meta = InMemoryMetaStore::default();

        let _first = acquire_lease(
            MetaPublicationStore::new(Arc::new(meta.clone())),
            7,
            100,
            50,
        )
        .await
        .expect("bootstrap");
        let first_state = MetaPublicationStore::new(Arc::new(meta.clone()))
            .load()
            .await
            .expect("load publication state")
            .expect("publication state");
        let err = acquire_lease(
            MetaPublicationStore::new(Arc::new(meta.clone())),
            9,
            120,
            50,
        )
        .await
        .expect_err("fresh lease should reject standby takeover");
        assert!(matches!(err, Error::LeaseStillFresh));

        let publication_state = MetaPublicationStore::new(Arc::new(meta.clone()))
            .load()
            .await
            .expect("load publication state");
        let publication_state = publication_state.expect("publication state");
        assert_eq!(publication_state.owner_id, first_state.owner_id);
        assert_eq!(publication_state.session_id, first_state.session_id);
    });
}

#[test]
fn readers_use_only_publication_state() {
    block_on(async {
        let meta = InMemoryMetaStore::default();
        let blob = InMemoryBlobStore::default();
        let publication_store = MetaPublicationStore::new(Arc::new(meta.clone()));
        assert!(matches!(
            publication_store
                .create_if_absent(&seeded_publication_state(11, [11u8; 16], 3))
                .await
                .expect("create publication state"),
            finalized_history_query::store::publication::CasOutcome::Applied(_)
        ));
        meta.put(
            BLOCK_RECORD_TABLE,
            &BlockRecordSpec::key(3),
            BlockRecord {
                block_hash: [3; 32],
                parent_hash: [2; 32],
                first_log_id: 9,
                count: 1,
            }
            .encode(),
            PutCond::Any,
        )
        .await
        .expect("seed block meta");

        let svc = FinalizedHistoryService::new_reader_only(Config::default(), meta, blob);
        assert_eq!(svc.indexed_finalized_head().await.expect("head"), 3);
        let state = publication_store
            .load_finalized_head_state()
            .await
            .expect("load finalized head state");
        assert_eq!(state.indexed_finalized_head, 3);
    });
}

#[test]
fn publication_state_key_is_encoded_at_the_canonical_location() {
    block_on(async {
        let meta = InMemoryMetaStore::default();
        meta.put(
            PUBLICATION_STATE_TABLE,
            PUBLICATION_STATE_SUFFIX,
            seeded_publication_state(1, [1u8; 16], 3).encode(),
            PutCond::Any,
        )
        .await
        .expect("write publication state");

        let record = meta
            .get(PUBLICATION_STATE_TABLE, PUBLICATION_STATE_SUFFIX)
            .await
            .expect("get publication state")
            .expect("publication state");
        let decoded =
            finalized_history_query::domain::types::PublicationState::decode(&record.value)
                .expect("decode");
        assert_eq!(decoded.indexed_finalized_head, 3);
    });
}

#[test]
fn ingest_returns_lease_lost_when_lease_expires_mid_batch() {
    block_on(async {
        CONTROLLED_OBSERVED_FINALIZED_BLOCK.store(1_000, Ordering::Relaxed);

        let config = Config {
            observe_upstream_finalized_block: Arc::new(controlled_observed_finalized_block),
            publication_lease_blocks: 50,
            publication_lease_renew_threshold_blocks: 0,
            ..Config::default()
        };
        let meta = ExpireBeforePublishMetaStore {
            inner: Arc::new(InMemoryMetaStore::default()),
            advanced: Arc::new(AtomicBool::new(false)),
        };
        let blob = InMemoryBlobStore::default();
        let authority =
            LeaseAuthority::new(MetaPublicationStore::new(Arc::new(meta.clone())), 1, 50, 0);
        let engine = IngestEngine::new(config, authority, meta, blob);

        engine
            .authority
            .begin_write(controlled_observed_finalized_block())
            .await
            .expect("bootstrap");

        let err = engine
            .ingest_finalized_block(&mk_block(1, [0; 32], vec![mk_log(1, 10, 20, 1, 0, 0)]))
            .await
            .expect_err("mid-batch lease expiry should fail with LeaseLost");
        assert!(matches!(err, Error::LeaseLost));
    });
}

#[test]
fn stale_writer_cannot_start_new_ingest_after_takeover() {
    block_on(async {
        let meta = InMemoryMetaStore::default();
        let blob = InMemoryBlobStore::default();
        let authority =
            LeaseAuthority::new(MetaPublicationStore::new(Arc::new(meta.clone())), 1, 50, 0);
        let engine = IngestEngine::new(lease_writer_config(), authority, meta.clone(), blob);

        engine
            .authority
            .begin_write(Some(100))
            .await
            .expect("writer 1 acquires publication");
        let takeover =
            LeaseAuthority::new(MetaPublicationStore::new(Arc::new(meta.clone())), 2, 50, 0);
        let _takeover_lease = takeover
            .begin_write(Some(151))
            .await
            .expect("writer 2 takes over publication");

        let err = engine
            .ingest_finalized_block(&mk_block(1, [0; 32], vec![mk_log(1, 10, 20, 1, 0, 0)]))
            .await
            .expect_err("stale writer ingest should fail");
        assert!(matches!(err, Error::LeaseLost));

        assert!(
            engine
                .meta_store
                .get(BLOCK_RECORD_TABLE, &BlockRecordSpec::key(1))
                .await
                .expect("read block meta")
                .is_none(),
            "stale writer should not write block metadata after takeover"
        );
        assert!(
            engine
                .blob_store
                .get_blob(BlockLogBlobSpec::TABLE, &BlockLogBlobSpec::key(1))
                .await
                .expect("read block blob")
                .is_none(),
            "stale writer should not write block blobs after takeover"
        );
    });
}

#[test]
fn service_clears_cached_writer_after_publication_conflict() {
    block_on(async {
        let meta = PublishConflictOnceMetaStore {
            inner: Arc::new(InMemoryMetaStore::default()),
            conflicted: Arc::new(AtomicBool::new(false)),
        };
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            meta.clone(),
            InMemoryBlobStore::default(),
            1,
        );

        let err = svc
            .ingest_finalized_block(mk_block(1, [0; 32], vec![mk_log(1, 10, 20, 1, 0, 0)]))
            .await
            .expect_err("first publish should surface publication conflict");
        assert!(matches!(err, Error::PublicationConflict));
        assert_eq!(
            svc.indexed_finalized_head()
                .await
                .expect("head after conflict"),
            1
        );

        svc.ingest_finalized_block(mk_block(2, [1; 32], vec![mk_log(1, 10, 21, 2, 0, 0)]))
            .await
            .expect("service should reacquire after publication conflict");
        assert_eq!(
            svc.indexed_finalized_head()
                .await
                .expect("head after retry"),
            2
        );
    });
}
