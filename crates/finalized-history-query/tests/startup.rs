#[allow(dead_code, unused_imports)]
mod helpers;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use finalized_history_query::Error;
use finalized_history_query::api::FinalizedHistoryService;
use finalized_history_query::config::Config;
use finalized_history_query::domain::keys::{PUBLICATION_STATE_SUFFIX, PUBLICATION_STATE_TABLE};
use finalized_history_query::logs::keys::BLOCK_RECORD_TABLE;
use finalized_history_query::logs::table_specs::BlockRecordSpec;
use finalized_history_query::logs::types::BlockRecord;
use finalized_history_query::startup::startup_plan;
use finalized_history_query::store::blob::InMemoryBlobStore;
use finalized_history_query::store::meta::InMemoryMetaStore;
use finalized_history_query::store::publication::{MetaPublicationStore, PublicationStore};
use finalized_history_query::store::traits::{MetaStore, PutCond};
use futures::executor::block_on;

use helpers::*;

#[test]
fn service_startup_bootstraps_publication_ownership() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            5,
        );

        let plan = svc.startup().await.expect("startup");
        assert_eq!(plan.head_state.indexed_finalized_head, 0);
        assert_eq!(svc.indexed_finalized_head().await.expect("head"), 0);
    });
}

#[test]
fn service_startup_uses_configured_lease_blocks() {
    block_on(async {
        let observed_upstream_finalized_block = 41;
        let config = Config {
            observe_upstream_finalized_block: Arc::new(move || {
                Some(observed_upstream_finalized_block)
            }),
            publication_lease_blocks: 7,
            ..Config::default()
        };
        let svc = FinalizedHistoryService::new_reader_writer(
            config,
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            5,
        );

        svc.startup().await.expect("startup");
        let publication_state = MetaPublicationStore::new(Arc::clone(&svc.ingest.meta_store))
            .load()
            .await
            .expect("load publication state")
            .expect("publication state");

        assert_eq!(
            publication_state.lease_valid_through_block,
            observed_upstream_finalized_block + 6
        );
    });
}

#[test]
fn lease_writer_startup_fails_closed_without_observed_finalized_block() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            Config::default(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            5,
        );

        let err = svc
            .startup()
            .await
            .expect_err("missing observation should fail closed");

        assert!(matches!(err, Error::LeaseObservationUnavailable));
    });
}

#[test]
fn startup_rechecks_observation_for_a_cached_writer() {
    block_on(async {
        let observation_available = Arc::new(AtomicBool::new(true));
        let config = Config {
            observe_upstream_finalized_block: {
                let observation_available = observation_available.clone();
                Arc::new(move || observation_available.load(Ordering::Relaxed).then_some(100))
            },
            ..Config::default()
        };
        let svc = FinalizedHistoryService::new_reader_writer(
            config,
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            5,
        );

        svc.startup().await.expect("first startup");
        observation_available.store(false, Ordering::Relaxed);

        let err = svc
            .startup()
            .await
            .expect_err("cached writer startup should fail closed without observation");

        assert!(matches!(err, Error::LeaseObservationUnavailable));
    });
}

#[test]
fn reader_only_startup_is_observational_and_ingest_is_rejected() {
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

        let svc = FinalizedHistoryService::new_reader_only(Config::default(), meta.clone(), blob);
        let plan = svc.startup().await.expect("reader-only startup");
        let state = publication_store
            .load()
            .await
            .expect("load")
            .expect("publication state");
        let err = svc
            .ingest_finalized_block(mk_block(4, [3; 32], vec![mk_log(1, 10, 20, 4, 0, 0)]))
            .await
            .expect_err("reader-only ingest should fail");

        assert_eq!(plan.head_state.indexed_finalized_head, 3);
        assert_eq!(state.owner_id, 11);
        assert_eq!(state.session_id, [11u8; 16]);
        assert!(matches!(err, Error::ReadOnlyMode(_)));
    });
}

#[test]
fn startup_plan_should_not_take_publication_ownership() {
    block_on(async {
        let meta = InMemoryMetaStore::default();
        let blob = InMemoryBlobStore::default();
        let publication_store = MetaPublicationStore::new(std::sync::Arc::new(meta.clone()));
        assert!(matches!(
            publication_store
                .create_if_absent(&seeded_publication_state(7, [7u8; 16], 0))
                .await
                .expect("seed publication state"),
            finalized_history_query::store::publication::CasOutcome::Applied(_)
        ));

        let tables = finalized_history_query::tables::Tables::without_cache(
            std::sync::Arc::new(meta.clone()),
            std::sync::Arc::new(blob.clone()),
        );
        let _ = startup_plan(&tables, &publication_store, 0)
            .await
            .expect("startup plan should succeed");

        let publication_state = meta
            .get(PUBLICATION_STATE_TABLE, PUBLICATION_STATE_SUFFIX)
            .await
            .expect("read publication state")
            .expect("publication state present");
        let publication_state = finalized_history_query::domain::types::PublicationState::decode(
            &publication_state.value,
        )
        .expect("decode publication state");
        assert_eq!(publication_state.owner_id, 7);
        assert_eq!(publication_state.session_id, [7u8; 16]);
    });
}

#[test]
fn startup_retry_reuses_the_same_session_after_ownership_is_acquired() {
    block_on(async {
        let inner = InMemoryMetaStore::default();
        inner
            .put(
                BLOCK_RECORD_TABLE,
                &BlockRecordSpec::key(1),
                BlockRecord {
                    block_hash: [1; 32],
                    parent_hash: [0; 32],
                    first_log_id: 0,
                    count: 0,
                }
                .encode(),
                PutCond::Any,
            )
            .await
            .expect("seed unpublished block meta");
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            inner.clone(),
            InMemoryBlobStore::default(),
            7,
        );

        svc.startup()
            .await
            .expect("startup should ignore unpublished suffix artifacts");
        assert!(
            svc.ingest
                .meta_store
                .get(BLOCK_RECORD_TABLE, &BlockRecordSpec::key(1))
                .await
                .expect("read block meta after startup")
                .is_some()
        );
    });
}

#[test]
fn service_can_publish_a_contiguous_batch() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        svc.startup().await.expect("startup");

        let blocks = vec![
            mk_block(1, [0; 32], vec![mk_log(1, 10, 20, 1, 0, 0)]),
            mk_block(2, [1; 32], vec![mk_log(1, 10, 21, 2, 0, 0)]),
        ];
        let (outcome, head_state) = (
            svc.ingest_finalized_blocks(blocks)
                .await
                .expect("batched ingest"),
            MetaPublicationStore::new(Arc::clone(&svc.ingest.meta_store))
                .load_finalized_head_state()
                .await
                .expect("head state"),
        );

        assert_eq!(outcome.indexed_finalized_head, 2);
        assert_eq!(head_state.indexed_finalized_head, 2);
    });
}
