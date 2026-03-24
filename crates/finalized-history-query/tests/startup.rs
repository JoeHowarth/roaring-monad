#[allow(dead_code, unused_imports)]
mod helpers;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use finalized_history_query::Error;
use finalized_history_query::api::FinalizedHistoryService;
use finalized_history_query::config::Config;
use finalized_history_query::core::state::{
    BLOCK_RECORD_TABLE, BlockRecord, BlockRecordSpec, PrimaryWindowRecord,
};
use finalized_history_query::family::Families;
use finalized_history_query::kernel::codec::StorageCodec;
use finalized_history_query::startup::startup_plan;
use finalized_history_query::store::blob::InMemoryBlobStore;
use finalized_history_query::store::meta::InMemoryMetaStore;
use finalized_history_query::store::publication::{MetaPublicationStore, PublicationStore};
use finalized_history_query::store::publication::{
    PUBLICATION_STATE_SUFFIX, PUBLICATION_STATE_TABLE,
};
use finalized_history_query::store::traits::{MetaStore, PutCond};
use futures::executor::block_on;

use helpers::*;

fn shared_block_record(
    block_hash: [u8; 32],
    parent_hash: [u8; 32],
    logs: Option<(u64, u32)>,
    traces: Option<(u64, u32)>,
) -> BlockRecord {
    BlockRecord {
        block_hash,
        parent_hash,
        logs: logs.map(|(first_primary_id, count)| PrimaryWindowRecord {
            first_primary_id,
            count,
        }),
        traces: traces.map(|(first_primary_id, count)| PrimaryWindowRecord {
            first_primary_id,
            count,
        }),
    }
}

#[test]
fn first_ingest_bootstraps_publication_ownership() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            5,
        );

        let outcome = svc
            .ingest_finalized_block(mk_block(1, [0; 32], vec![mk_log(1, 10, 20, 1, 0, 0)]))
            .await
            .expect("ingest");
        assert_eq!(outcome.indexed_finalized_head, 1);
        assert_eq!(svc.indexed_finalized_head().await.expect("head"), 1);
    });
}

#[test]
fn first_ingest_uses_configured_lease_blocks() {
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

        svc.ingest_finalized_block(mk_block(1, [0; 32], vec![]))
            .await
            .expect("ingest");
        let publication_state = MetaPublicationStore::new(svc.meta_store().clone())
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
fn lease_writer_ingest_fails_closed_without_observed_finalized_block() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            Config::default(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            5,
        );

        let err = svc
            .ingest_finalized_block(mk_block(1, [0; 32], vec![]))
            .await
            .expect_err("missing observation should fail closed");

        assert!(matches!(err, Error::LeaseObservationUnavailable));
    });
}

#[test]
fn ingest_rechecks_observation_for_a_cached_writer() {
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

        svc.ingest_finalized_block(mk_block(1, [0; 32], vec![]))
            .await
            .expect("first ingest");
        observation_available.store(false, Ordering::Relaxed);

        let err = svc
            .ingest_finalized_block(mk_block(2, [1; 32], vec![]))
            .await
            .expect_err("cached writer ingest should fail closed without observation");

        assert!(matches!(err, Error::LeaseObservationUnavailable));
    });
}

#[test]
fn reader_only_plan_is_observational_and_ingest_is_rejected() {
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
            shared_block_record([3; 32], [2; 32], Some((9, 1)), Some((0, 0))).encode(),
            PutCond::Any,
        )
        .await
        .expect("seed block meta");

        let svc = FinalizedHistoryService::new_reader_only(Config::default(), meta.clone(), blob);
        let runtime = finalized_history_query::runtime::Runtime::new(
            meta.clone(),
            svc.blob_store().clone(),
            finalized_history_query::tables::BytesCacheConfig::default(),
        );
        let plan = startup_plan(&runtime, &publication_store, &Families::default(), 0)
            .await
            .expect("reader-only startup plan");
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

        let runtime = finalized_history_query::runtime::Runtime::new(
            meta.clone(),
            blob.clone(),
            finalized_history_query::tables::BytesCacheConfig::default(),
        );
        let _ = startup_plan(&runtime, &publication_store, &Families::default(), 0)
            .await
            .expect("startup plan should succeed");

        let publication_state = meta
            .get(PUBLICATION_STATE_TABLE, PUBLICATION_STATE_SUFFIX)
            .await
            .expect("read publication state")
            .expect("publication state present");
        let publication_state =
            finalized_history_query::store::publication::PublicationState::decode(
                &publication_state.value,
            )
            .expect("decode publication state");
        assert_eq!(publication_state.owner_id, 7);
        assert_eq!(publication_state.session_id, [7u8; 16]);
    });
}

#[test]
fn first_ingest_ignores_unpublished_suffix_artifacts() {
    block_on(async {
        let inner = InMemoryMetaStore::default();
        inner
            .put(
                BLOCK_RECORD_TABLE,
                &BlockRecordSpec::key(1),
                shared_block_record([1; 32], [0; 32], Some((0, 0)), None).encode(),
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

        svc.ingest_finalized_block(mk_block(1, [0; 32], vec![]))
            .await
            .expect("ingest should ignore unpublished suffix artifacts");
        assert!(
            svc.meta_store()
                .get(BLOCK_RECORD_TABLE, &BlockRecordSpec::key(1))
                .await
                .expect("read block meta after ingest")
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

        let blocks = vec![
            mk_block(1, [0; 32], vec![mk_log(1, 10, 20, 1, 0, 0)]),
            mk_block(2, [1; 32], vec![mk_log(1, 10, 21, 2, 0, 0)]),
        ];
        let (outcome, head_state) = (
            svc.ingest_finalized_blocks(blocks)
                .await
                .expect("batched ingest"),
            MetaPublicationStore::new(svc.meta_store().clone())
                .load_finalized_head_state()
                .await
                .expect("head state"),
        );

        assert_eq!(outcome.indexed_finalized_head, 2);
        assert_eq!(head_state.indexed_finalized_head, 2);
    });
}

#[test]
fn startup_recovers_trace_state_from_published_head_only() {
    block_on(async {
        let meta = InMemoryMetaStore::default();
        let blob = InMemoryBlobStore::default();
        let publication_store = MetaPublicationStore::new(std::sync::Arc::new(meta.clone()));
        assert!(matches!(
            publication_store
                .create_if_absent(&seeded_publication_state(3, [3u8; 16], 2))
                .await
                .expect("seed publication state"),
            finalized_history_query::store::publication::CasOutcome::Applied(_)
        ));

        meta.put(
            BLOCK_RECORD_TABLE,
            &BlockRecordSpec::key(2),
            shared_block_record([2; 32], [1; 32], Some((12, 0)), Some((40, 3))).encode(),
            PutCond::Any,
        )
        .await
        .expect("seed published block record");
        meta.put(
            BLOCK_RECORD_TABLE,
            &BlockRecordSpec::key(3),
            shared_block_record([3; 32], [2; 32], None, Some((999, 7))).encode(),
            PutCond::Any,
        )
        .await
        .expect("seed unpublished block record");

        let runtime = finalized_history_query::runtime::Runtime::new(
            meta,
            blob,
            finalized_history_query::tables::BytesCacheConfig::default(),
        );
        let plan = startup_plan(&runtime, &publication_store, &Families::default(), 0)
            .await
            .expect("startup plan");

        assert_eq!(plan.head_state.indexed_finalized_head, 2);
        assert_eq!(plan.trace_state.next_trace_id.get(), 43);
    });
}

#[test]
fn startup_rejects_missing_trace_records_for_nonzero_published_head() {
    block_on(async {
        let meta = InMemoryMetaStore::default();
        let blob = InMemoryBlobStore::default();
        let publication_store = MetaPublicationStore::new(std::sync::Arc::new(meta.clone()));
        assert!(matches!(
            publication_store
                .create_if_absent(&seeded_publication_state(7, [7u8; 16], 3))
                .await
                .expect("seed publication state"),
            finalized_history_query::store::publication::CasOutcome::Applied(_)
        ));
        meta.put(
            BLOCK_RECORD_TABLE,
            &BlockRecordSpec::key(3),
            shared_block_record([3; 32], [2; 32], Some((8, 2)), None).encode(),
            PutCond::Any,
        )
        .await
        .expect("seed published block record");

        let runtime = finalized_history_query::runtime::Runtime::new(
            meta,
            blob,
            finalized_history_query::tables::BytesCacheConfig::default(),
        );
        let err = startup_plan(&runtime, &publication_store, &Families::default(), 0)
            .await
            .expect_err("startup should fail closed when published trace head metadata is missing");

        assert!(matches!(err, finalized_history_query::Error::NotFound));
    });
}
