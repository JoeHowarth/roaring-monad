#[allow(dead_code, unused_imports)]
mod helpers;

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use finalized_history_query::api::FinalizedHistoryService;
use finalized_history_query::config::Config;
use finalized_history_query::core::state::{
    BLOCK_RECORD_TABLE, BlockRecord, BlockRecordSpec, PrimaryWindowRecord,
};
use finalized_history_query::family::Families;
use finalized_history_query::ingest::authority::LeaseAuthority;
use finalized_history_query::ingest::engine::IngestEngine;
use finalized_history_query::kernel::codec::StorageCodec;
use finalized_history_query::kernel::sharded_streams::page_start_local;
use finalized_history_query::kernel::table_specs::ScannableTableSpec;
use finalized_history_query::logs::table_specs::{
    BlobTableSpec, BlockLogBlobSpec, LogBitmapByBlockSpec, LogDirByBlockSpec,
};
use finalized_history_query::runtime::Runtime;
use finalized_history_query::store::blob::InMemoryBlobStore;
use finalized_history_query::store::meta::InMemoryMetaStore;
use finalized_history_query::store::publication::{
    CasOutcome, MetaPublicationStore, PublicationState, PublicationStore,
};
use finalized_history_query::store::publication::{
    PUBLICATION_STATE_SUFFIX, PUBLICATION_STATE_TABLE,
};
use finalized_history_query::store::traits::{BlobStore, MetaStore, PutCond};
use finalized_history_query::{Error, WriteAuthority, WriteContinuity, WriteSession};
use futures::executor::block_on;

use helpers::*;

const STREAM_PAGE_LOCAL_ID_SPAN: u32 = 4_096;

struct BootstrapRaceStore {
    state: Mutex<Option<PublicationState>>,
    losing_owner: PublicationState,
}

impl BootstrapRaceStore {
    fn new(losing_owner: PublicationState) -> Self {
        Self {
            state: Mutex::new(None),
            losing_owner,
        }
    }
}

impl PublicationStore for BootstrapRaceStore {
    async fn load(&self) -> finalized_history_query::Result<Option<PublicationState>> {
        Ok(self.state.lock().expect("state lock").clone())
    }

    async fn create_if_absent(
        &self,
        _initial: &PublicationState,
    ) -> finalized_history_query::Result<CasOutcome<PublicationState>> {
        let mut guard = self.state.lock().expect("state lock");
        if guard.is_none() {
            *guard = Some(self.losing_owner.clone());
        }
        Ok(CasOutcome::Failed {
            current: guard.clone(),
        })
    }

    async fn compare_and_set(
        &self,
        expected: &PublicationState,
        next: &PublicationState,
    ) -> finalized_history_query::Result<CasOutcome<PublicationState>> {
        let mut guard = self.state.lock().expect("state lock");
        match guard.as_ref() {
            Some(current) if current == expected => {
                *guard = Some(next.clone());
                Ok(CasOutcome::Applied(next.clone()))
            }
            Some(current) => Ok(CasOutcome::Failed {
                current: Some(current.clone()),
            }),
            None => Ok(CasOutcome::Failed { current: None }),
        }
    }
}

fn publication_store(store: &InMemoryMetaStore) -> MetaPublicationStore<InMemoryMetaStore> {
    MetaPublicationStore::new(store.clone())
}

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
            .meta_store()
            .get(PUBLICATION_STATE_TABLE, PUBLICATION_STATE_SUFFIX)
            .await
            .expect("publication state get")
            .expect("publication state");
        let publication_state =
            finalized_history_query::store::publication::PublicationState::decode(
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
            svc.meta_store()
                .scan_get(
                    LogDirByBlockSpec::TABLE,
                    &LogDirByBlockSpec::partition(0),
                    &LogDirByBlockSpec::clustering(1),
                )
                .await
                .expect("directory fragment get")
                .is_some()
        );

        let sid = finalized_history_query::kernel::sharded_streams::sharded_stream_id(
            "addr",
            &[1; 20],
            finalized_history_query::core::ids::LogShard::new(0)
                .unwrap()
                .get(),
        );
        let page_start = page_start_local(0, STREAM_PAGE_LOCAL_ID_SPAN);
        assert!(
            svc.meta_store()
                .scan_get(
                    LogBitmapByBlockSpec::TABLE,
                    &LogBitmapByBlockSpec::partition(&sid, page_start),
                    &LogBitmapByBlockSpec::clustering(1),
                )
                .await
                .expect("stream fragment get")
                .is_some()
        );
    });
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
fn acquire_publication_does_not_accept_foreign_owner_after_bootstrap_race() {
    block_on(async {
        let store = BootstrapRaceStore::new(PublicationState {
            owner_id: 9,
            session_id: [9u8; 16],
            indexed_finalized_head: 0,
            lease_valid_through_block: 500,
        });
        let authority = LeaseAuthority::new(store, 7, 100, 0);

        let session = authority
            .begin_write(Some(1_000))
            .await
            .expect("acquire publication");

        assert_eq!(session.state().indexed_finalized_head, 0);
        assert_eq!(session.state().continuity, WriteContinuity::Reacquired);
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
fn acquire_fails_closed_without_observed_finalized_block() {
    block_on(async {
        let store = InMemoryMetaStore::default();
        let authority = LeaseAuthority::new(publication_store(&store), 7, 50, 0);

        let err = authority
            .begin_write(None)
            .await
            .expect_err("missing observation should fail closed");

        assert!(matches!(err, Error::LeaseObservationUnavailable));
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
fn same_owner_restart_after_expiry_uses_new_session() {
    block_on(async {
        let store = InMemoryMetaStore::default();
        let first = LeaseAuthority::new(publication_store(&store), 7, 50, 0);
        let second = LeaseAuthority::new(publication_store(&store), 7, 50, 0);

        first
            .begin_write(Some(100))
            .await
            .expect("first acquire publication");
        let first_session = publication_store(&store)
            .load()
            .await
            .expect("load")
            .expect("state")
            .session_id;
        second
            .begin_write(Some(151))
            .await
            .expect("same owner restart after expiry");
        let second_session = publication_store(&store)
            .load()
            .await
            .expect("load")
            .expect("state")
            .session_id;

        assert_ne!(second_session, first_session);
    });
}

#[test]
fn same_owner_restart_before_expiry_uses_new_session() {
    block_on(async {
        let store = InMemoryMetaStore::default();
        let first = LeaseAuthority::new(publication_store(&store), 7, 50, 0);
        let second = LeaseAuthority::new(publication_store(&store), 7, 50, 0);

        first
            .begin_write(Some(100))
            .await
            .expect("first acquire publication");
        let store_view = publication_store(&store);
        let first_session = store_view
            .load()
            .await
            .expect("load")
            .expect("state")
            .session_id;
        second
            .begin_write(Some(120))
            .await
            .expect("same owner restart before expiry");
        let second_session = store_view
            .load()
            .await
            .expect("load")
            .expect("state")
            .session_id;

        assert_ne!(second_session, first_session);

        let state = publication_store(&store)
            .load()
            .await
            .expect("load")
            .expect("publication state");
        assert_eq!(state.owner_id, 7);
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
fn begin_write_returns_lease_lost_after_external_takeover() {
    block_on(async {
        let store = InMemoryMetaStore::default();
        let authority = LeaseAuthority::new(publication_store(&store), 7, 50, 10);
        authority.begin_write(Some(100)).await.expect("acquire");
        let takeover = LeaseAuthority::new(publication_store(&store), 8, 50, 10);
        let _ = takeover.begin_write(Some(151)).await.expect("takeover");

        let err = authority
            .begin_write(Some(151))
            .await
            .expect_err("begin_write should observe takeover");

        assert!(matches!(err, Error::LeaseLost));
    });
}

#[test]
fn publish_returns_lease_lost_on_session_mismatch() {
    block_on(async {
        let store = InMemoryMetaStore::default();
        let authority = LeaseAuthority::new(publication_store(&store), 7, 50, 0);
        let session = authority.begin_write(Some(100)).await.expect("acquire");
        let takeover = LeaseAuthority::new(publication_store(&store), 8, 50, 0);
        let _ = takeover.begin_write(Some(151)).await.expect("takeover");

        let err = session
            .publish(1, Some(151))
            .await
            .expect_err("publish should fail after takeover");

        assert!(matches!(err, Error::LeaseLost));
    });
}

#[test]
fn publish_returns_publication_conflict_on_head_mismatch() {
    block_on(async {
        let store = InMemoryMetaStore::default();
        let authority = LeaseAuthority::new(publication_store(&store), 7, 50, 0);
        let session = authority.begin_write(Some(100)).await.expect("acquire");
        let pub_store = publication_store(&store);
        let current = pub_store.load().await.expect("load").expect("state");
        let next = PublicationState {
            indexed_finalized_head: 9,
            ..current.clone()
        };
        let _ = pub_store
            .compare_and_set(&current, &next)
            .await
            .expect("mutate state");

        let err = session
            .publish(1, Some(100))
            .await
            .expect_err("publish should reject head mismatch");

        assert!(matches!(err, Error::PublicationConflict));
    });
}

#[test]
fn same_session_reacquire_after_expiry_keeps_session() {
    block_on(async {
        let store = InMemoryMetaStore::default();
        let authority = LeaseAuthority::new(publication_store(&store), 7, 50, 0);

        let first_continuity = authority
            .begin_write(Some(100))
            .await
            .expect("first acquire")
            .state()
            .continuity;
        assert_eq!(first_continuity, WriteContinuity::Fresh);
        let first_session = publication_store(&store)
            .load()
            .await
            .expect("load")
            .expect("state")
            .session_id;
        let second_continuity = authority
            .begin_write(Some(150))
            .await
            .expect("reacquire after expiry")
            .state()
            .continuity;
        assert_eq!(second_continuity, WriteContinuity::Reacquired);
        let second_session = publication_store(&store)
            .load()
            .await
            .expect("load")
            .expect("state")
            .session_id;

        assert_eq!(second_session, first_session);
    });
}

#[test]
fn same_session_acquire_before_expiry_keeps_session() {
    block_on(async {
        let store = InMemoryMetaStore::default();
        let authority = LeaseAuthority::new(publication_store(&store), 7, 50, 0);

        let first_continuity = authority
            .begin_write(Some(100))
            .await
            .expect("first acquire")
            .state()
            .continuity;
        assert_eq!(first_continuity, WriteContinuity::Fresh);
        let first_session = publication_store(&store)
            .load()
            .await
            .expect("load")
            .expect("state")
            .session_id;
        let second_continuity = authority
            .begin_write(Some(140))
            .await
            .expect("reuse before expiry")
            .state()
            .continuity;
        assert_eq!(second_continuity, WriteContinuity::Continuous);
        let second_session = publication_store(&store)
            .load()
            .await
            .expect("load")
            .expect("state")
            .session_id;

        assert_eq!(second_session, first_session);
    });
}

#[test]
fn lease_blocks_grants_exact_n_blocks() {
    block_on(async {
        let store = InMemoryMetaStore::default();
        let first = LeaseAuthority::new(publication_store(&store), 7, 10, 0);
        let second = LeaseAuthority::new(publication_store(&store), 8, 10, 0);

        let _ = first.begin_write(Some(100)).await.expect("bootstrap");
        let err = second
            .begin_write(Some(109))
            .await
            .expect_err("should be still fresh at last valid block");
        assert!(matches!(err, Error::LeaseStillFresh));

        second
            .begin_write(Some(110))
            .await
            .expect("takeover at first expired block");
        assert_eq!(
            publication_store(&store)
                .load()
                .await
                .expect("load")
                .expect("state")
                .owner_id,
            8
        );
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
                logs: Some(PrimaryWindowRecord {
                    first_primary_id: 9,
                    count: 1,
                }),
                txs: Some(PrimaryWindowRecord {
                    first_primary_id: 0,
                    count: 0,
                }),
                traces: Some(PrimaryWindowRecord {
                    first_primary_id: 0,
                    count: 0,
                }),
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
            finalized_history_query::store::publication::PublicationState::decode(&record.value)
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
        let runtime = Runtime::new(meta.clone(), blob.clone(), config.bytes_cache);
        let authority =
            LeaseAuthority::new(MetaPublicationStore::new(Arc::new(meta.clone())), 1, 50, 0);
        let engine = IngestEngine::new(config, authority, Families::default());

        engine
            .authority
            .begin_write(controlled_observed_finalized_block())
            .await
            .expect("bootstrap");

        let err = engine
            .ingest_finalized_block(
                &runtime,
                &mk_block(1, [0; 32], vec![mk_log(1, 10, 20, 1, 0, 0)]),
            )
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
        let runtime = Runtime::new(
            meta.clone(),
            blob.clone(),
            finalized_history_query::tables::BytesCacheConfig::default(),
        );
        let authority =
            LeaseAuthority::new(MetaPublicationStore::new(Arc::new(meta.clone())), 1, 50, 0);
        let engine = IngestEngine::new(lease_writer_config(), authority, Families::default());

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
            .ingest_finalized_block(
                &runtime,
                &mk_block(1, [0; 32], vec![mk_log(1, 10, 20, 1, 0, 0)]),
            )
            .await
            .expect_err("stale writer ingest should fail");
        assert!(matches!(err, Error::LeaseLost));

        assert!(
            runtime
                .meta_store
                .get(BLOCK_RECORD_TABLE, &BlockRecordSpec::key(1))
                .await
                .expect("read block meta")
                .is_none(),
            "stale writer should not write block metadata after takeover"
        );
        assert!(
            runtime
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
