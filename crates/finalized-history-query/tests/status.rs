#[allow(dead_code, unused_imports)]
mod helpers;

use finalized_history_query::Config;
use finalized_history_query::Error;
use finalized_history_query::api::FinalizedHistoryService;
use finalized_history_query::core::state::{
    BLOCK_RECORD_TABLE, BlockRecord, BlockRecordSpec, PrimaryWindowRecord,
};
use finalized_history_query::family::Families;
use finalized_history_query::kernel::codec::StorageCodec;
use finalized_history_query::status::service_status;
use finalized_history_query::store::blob::InMemoryBlobStore;
use finalized_history_query::store::meta::InMemoryMetaStore;
use finalized_history_query::store::publication::{MetaPublicationStore, PublicationStore};
use finalized_history_query::store::publication::{
    PUBLICATION_STATE_SUFFIX, PUBLICATION_STATE_TABLE,
};
use finalized_history_query::store::traits::{MetaStore, PutCond};
use futures::executor::block_on;
use std::sync::Arc;

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
fn reader_only_status_is_observational_and_ingest_is_rejected() {
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
        let status = svc.status().await.expect("reader-only status");
        let state = publication_store
            .load()
            .await
            .expect("load")
            .expect("publication state");
        let err = svc
            .ingest_finalized_block(mk_block(4, [3; 32], vec![mk_log(1, 10, 20, 4, 0, 0)]))
            .await
            .expect_err("reader-only ingest should fail");

        assert_eq!(status.head_state.indexed_finalized_head, 3);
        assert_eq!(state.owner_id, 11);
        assert_eq!(state.session_id, [11u8; 16]);
        assert!(matches!(err, Error::ReadOnlyMode(_)));
    });
}

#[test]
fn status_does_not_take_publication_ownership() {
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
        let _ = service_status(&runtime, &publication_store, &Families::default())
            .await
            .expect("status should succeed");

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
fn status_recovers_trace_state_from_published_head_only() {
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
        let status = service_status(&runtime, &publication_store, &Families::default())
            .await
            .expect("status");

        assert_eq!(status.head_state.indexed_finalized_head, 2);
        assert_eq!(status.trace_state.next_trace_id.get(), 43);
    });
}

#[test]
fn status_rejects_missing_trace_records_for_nonzero_published_head() {
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
        let err = service_status(&runtime, &publication_store, &Families::default())
            .await
            .expect_err("status should fail closed when published trace head metadata is missing");

        assert!(matches!(err, finalized_history_query::Error::NotFound));
    });
}
