#[allow(dead_code, unused_imports)]
mod helpers;

use std::sync::Arc;

use finalized_history_query::api::FinalizedHistoryService;
use finalized_history_query::logs::keys::{
    BITMAP_PAGE_META_TABLE, BLOCK_RECORD_TABLE, LOG_DIR_BY_BLOCK_TABLE,
    LOG_DIRECTORY_SUB_BUCKET_SIZE, MAX_LOCAL_ID, STREAM_PAGE_LOCAL_ID_SPAN,
};
use finalized_history_query::logs::table_specs::{
    self, BitmapPageBlobSpec, BitmapPageMetaSpec, BlobTableSpec, BlockRecordSpec, LogDirByBlockSpec,
};
use finalized_history_query::logs::types::BlockRecord;
use finalized_history_query::store::blob::InMemoryBlobStore;
use finalized_history_query::store::meta::InMemoryMetaStore;
use finalized_history_query::store::publication::{MetaPublicationStore, PublicationStore};
use finalized_history_query::store::traits::{BlobStore, MetaStore, PutCond};
use futures::executor::block_on;

use helpers::*;

#[test]
fn ingest_and_query_across_24_bit_log_shard_boundary() {
    block_on(async {
        let meta = InMemoryMetaStore::default();
        let blob = InMemoryBlobStore::default();
        let publication_store = MetaPublicationStore::new(Arc::new(meta.clone()));
        assert!(matches!(
            publication_store
                .create_if_absent(&seeded_publication_state_with_valid_through(
                    1, [1u8; 16], 1, 0,
                ))
                .await
                .expect("seed publication state"),
            finalized_history_query::store::publication::CasOutcome::Applied(_)
        ));
        meta.put(
            BLOCK_RECORD_TABLE,
            &BlockRecordSpec::key(1),
            BlockRecord {
                block_hash: [1; 32],
                parent_hash: [0; 32],
                first_log_id: u64::from(MAX_LOCAL_ID),
                count: 0,
            }
            .encode(),
            PutCond::Any,
        )
        .await
        .expect("seed block meta");

        let svc = FinalizedHistoryService::new_reader_writer(lease_writer_config(), meta, blob, 1);
        svc.ingest_finalized_block(mk_block(
            2,
            [1; 32],
            vec![mk_log(7, 10, 20, 2, 0, 0), mk_log(7, 10, 21, 2, 0, 1)],
        ))
        .await
        .expect("ingest block 2");

        let page = query_page(&svc, 2, 2, indexed_address_filter(7), 10, None)
            .await
            .expect("query");
        assert_eq!(page.items.len(), 2);
    });
}

#[test]
fn sealed_sub_bucket_and_page_compaction_are_written_when_boundaries_close() {
    block_on(async {
        let meta = InMemoryMetaStore::default();
        let blob = InMemoryBlobStore::default();
        let first_log_id = u64::from(STREAM_PAGE_LOCAL_ID_SPAN - 1);
        let publication_store = MetaPublicationStore::new(Arc::new(meta.clone()));
        assert!(matches!(
            publication_store
                .create_if_absent(&seeded_publication_state_with_valid_through(
                    1, [1u8; 16], 1, 0,
                ))
                .await
                .expect("seed publication state"),
            finalized_history_query::store::publication::CasOutcome::Applied(_)
        ));
        meta.put(
            BLOCK_RECORD_TABLE,
            &BlockRecordSpec::key(1),
            BlockRecord {
                block_hash: [1; 32],
                parent_hash: [0; 32],
                first_log_id,
                count: 0,
            }
            .encode(),
            PutCond::Any,
        )
        .await
        .expect("seed block meta");

        let svc = FinalizedHistoryService::new_reader_writer(lease_writer_config(), meta, blob, 1);
        let block = mk_block(
            2,
            [1; 32],
            vec![mk_log(5, 10, 20, 2, 0, 0), mk_log(5, 10, 21, 2, 0, 1)],
        );
        svc.ingest_finalized_block(block).await.expect("ingest");

        let sid = table_specs::stream_id(
            "addr",
            &[5; 20],
            finalized_history_query::core::ids::LogShard::new(0).unwrap(),
        );
        let page_start = table_specs::stream_page_start_local(STREAM_PAGE_LOCAL_ID_SPAN - 1);
        assert!(
            svc.meta_store()
                .get(
                    BITMAP_PAGE_META_TABLE,
                    &BitmapPageMetaSpec::key(&sid, page_start),
                )
                .await
                .expect("stream page meta")
                .is_some()
        );
        assert!(
            svc.blob_store()
                .get_blob(
                    BitmapPageBlobSpec::TABLE,
                    &BitmapPageBlobSpec::key(&sid, page_start)
                )
                .await
                .expect("stream page blob")
                .is_some()
        );
    });
}

#[test]
fn directory_fragments_exist_for_blocks_crossing_sub_bucket_boundaries() {
    block_on(async {
        let meta = InMemoryMetaStore::default();
        let blob = InMemoryBlobStore::default();
        let publication_store = MetaPublicationStore::new(Arc::new(meta.clone()));
        assert!(matches!(
            publication_store
                .create_if_absent(&seeded_publication_state_with_valid_through(
                    1, [1u8; 16], 1, 0,
                ))
                .await
                .expect("seed publication state"),
            finalized_history_query::store::publication::CasOutcome::Applied(_)
        ));
        meta.put(
            BLOCK_RECORD_TABLE,
            &BlockRecordSpec::key(1),
            BlockRecord {
                block_hash: [1; 32],
                parent_hash: [0; 32],
                first_log_id: LOG_DIRECTORY_SUB_BUCKET_SIZE - 2,
                count: 0,
            }
            .encode(),
            PutCond::Any,
        )
        .await
        .expect("seed block meta");

        let svc = FinalizedHistoryService::new_reader_writer(lease_writer_config(), meta, blob, 1);
        svc.ingest_finalized_block(mk_block(
            2,
            [1; 32],
            vec![
                mk_log(9, 10, 20, 2, 0, 0),
                mk_log(9, 10, 21, 2, 0, 1),
                mk_log(9, 10, 22, 2, 0, 2),
            ],
        ))
        .await
        .expect("ingest");

        assert!(
            svc.meta_store()
                .scan_get(
                    LOG_DIR_BY_BLOCK_TABLE,
                    &LogDirByBlockSpec::partition(0),
                    &LogDirByBlockSpec::clustering(2),
                )
                .await
                .expect("directory fragment")
                .is_some()
        );
    });
}
