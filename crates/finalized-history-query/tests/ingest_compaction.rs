#[allow(dead_code, unused_imports)]
mod helpers;

use std::sync::Arc;

use bytes::Bytes;
use finalized_history_query::api::FinalizedHistoryService;
use finalized_history_query::core::state::{
    BLOCK_RECORD_TABLE, BlockRecord, BlockRecordSpec, PrimaryWindowRecord,
};
use finalized_history_query::kernel::codec::StorageCodec;
use finalized_history_query::kernel::sharded_streams::page_start_local;
use finalized_history_query::kernel::table_specs::{page_stream_key, stream_page_key, u64_key};
use finalized_history_query::logs::keys::{
    BITMAP_BY_BLOCK_TABLE, BITMAP_PAGE_META_TABLE, DIRECTORY_SUB_BUCKET_SIZE,
    LOG_DIR_BY_BLOCK_TABLE, MAX_LOCAL_ID, OPEN_BITMAP_PAGE_TABLE, STREAM_PAGE_LOCAL_ID_SPAN,
};
use finalized_history_query::logs::table_specs::{
    BitmapPageBlobSpec, BitmapPageMetaSpec, BlobTableSpec, LogDirByBlockSpec,
};
use finalized_history_query::store::blob::InMemoryBlobStore;
use finalized_history_query::store::meta::InMemoryMetaStore;
use finalized_history_query::store::publication::{MetaPublicationStore, PublicationStore};
use finalized_history_query::store::traits::{BlobStore, MetaStore, PutCond};
use finalized_history_query::streams::{BitmapBlob, encode_bitmap_blob};
use futures::executor::block_on;
use roaring::RoaringBitmap;

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
            shared_block_record(
                [1; 32],
                [0; 32],
                Some((u64::from(MAX_LOCAL_ID), 0)),
                Some((0, 0)),
            )
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
            shared_block_record([1; 32], [0; 32], Some((first_log_id, 0)), Some((0, 0))).encode(),
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

        let sid = finalized_history_query::kernel::sharded_streams::sharded_stream_id(
            "addr",
            &[5; 20],
            finalized_history_query::core::ids::LogShard::new(0)
                .unwrap()
                .get(),
        );
        let page_start = page_start_local(STREAM_PAGE_LOCAL_ID_SPAN - 1, STREAM_PAGE_LOCAL_ID_SPAN);
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
            shared_block_record(
                [1; 32],
                [0; 32],
                Some((DIRECTORY_SUB_BUCKET_SIZE - 2, 0)),
                Some((0, 0)),
            )
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

#[test]
fn direct_ingest_repairs_stale_sealed_open_page_markers_before_writing() {
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
            shared_block_record([1; 32], [0; 32], Some((first_log_id, 1)), Some((0, 0))).encode(),
            PutCond::Any,
        )
        .await
        .expect("seed block meta");

        let sid = finalized_history_query::kernel::sharded_streams::sharded_stream_id(
            "addr",
            &[5; 20],
            finalized_history_query::core::ids::LogShard::new(0)
                .unwrap()
                .get(),
        );
        let page_start = page_start_local(STREAM_PAGE_LOCAL_ID_SPAN - 1, STREAM_PAGE_LOCAL_ID_SPAN);
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(STREAM_PAGE_LOCAL_ID_SPAN - 1);
        meta.scan_put(
            BITMAP_BY_BLOCK_TABLE,
            &stream_page_key(&sid, page_start),
            &u64_key(1),
            encode_bitmap_blob(&BitmapBlob {
                min_local: STREAM_PAGE_LOCAL_ID_SPAN - 1,
                max_local: STREAM_PAGE_LOCAL_ID_SPAN - 1,
                count: 1,
                crc32: 0,
                bitmap,
            })
            .expect("encode bitmap fragment"),
            PutCond::Any,
        )
        .await
        .expect("seed bitmap fragment");
        meta.scan_put(
            OPEN_BITMAP_PAGE_TABLE,
            &u64_key(0),
            &page_stream_key(page_start, &sid),
            Bytes::new(),
            PutCond::Any,
        )
        .await
        .expect("seed stale open page marker");

        let svc = FinalizedHistoryService::new_reader_writer(lease_writer_config(), meta, blob, 2);
        svc.ingest_finalized_block(mk_block(2, [1; 32], vec![]))
            .await
            .expect("ingest should repair stale markers");

        assert!(
            svc.meta_store()
                .get(
                    BITMAP_PAGE_META_TABLE,
                    &BitmapPageMetaSpec::key(&sid, page_start),
                )
                .await
                .expect("stream page meta after repair")
                .is_some()
        );
        assert!(
            svc.meta_store()
                .scan_get(
                    OPEN_BITMAP_PAGE_TABLE,
                    &u64_key(0),
                    &page_stream_key(page_start, &sid),
                )
                .await
                .expect("open page marker after repair")
                .is_none()
        );
    });
}
