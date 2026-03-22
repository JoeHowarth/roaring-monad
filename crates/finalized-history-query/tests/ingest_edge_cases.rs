#[allow(dead_code, unused_imports)]
mod helpers;

use finalized_history_query::Error;
use finalized_history_query::api::FinalizedHistoryService;
use finalized_history_query::logs::keys::{BLOCK_LOG_HEADER_TABLE, BLOCK_RECORD_TABLE};
use finalized_history_query::logs::table_specs::{
    BlobTableSpec, BlockLogBlobSpec, BlockLogHeaderSpec, BlockRecordSpec,
};
use finalized_history_query::logs::types::{BlockLogHeader, BlockRecord};
use finalized_history_query::store::blob::InMemoryBlobStore;
use finalized_history_query::store::meta::InMemoryMetaStore;
use finalized_history_query::store::traits::{BlobStore, MetaStore};
use futures::executor::block_on;

use helpers::*;

// --- Block sequence validation ---

#[test]
fn ingest_rejects_empty_block_list() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        let err = svc
            .ingest_finalized_blocks(vec![])
            .await
            .expect_err("empty list");
        assert!(matches!(err, Error::InvalidParams(_)));
    });
}

#[test]
fn ingest_rejects_block_gap() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        svc.ingest_finalized_block(mk_block(1, [0; 32], vec![mk_log(1, 10, 20, 1, 0, 0)]))
            .await
            .expect("ingest block 1");

        // Skip block 2, try to ingest block 3.
        let err = svc
            .ingest_finalized_block(mk_block(3, [1; 32], vec![mk_log(1, 10, 20, 3, 0, 0)]))
            .await
            .expect_err("block gap");
        assert!(matches!(err, Error::InvalidSequence { expected: 2, .. }));
    });
}

#[test]
fn ingest_rejects_parent_hash_mismatch() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        svc.ingest_finalized_block(mk_block(1, [0; 32], vec![mk_log(1, 10, 20, 1, 0, 0)]))
            .await
            .expect("ingest block 1");

        // Block 2 with wrong parent hash.
        let err = svc
            .ingest_finalized_block(mk_block(
                2,
                [99; 32], // should be [1; 32]
                vec![mk_log(1, 10, 20, 2, 0, 0)],
            ))
            .await
            .expect_err("parent hash mismatch");
        assert!(matches!(err, Error::InvalidParent));
    });
}

#[test]
fn ingest_rejects_parent_hash_mismatch_within_batch() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        let block1 = mk_block(1, [0; 32], vec![mk_log(1, 10, 20, 1, 0, 0)]);
        let mut block2 = mk_block(2, [1; 32], vec![mk_log(1, 10, 21, 2, 0, 0)]);
        block2.parent_hash = [99; 32]; // wrong parent

        let err = svc
            .ingest_finalized_blocks(vec![block1, block2])
            .await
            .expect_err("intra-batch parent mismatch");
        assert!(matches!(err, Error::InvalidParent));
    });
}

// --- Empty logs in a block ---

#[test]
fn ingest_block_with_zero_logs_writes_empty_artifacts() {
    block_on(async {
        let meta = InMemoryMetaStore::default();
        let blob = InMemoryBlobStore::default();
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            meta.clone(),
            blob.clone(),
            1,
        );

        svc.ingest_finalized_block(mk_block(1, [0; 32], vec![]))
            .await
            .expect("ingest empty block");

        // block_record must exist with count=0.
        let record_bytes = meta
            .get(BLOCK_RECORD_TABLE, &BlockRecordSpec::key(1))
            .await
            .expect("read block record")
            .expect("block record must be present for empty block");
        let record = BlockRecord::decode(&record_bytes.value).expect("decode");
        assert_eq!(record.count, 0);

        // block_log_header must exist (sentinel with one offset).
        let header_bytes = meta
            .get(BLOCK_LOG_HEADER_TABLE, &BlockLogHeaderSpec::key(1))
            .await
            .expect("read header")
            .expect("block_log_header must be present for empty block");
        let header = BlockLogHeader::decode(&header_bytes.value).expect("decode");
        assert_eq!(header.offsets, vec![0]);

        // block_log_blob must exist (empty).
        let blob_bytes = blob
            .get_blob(BlockLogBlobSpec::TABLE, &BlockLogBlobSpec::key(1))
            .await
            .expect("read blob")
            .expect("block_log_blob must be present for empty block");
        assert!(blob_bytes.is_empty());
    });
}

#[test]
fn ingest_block_with_zero_logs_advances_head() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        // Block 1 has logs, block 2 has none.
        svc.ingest_finalized_block(mk_block(1, [0; 32], vec![mk_log(1, 10, 20, 1, 0, 0)]))
            .await
            .expect("ingest block 1");

        let outcome = svc
            .ingest_finalized_block(mk_block(2, [1; 32], vec![]))
            .await
            .expect("ingest empty block 2");
        assert_eq!(outcome.indexed_finalized_head, 2);
        assert_eq!(outcome.written_logs, 0);

        // Querying the range still works.
        let page = query_page(&svc, 1, 2, indexed_address_filter(1), 10, None)
            .await
            .expect("query spanning empty block");
        assert_eq!(page.items.len(), 1);
    });
}

#[test]
fn ingest_block_with_zero_logs_followed_by_block_with_logs() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        svc.ingest_finalized_block(mk_block(1, [0; 32], vec![]))
            .await
            .expect("ingest empty block 1");

        svc.ingest_finalized_block(mk_block(2, [1; 32], vec![mk_log(1, 10, 20, 2, 0, 0)]))
            .await
            .expect("ingest block 2");

        let page = query_page(&svc, 1, 2, indexed_address_filter(1), 10, None)
            .await
            .expect("query");
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].block_num, 2);
    });
}
