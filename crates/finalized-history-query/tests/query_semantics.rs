#[allow(dead_code, unused_imports)]
mod helpers;

use finalized_history_query::api::FinalizedHistoryService;
use finalized_history_query::store::blob::InMemoryBlobStore;
use finalized_history_query::store::meta::InMemoryMetaStore;
use futures::executor::block_on;

use helpers::*;

#[test]
fn ingest_and_query_with_limits_and_resume() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        svc.ingest_finalized_block(mk_block(
            1,
            [0; 32],
            vec![mk_log(1, 10, 20, 1, 0, 0), mk_log(2, 11, 21, 1, 0, 1)],
        ))
        .await
        .expect("ingest block 1");
        svc.ingest_finalized_block(mk_block(
            2,
            [1; 32],
            vec![mk_log(1, 10, 22, 2, 0, 0), mk_log(1, 12, 23, 2, 0, 1)],
        ))
        .await
        .expect("ingest block 2");

        let first = query_page(&svc, 1, 2, indexed_address_filter(1), 2, None)
            .await
            .expect("first page");
        assert_eq!(first.items.len(), 2);
        assert!(first.meta.has_more);
        assert_eq!(first.meta.next_resume_log_id, Some(2));

        let second = query_page(
            &svc,
            1,
            2,
            indexed_address_filter(1),
            2,
            first.meta.next_resume_log_id,
        )
        .await
        .expect("second page");
        assert_eq!(second.items.len(), 1);
        assert!(!second.meta.has_more);
        assert_eq!(second.items[0].block_num, 2);
    });
}

#[test]
fn query_range_clips_to_published_head() {
    block_on(async {
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );
        svc.ingest_finalized_block(mk_block(1, [0; 32], vec![mk_log(1, 10, 20, 1, 0, 0)]))
            .await
            .expect("ingest");

        let page = query_page(&svc, 1, 99, indexed_address_filter(1), 10, None)
            .await
            .expect("query");
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.meta.resolved_to_block.number, 1);
    });
}
