#[allow(dead_code, unused_imports)]
mod helpers;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use finalized_history_query::api::FinalizedHistoryService;
use finalized_history_query::core::state::{
    BLOCK_RECORD_TABLE, BlockRecord, BlockRecordSpec, PrimaryWindowRecord,
};
use finalized_history_query::kernel::codec::StorageCodec;
use finalized_history_query::kernel::table_specs::ScannableTableSpec;
use finalized_history_query::logs::table_specs::OpenBitmapPageSpec;
use finalized_history_query::store::blob::InMemoryBlobStore;
use finalized_history_query::store::meta::InMemoryMetaStore;
use finalized_history_query::store::traits::{
    DelCond, MetaStore, Page, PutCond, PutResult, Record, ScannableTableId, TableId,
};
use finalized_history_query::traces::table_specs::TraceOpenBitmapPageSpec;
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

#[derive(Clone, Default)]
struct CountingMetaStore {
    inner: InMemoryMetaStore,
    open_page_scan_lists: Arc<AtomicUsize>,
}

impl CountingMetaStore {
    fn take_open_page_scan_lists(&self) -> usize {
        self.open_page_scan_lists.swap(0, Ordering::Relaxed)
    }
}

impl MetaStore for CountingMetaStore {
    async fn get(
        &self,
        table: TableId,
        key: &[u8],
    ) -> finalized_history_query::Result<Option<Record>> {
        self.inner.get(table, key).await
    }

    async fn put(
        &self,
        table: TableId,
        key: &[u8],
        value: Bytes,
        cond: PutCond,
    ) -> finalized_history_query::Result<PutResult> {
        self.inner.put(table, key, value, cond).await
    }

    async fn delete(
        &self,
        table: TableId,
        key: &[u8],
        cond: DelCond,
    ) -> finalized_history_query::Result<()> {
        self.inner.delete(table, key, cond).await
    }

    async fn scan_get(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
    ) -> finalized_history_query::Result<Option<Record>> {
        self.inner.scan_get(table, partition, clustering).await
    }

    async fn scan_put(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
        value: Bytes,
        cond: PutCond,
    ) -> finalized_history_query::Result<PutResult> {
        self.inner
            .scan_put(table, partition, clustering, value, cond)
            .await
    }

    async fn scan_delete(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
        cond: DelCond,
    ) -> finalized_history_query::Result<()> {
        self.inner
            .scan_delete(table, partition, clustering, cond)
            .await
    }

    async fn scan_list(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> finalized_history_query::Result<Page> {
        if matches!(
            table,
            OpenBitmapPageSpec::TABLE | TraceOpenBitmapPageSpec::TABLE
        ) {
            self.open_page_scan_lists.fetch_add(1, Ordering::Relaxed);
        }
        self.inner
            .scan_list(table, partition, prefix, cursor, limit)
            .await
    }
}

#[test]
fn continuous_writer_ingest_skips_open_page_repair_scans() {
    block_on(async {
        let meta = CountingMetaStore::default();
        let blob = InMemoryBlobStore::default();
        let svc = FinalizedHistoryService::new_reader_writer(
            lease_writer_config(),
            meta.clone(),
            blob,
            5,
        );

        svc.ingest_finalized_block(mk_block(1, [0; 32], vec![]))
            .await
            .expect("first ingest");
        assert!(
            meta.take_open_page_scan_lists() > 0,
            "fresh acquisition should perform recovery scans"
        );

        svc.ingest_finalized_block(mk_block(2, [1; 32], vec![]))
            .await
            .expect("second ingest");
        assert_eq!(
            meta.take_open_page_scan_lists(),
            0,
            "continuous lease renewals should not rescan open-page tables"
        );
    });
}

#[test]
fn ingest_ignores_unpublished_suffix_artifacts_during_recovery() {
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
