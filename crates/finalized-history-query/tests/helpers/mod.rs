use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use bytes::Bytes;
use finalized_history_query::api::{
    ExecutionBudget, FinalizedHistoryService, QueryLogsRequest, QueryOrder,
};
use finalized_history_query::config::Config;
use finalized_history_query::domain::keys::PUBLICATION_STATE_TABLE;
use finalized_history_query::domain::types::PublicationState;
use finalized_history_query::ingest::authority::lease::LeaseAuthority;
use finalized_history_query::logs::table_specs::{BlobTableSpec, BlockLogBlobSpec};
use finalized_history_query::logs::types::{Block, Log};
use finalized_history_query::store::blob::InMemoryBlobStore;
use finalized_history_query::store::meta::InMemoryMetaStore;
use finalized_history_query::store::publication::{MetaPublicationStore, PublicationStore};
use finalized_history_query::store::traits::{
    BlobStore, BlobTableId, DelCond, MetaStore, Page, PutCond, PutResult, Record, ScannableTableId,
    TableId,
};
use finalized_history_query::{Clause, Error, LogFilter, WriteAuthority, WriteSession};

pub static CONTROLLED_OBSERVED_FINALIZED_BLOCK: AtomicU64 = AtomicU64::new(0);

pub fn mk_log(
    address: u8,
    topic0: u8,
    topic1: u8,
    block_num: u64,
    tx_idx: u32,
    log_idx: u32,
) -> Log {
    Log {
        address: [address; 20],
        topics: vec![[topic0; 32], [topic1; 32]],
        data: vec![address, topic0, topic1],
        block_num,
        tx_idx,
        log_idx,
        block_hash: [block_num as u8; 32],
    }
}

pub fn mk_block(block_num: u64, parent_hash: [u8; 32], logs: Vec<Log>) -> Block {
    Block {
        block_num,
        block_hash: [block_num as u8; 32],
        parent_hash,
        logs,
        txs: Vec::new(),
        traces: Vec::new(),
    }
}

pub fn indexed_address_filter(address: u8) -> LogFilter {
    LogFilter {
        address: Some(Clause::One([address; 20])),
        topic0: None,
        topic1: None,
        topic2: None,
        topic3: None,
    }
}

pub fn seeded_publication_state(
    owner_id: u64,
    session_id: [u8; 16],
    indexed_finalized_head: u64,
) -> PublicationState {
    seeded_publication_state_with_valid_through(
        owner_id,
        session_id,
        indexed_finalized_head,
        u64::MAX,
    )
}

pub fn seeded_publication_state_with_valid_through(
    owner_id: u64,
    session_id: [u8; 16],
    indexed_finalized_head: u64,
    lease_valid_through_block: u64,
) -> PublicationState {
    PublicationState {
        owner_id,
        session_id,
        indexed_finalized_head,
        lease_valid_through_block,
    }
}

pub fn static_observed_finalized_block() -> Option<u64> {
    Some(u64::MAX / 4)
}

pub fn lease_writer_config() -> Config {
    Config {
        observe_upstream_finalized_block: Arc::new(static_observed_finalized_block),
        ..Config::default()
    }
}

pub async fn query_page<A, M, B>(
    svc: &FinalizedHistoryService<A, M, B>,
    from_block: u64,
    to_block: u64,
    filter: LogFilter,
    limit: usize,
    resume_log_id: Option<u64>,
) -> finalized_history_query::Result<finalized_history_query::core::page::QueryPage<Log>>
where
    A: WriteAuthority,
    M: MetaStore,
    B: BlobStore,
{
    svc.query_logs(
        QueryLogsRequest {
            from_block,
            to_block,
            order: QueryOrder::Ascending,
            resume_log_id,
            limit,
            filter,
        },
        ExecutionBudget { max_results: None },
    )
    .await
}

pub async fn acquire_lease<P: PublicationStore + Clone>(
    publication_store: P,
    owner_id: u64,
    observed_upstream_finalized_block: u64,
    lease_blocks: u64,
) -> finalized_history_query::Result<u64> {
    let authority = LeaseAuthority::new(publication_store, owner_id, lease_blocks, 0);
    let session = authority
        .begin_write(Some(observed_upstream_finalized_block))
        .await?;
    Ok(session.state().indexed_finalized_head)
}

pub fn controlled_observed_finalized_block() -> Option<u64> {
    Some(CONTROLLED_OBSERVED_FINALIZED_BLOCK.load(Ordering::Relaxed))
}

#[derive(Clone)]
pub struct ExpireBeforePublishMetaStore {
    pub inner: Arc<InMemoryMetaStore>,
    pub advanced: Arc<AtomicBool>,
}

impl ExpireBeforePublishMetaStore {
    fn publication_store(&self) -> MetaPublicationStore<Self> {
        MetaPublicationStore::new(self.clone())
    }
}

impl MetaStore for ExpireBeforePublishMetaStore {
    async fn get(
        &self,
        family: TableId,
        key: &[u8],
    ) -> finalized_history_query::Result<Option<Record>> {
        self.inner.get(family, key).await
    }

    async fn put(
        &self,
        family: TableId,
        key: &[u8],
        value: Bytes,
        cond: PutCond,
    ) -> finalized_history_query::Result<PutResult> {
        if family == PUBLICATION_STATE_TABLE && matches!(cond, PutCond::IfVersion(_)) {
            let Some(current) = self.inner.get(family, key).await? else {
                return self.inner.put(family, key, value, cond).await;
            };
            let current_state = PublicationState::decode(&current.value)?;
            let next_state = PublicationState::decode(&value)?;
            if next_state.indexed_finalized_head > current_state.indexed_finalized_head
                && current_state.lease_valid_through_block
                    < CONTROLLED_OBSERVED_FINALIZED_BLOCK.load(Ordering::Relaxed)
            {
                return Ok(PutResult {
                    applied: false,
                    version: Some(current.version),
                });
            }
        }
        let result = self.inner.put(family, key, value, cond).await?;
        if family != PUBLICATION_STATE_TABLE && !self.advanced.swap(true, Ordering::Relaxed) {
            let publication_state = self
                .publication_store()
                .load()
                .await?
                .expect("publication state should exist before artifact writes");
            CONTROLLED_OBSERVED_FINALIZED_BLOCK.store(
                publication_state
                    .lease_valid_through_block
                    .saturating_add(1),
                Ordering::Relaxed,
            );
        }
        Ok(result)
    }

    async fn delete(
        &self,
        family: TableId,
        key: &[u8],
        cond: DelCond,
    ) -> finalized_history_query::Result<()> {
        self.inner.delete(family, key, cond).await
    }

    async fn scan_get(
        &self,
        family: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
    ) -> finalized_history_query::Result<Option<Record>> {
        self.inner.scan_get(family, partition, clustering).await
    }

    async fn scan_put(
        &self,
        family: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
        value: Bytes,
        cond: PutCond,
    ) -> finalized_history_query::Result<PutResult> {
        let result = self
            .inner
            .scan_put(family, partition, clustering, value, cond)
            .await?;
        if !self.advanced.swap(true, Ordering::Relaxed) {
            let publication_state = self
                .publication_store()
                .load()
                .await?
                .expect("publication state should exist before artifact writes");
            CONTROLLED_OBSERVED_FINALIZED_BLOCK.store(
                publication_state
                    .lease_valid_through_block
                    .saturating_add(1),
                Ordering::Relaxed,
            );
        }
        Ok(result)
    }

    async fn scan_delete(
        &self,
        family: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
        cond: DelCond,
    ) -> finalized_history_query::Result<()> {
        self.inner
            .scan_delete(family, partition, clustering, cond)
            .await
    }

    async fn scan_list(
        &self,
        family: ScannableTableId,
        partition: &[u8],
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> finalized_history_query::Result<Page> {
        self.inner
            .scan_list(family, partition, prefix, cursor, limit)
            .await
    }
}

#[derive(Clone)]
pub struct PublishConflictOnceMetaStore {
    pub inner: Arc<InMemoryMetaStore>,
    pub conflicted: Arc<AtomicBool>,
}

impl MetaStore for PublishConflictOnceMetaStore {
    async fn get(
        &self,
        family: TableId,
        key: &[u8],
    ) -> finalized_history_query::Result<Option<Record>> {
        self.inner.get(family, key).await
    }

    async fn put(
        &self,
        family: TableId,
        key: &[u8],
        value: Bytes,
        cond: PutCond,
    ) -> finalized_history_query::Result<PutResult> {
        if family == PUBLICATION_STATE_TABLE
            && matches!(cond, PutCond::IfVersion(_))
            && !self.conflicted.swap(true, Ordering::Relaxed)
        {
            let result = self.inner.put(family, key, value, cond).await?;
            return Ok(PutResult {
                applied: false,
                version: result.version,
            });
        }
        self.inner.put(family, key, value, cond).await
    }

    async fn delete(
        &self,
        family: TableId,
        key: &[u8],
        cond: DelCond,
    ) -> finalized_history_query::Result<()> {
        self.inner.delete(family, key, cond).await
    }

    async fn scan_get(
        &self,
        family: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
    ) -> finalized_history_query::Result<Option<Record>> {
        self.inner.scan_get(family, partition, clustering).await
    }

    async fn scan_put(
        &self,
        family: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
        value: Bytes,
        cond: PutCond,
    ) -> finalized_history_query::Result<PutResult> {
        self.inner
            .scan_put(family, partition, clustering, value, cond)
            .await
    }

    async fn scan_delete(
        &self,
        family: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
        cond: DelCond,
    ) -> finalized_history_query::Result<()> {
        self.inner
            .scan_delete(family, partition, clustering, cond)
            .await
    }

    async fn scan_list(
        &self,
        family: ScannableTableId,
        partition: &[u8],
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> finalized_history_query::Result<Page> {
        self.inner
            .scan_list(family, partition, prefix, cursor, limit)
            .await
    }
}

#[derive(Clone)]
pub struct CountingBlobStore {
    pub inner: Arc<InMemoryBlobStore>,
    pub target_key: Vec<u8>,
    pub get_blob_calls: Arc<AtomicU64>,
    pub read_range_calls: Arc<AtomicU64>,
    pub read_range_bytes: Arc<AtomicU64>,
}

impl BlobStore for CountingBlobStore {
    async fn put_blob(
        &self,
        table: BlobTableId,
        key: &[u8],
        value: Bytes,
    ) -> finalized_history_query::Result<()> {
        self.inner.put_blob(table, key, value).await
    }

    async fn get_blob(
        &self,
        table: BlobTableId,
        key: &[u8],
    ) -> finalized_history_query::Result<Option<Bytes>> {
        if table == BlockLogBlobSpec::TABLE && key == self.target_key.as_slice() {
            self.get_blob_calls.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.get_blob(table, key).await
    }

    async fn read_range(
        &self,
        table: BlobTableId,
        key: &[u8],
        start: u64,
        end_exclusive: u64,
    ) -> finalized_history_query::Result<Option<Bytes>> {
        if table == BlockLogBlobSpec::TABLE && key == self.target_key.as_slice() {
            self.read_range_calls.fetch_add(1, Ordering::Relaxed);
            self.read_range_bytes
                .fetch_add(end_exclusive.saturating_sub(start), Ordering::Relaxed);
        }
        self.inner
            .read_range(table, key, start, end_exclusive)
            .await
    }

    async fn delete_blob(
        &self,
        table: BlobTableId,
        key: &[u8],
    ) -> finalized_history_query::Result<()> {
        self.inner.delete_blob(table, key).await
    }

    async fn list_prefix(
        &self,
        table: BlobTableId,
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> finalized_history_query::Result<Page> {
        self.inner.list_prefix(table, prefix, cursor, limit).await
    }
}
