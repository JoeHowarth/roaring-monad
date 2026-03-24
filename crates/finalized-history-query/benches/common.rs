#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use finalized_history_query::api::{
    ExecutionBudget, FinalizedHistoryService, QueryLogsRequest, QueryOrder,
};
use finalized_history_query::config::Config;
use finalized_history_query::core::directory_resolver::ResolvedPrimaryLocation;
use finalized_history_query::core::ids::{LogId, LogLocalId, LogShard, compose_log_id};
use finalized_history_query::core::refs::BlockRef;
use finalized_history_query::core::state::{
    BLOCK_RECORD_TABLE, BlockRecord, BlockRecordSpec, PrimaryWindowRecord,
};
use finalized_history_query::kernel::codec::StorageCodec;
use finalized_history_query::kernel::table_specs::PointTableSpec;
use finalized_history_query::logs::codec::validate_log;
use finalized_history_query::logs::keys::{LOG_DIRECTORY_BUCKET_SIZE, MAX_LOCAL_ID};
use finalized_history_query::logs::materialize::LogMaterializer;
use finalized_history_query::logs::table_specs::{
    BlobTableSpec, BlockLogBlobSpec, BlockLogHeaderSpec, LogDirBucketSpec,
};
use finalized_history_query::logs::types::{BlockLogHeader, DirBucket, Log};
use finalized_history_query::query::runner::{QueryIdRange, QueryMaterializer, ShardBitmapSet};
use finalized_history_query::store::blob::InMemoryBlobStore;
use finalized_history_query::store::meta::InMemoryMetaStore;
use finalized_history_query::store::publication::MetaPublicationStore;
use finalized_history_query::store::publication::PublicationState;
use finalized_history_query::store::publication::{
    PUBLICATION_STATE_SUFFIX, PUBLICATION_STATE_TABLE,
};
use finalized_history_query::store::traits::{BlobStore, BlobTableId, MetaStore, PutCond, TableId};
use finalized_history_query::tables::{BytesCacheConfig, TableCacheConfig, Tables};
use finalized_history_query::{
    Clause, FinalizedBlock, LeaseAuthority, LogFilter, QueryPage, Result, WriteAuthority,
};
use futures::executor::block_on;
use roaring::RoaringBitmap;

pub const HIGH_SHARD: u64 = 0x1_0000_0000;
pub const DEFAULT_WRITER_ID: u64 = 1;

fn bench_hash(block_num: u64) -> [u8; 32] {
    let mut hash = [0u8; 32];
    hash[..8].copy_from_slice(&block_num.to_be_bytes());
    hash
}

fn static_observed_finalized_block() -> Option<u64> {
    Some(u64::MAX / 4)
}

pub type BenchService = FinalizedHistoryService<
    LeaseAuthority<MetaPublicationStore<InMemoryMetaStore>>,
    InMemoryMetaStore,
    InMemoryBlobStore,
>;
pub type CountingBenchService = FinalizedHistoryService<
    LeaseAuthority<MetaPublicationStore<InMemoryMetaStore>>,
    InMemoryMetaStore,
    CountingBlobStore,
>;

#[derive(Debug, Default)]
pub struct BlobAccessCounters {
    get_blob_calls: AtomicU64,
    read_range_calls: AtomicU64,
    read_range_bytes: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BlobAccessSnapshot {
    pub get_blob_calls: u64,
    pub read_range_calls: u64,
    pub read_range_bytes: u64,
}

impl BlobAccessCounters {
    pub fn reset(&self) {
        self.get_blob_calls.store(0, Ordering::Relaxed);
        self.read_range_calls.store(0, Ordering::Relaxed);
        self.read_range_bytes.store(0, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> BlobAccessSnapshot {
        BlobAccessSnapshot {
            get_blob_calls: self.get_blob_calls.load(Ordering::Relaxed),
            read_range_calls: self.read_range_calls.load(Ordering::Relaxed),
            read_range_bytes: self.read_range_bytes.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Default)]
pub struct CountingBlobStore {
    inner: Arc<InMemoryBlobStore>,
    counters: Arc<BlobAccessCounters>,
}

impl CountingBlobStore {
    pub fn counters(&self) -> Arc<BlobAccessCounters> {
        self.counters.clone()
    }
}

impl BlobStore for CountingBlobStore {
    async fn put_blob(&self, table: BlobTableId, key: &[u8], value: Bytes) -> Result<()> {
        self.inner.put_blob(table, key, value).await
    }

    async fn get_blob(&self, table: BlobTableId, key: &[u8]) -> Result<Option<Bytes>> {
        if table == BlockLogBlobSpec::TABLE {
            self.counters.get_blob_calls.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.get_blob(table, key).await
    }

    async fn read_range(
        &self,
        table: BlobTableId,
        key: &[u8],
        start: u64,
        end_exclusive: u64,
    ) -> Result<Option<Bytes>> {
        if table == BlockLogBlobSpec::TABLE {
            self.counters
                .read_range_calls
                .fetch_add(1, Ordering::Relaxed);
            self.counters
                .read_range_bytes
                .fetch_add(end_exclusive.saturating_sub(start), Ordering::Relaxed);
        }
        self.inner
            .read_range(table, key, start, end_exclusive)
            .await
    }

    async fn delete_blob(&self, table: BlobTableId, key: &[u8]) -> Result<()> {
        self.inner.delete_blob(table, key).await
    }

    async fn list_prefix(
        &self,
        table: BlobTableId,
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<finalized_history_query::store::traits::Page> {
        self.inner.list_prefix(table, prefix, cursor, limit).await
    }
}

#[derive(Debug, Clone)]
pub struct SeededLogBlock {
    pub block_num: u64,
    pub first_log_id: u64,
    pub logs: Vec<Log>,
}

#[derive(Debug, Clone, Copy)]
pub struct StubPrimary {
    pub id: LogId,
    pub block_ref: BlockRef,
}

#[derive(Debug, Default)]
pub struct PassThroughMaterializer {
    block_span: u64,
}

impl PassThroughMaterializer {
    pub fn new(block_span: u64) -> Self {
        Self {
            block_span: block_span.max(1),
        }
    }
}

impl QueryMaterializer for PassThroughMaterializer {
    type Filter = ();
    type Id = LogId;
    type Item = StubPrimary;
    type Output = StubPrimary;

    async fn resolve_id(&mut self, id: LogId) -> Result<Option<ResolvedPrimaryLocation>> {
        let block_num = (id.get() / self.block_span).saturating_add(1);
        Ok(Some(ResolvedPrimaryLocation {
            block_num,
            local_ordinal: id.local().get() as usize,
        }))
    }

    async fn load_run(
        &mut self,
        run: &[(Self::Id, ResolvedPrimaryLocation)],
    ) -> Result<Vec<(Self::Id, Self::Item)>> {
        Ok(run
            .iter()
            .map(|(id, location)| {
                (
                    *id,
                    StubPrimary {
                        id: *id,
                        block_ref: BlockRef {
                            number: location.block_num,
                            hash: bench_hash(location.block_num),
                            parent_hash: bench_hash(location.block_num.saturating_sub(1)),
                        },
                    },
                )
            })
            .collect())
    }

    async fn block_ref_for(&mut self, item: &Self::Item) -> Result<BlockRef> {
        Ok(item.block_ref)
    }

    fn exact_match(&self, _item: &Self::Item, _filter: &Self::Filter) -> bool {
        true
    }

    fn into_output(item: Self::Item) -> Self::Output {
        item
    }
}

pub fn build_service() -> BenchService {
    FinalizedHistoryService::new_reader_writer(
        Config {
            observe_upstream_finalized_block: Arc::new(static_observed_finalized_block),
            planner_max_or_terms: 256,
            ..Config::default()
        },
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        DEFAULT_WRITER_ID,
    )
}

pub fn build_counting_service() -> (CountingBenchService, Arc<BlobAccessCounters>) {
    let blob_store = CountingBlobStore::default();
    let counters = blob_store.counters();
    let svc = FinalizedHistoryService::new_reader_writer(
        Config {
            observe_upstream_finalized_block: Arc::new(static_observed_finalized_block),
            planner_max_or_terms: 256,
            bytes_cache: BytesCacheConfig {
                block_log_header: TableCacheConfig { max_bytes: 1 << 20 },
                log_dir_buckets: TableCacheConfig { max_bytes: 1 << 20 },
                log_dir_sub_buckets: TableCacheConfig { max_bytes: 1 << 20 },
                point_log_payloads: TableCacheConfig { max_bytes: 4 << 20 },
                ..BytesCacheConfig::disabled()
            },
            ..Config::default()
        },
        InMemoryMetaStore::default(),
        blob_store,
        DEFAULT_WRITER_ID,
    );
    (svc, counters)
}

pub fn build_service_with_stores(
    meta_store: InMemoryMetaStore,
    blob_store: InMemoryBlobStore,
) -> BenchService {
    FinalizedHistoryService::new_reader_writer(
        Config {
            observe_upstream_finalized_block: Arc::new(static_observed_finalized_block),
            planner_max_or_terms: 256,
            ..Config::default()
        },
        meta_store,
        blob_store,
        DEFAULT_WRITER_ID,
    )
}

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

pub fn mk_block(block_num: u64, parent_hash: [u8; 32], logs: Vec<Log>) -> FinalizedBlock {
    FinalizedBlock {
        block_num,
        block_hash: bench_hash(block_num),
        parent_hash,
        logs,
        txs: Vec::new(),
        trace_rlp: Vec::new(),
    }
}

pub fn seed_service_blocks(svc: &BenchService, blocks: u64, logs_per_block: u32) {
    block_on(async {
        let mut parent = [0u8; 32];
        for block_num in 1..=blocks {
            let mut logs = Vec::with_capacity(logs_per_block as usize);
            for log_idx in 0..logs_per_block {
                logs.push(mk_log(
                    (log_idx % 64) as u8,
                    (log_idx % 16) as u8,
                    (log_idx % 64) as u8,
                    block_num,
                    0,
                    log_idx,
                ));
            }
            let block = mk_block(block_num, parent, logs);
            parent = block.block_hash;
            svc.ingest_finalized_block(block)
                .await
                .expect("seed ingest");
        }
    });
}

pub fn query_page<A, M, B>(
    svc: &FinalizedHistoryService<A, M, B>,
    from_block: u64,
    to_block: u64,
    filter: LogFilter,
    limit: usize,
    resume_log_id: Option<u64>,
) -> QueryPage<Log>
where
    A: WriteAuthority,
    M: MetaStore,
    B: BlobStore,
{
    block_on(svc.query_logs(
        QueryLogsRequest {
            from_block: Some(from_block),
            to_block: Some(to_block),
            from_block_hash: None,
            to_block_hash: None,
            order: QueryOrder::Ascending,
            resume_log_id,
            limit,
            filter,
        },
        ExecutionBudget::default(),
    ))
    .expect("query")
}

pub fn query_len<A, M, B>(
    svc: &FinalizedHistoryService<A, M, B>,
    from_block: u64,
    to_block: u64,
    filter: LogFilter,
    limit: usize,
) -> usize
where
    A: WriteAuthority,
    M: MetaStore,
    B: BlobStore,
{
    query_page(svc, from_block, to_block, filter, limit, None)
        .items
        .len()
}

pub fn contiguous_block_filter() -> LogFilter {
    LogFilter {
        address: Some(Clause::One([90; 20])),
        topic0: None,
        topic1: None,
        topic2: None,
        topic3: None,
    }
}

pub fn non_contiguous_block_filter() -> LogFilter {
    LogFilter {
        address: Some(Clause::One([91; 20])),
        topic0: None,
        topic1: None,
        topic2: None,
        topic3: None,
    }
}

pub fn sparse_cross_block_filter() -> LogFilter {
    LogFilter {
        address: Some(Clause::One([92; 20])),
        topic0: None,
        topic1: None,
        topic2: None,
        topic3: None,
    }
}

pub fn mixed_page_filter() -> LogFilter {
    LogFilter {
        address: Some(Clause::One([93; 20])),
        topic0: None,
        topic1: None,
        topic2: None,
        topic3: None,
    }
}

pub fn seed_sparse_cross_block_fixture(svc: &CountingBenchService, blocks: u64) {
    block_on(async {
        let mut parent = [0u8; 32];
        for block_num in 1..=blocks {
            let block = mk_block(
                block_num,
                parent,
                vec![Log {
                    address: [92; 20],
                    topics: vec![[1; 32]],
                    data: vec![1],
                    block_num,
                    tx_idx: 0,
                    log_idx: 0,
                    block_hash: [block_num as u8; 32],
                }],
            );
            parent = block.block_hash;
            svc.ingest_finalized_block(block)
                .await
                .expect("seed ingest");
        }
    });
}

pub fn seed_contiguous_block_fixture(svc: &CountingBenchService, block_num: u64, matches: u32) {
    block_on(async {
        let logs = (0..matches)
            .map(|idx| Log {
                address: [90; 20],
                topics: vec![[2; 32]],
                data: vec![idx as u8],
                block_num,
                tx_idx: 0,
                log_idx: idx,
                block_hash: [block_num as u8; 32],
            })
            .collect();
        svc.ingest_finalized_block(mk_block(block_num, [0; 32], logs))
            .await
            .expect("seed ingest");
    });
}

pub fn seed_non_contiguous_block_fixture(
    svc: &CountingBenchService,
    block_num: u64,
    total_logs: u32,
) {
    block_on(async {
        let logs = (0..total_logs)
            .map(|idx| Log {
                address: if idx % 2 == 0 { [91; 20] } else { [0; 20] },
                topics: vec![[3; 32]],
                data: vec![idx as u8],
                block_num,
                tx_idx: 0,
                log_idx: idx,
                block_hash: [block_num as u8; 32],
            })
            .collect();
        svc.ingest_finalized_block(mk_block(block_num, [0; 32], logs))
            .await
            .expect("seed ingest");
    });
}

pub fn seed_mixed_page_fixture(svc: &CountingBenchService) {
    block_on(async {
        let mut parent = [0; 32];
        let first = mk_block(
            1,
            parent,
            (0..16)
                .map(|idx| Log {
                    address: [93; 20],
                    topics: vec![[4; 32]],
                    data: vec![idx as u8],
                    block_num: 1,
                    tx_idx: 0,
                    log_idx: idx,
                    block_hash: [1; 32],
                })
                .collect(),
        );
        parent = first.block_hash;
        svc.ingest_finalized_block(first)
            .await
            .expect("seed block 1");
        for block_num in 2..=9 {
            let block = mk_block(
                block_num,
                parent,
                vec![Log {
                    address: [93; 20],
                    topics: vec![[5; 32]],
                    data: vec![block_num as u8],
                    block_num,
                    tx_idx: 0,
                    log_idx: 0,
                    block_hash: [block_num as u8; 32],
                }],
            );
            parent = block.block_hash;
            svc.ingest_finalized_block(block)
                .await
                .expect("seed sparse block");
        }
    });
}

pub fn narrow_indexed_filter() -> LogFilter {
    LogFilter {
        address: Some(Clause::One([5; 20])),
        topic0: Some(Clause::One([5; 32])),
        topic1: Some(Clause::One([5; 32])),
        topic2: None,
        topic3: None,
    }
}

pub fn intersection_filter() -> LogFilter {
    LogFilter {
        address: Some(Clause::One([11; 20])),
        topic0: Some(Clause::One([11; 32])),
        topic1: None,
        topic2: None,
        topic3: None,
    }
}

pub fn wide_or_filter(width: usize) -> LogFilter {
    let addresses = (0..width).map(|i| [(i % 64) as u8; 20]).collect();
    LogFilter {
        address: Some(Clause::Or(addresses)),
        topic0: None,
        topic1: None,
        topic2: None,
        topic3: None,
    }
}

pub fn pagination_filter() -> LogFilter {
    LogFilter {
        address: Some(Clause::One([3; 20])),
        topic0: None,
        topic1: None,
        topic2: None,
        topic3: None,
    }
}

pub fn log_id(shard: u64, local: u32) -> LogId {
    compose_log_id(
        LogShard::new(shard).expect("valid log shard"),
        LogLocalId::new(local).expect("valid local id"),
    )
}

pub fn primary_range(
    start_shard: u64,
    start_local: u32,
    end_shard: u64,
    end_local: u32,
) -> QueryIdRange<LogId> {
    QueryIdRange::new(
        log_id(start_shard, start_local),
        log_id(end_shard, end_local),
    )
    .expect("valid primary id range")
}

pub fn dense_bitmap(start: u32, len: u32) -> RoaringBitmap {
    let mut bitmap = RoaringBitmap::new();
    bitmap.insert_range(start..start.saturating_add(len));
    bitmap
}

pub fn patterned_bitmap(start: u32, step: u32, count: u32) -> RoaringBitmap {
    let mut bitmap = RoaringBitmap::new();
    let mut value = start;
    for _ in 0..count {
        bitmap.insert(value);
        value = value.saturating_add(step);
    }
    bitmap
}

pub fn shard_bitmap_set(shards: &[u64], bitmap: &RoaringBitmap) -> ShardBitmapSet {
    shards
        .iter()
        .map(|&shard| (shard, bitmap.clone()))
        .collect()
}

pub fn low_shards(count: u64) -> Vec<u64> {
    (0..count).collect()
}

pub fn seed_materialized_blocks(
    meta_store: &InMemoryMetaStore,
    blob_store: &InMemoryBlobStore,
    blocks: &[SeededLogBlock],
) {
    let mut sorted_blocks = blocks.to_vec();
    sorted_blocks.sort_by_key(|block| block.block_num);

    block_on(async {
        let mut bucket_entries: BTreeMap<u64, Vec<(u64, u64, u64)>> = BTreeMap::new();

        for block in &sorted_blocks {
            let block_hash = bench_hash(block.block_num);
            let parent_hash = bench_hash(block.block_num.saturating_sub(1));
            let mut offsets = Vec::with_capacity(block.logs.len().saturating_add(1));
            let mut payload = Vec::new();
            offsets.push(0);

            for log in &block.logs {
                assert!(validate_log(log), "synthetic log must be valid");
                let encoded = log.encode();
                payload.extend_from_slice(encoded.as_ref());
                offsets.push(u32::try_from(payload.len()).expect("payload length fits in u32"));
            }

            put_meta_record(
                meta_store,
                BLOCK_RECORD_TABLE,
                &BlockRecordSpec::key(block.block_num),
                BlockRecord {
                    block_hash,
                    parent_hash,
                    logs: Some(PrimaryWindowRecord {
                        first_primary_id: block.first_log_id,
                        count: u32::try_from(block.logs.len())
                            .expect("block log count fits in u32"),
                    }),
                    traces: None,
                }
                .encode(),
            )
            .await;

            if !block.logs.is_empty() {
                put_meta_record(
                    meta_store,
                    BlockLogHeaderSpec::TABLE,
                    &BlockLogHeaderSpec::key(block.block_num),
                    BlockLogHeader { offsets }.encode(),
                )
                .await;
                blob_store
                    .put_blob(
                        BlockLogBlobSpec::TABLE,
                        &BlockLogBlobSpec::key(block.block_num),
                        Bytes::from(payload),
                    )
                    .await
                    .expect("put block blob");
            }

            let count = block.logs.len() as u64;
            let end_exclusive = block.first_log_id.saturating_add(count);
            let last_covered_id = end_exclusive.saturating_sub(1);
            let mut bucket_start = LogDirBucketSpec::bucket_start(LogId::new(block.first_log_id));
            let last_bucket_start = LogDirBucketSpec::bucket_start(LogId::new(last_covered_id));
            loop {
                bucket_entries.entry(bucket_start).or_default().push((
                    block.block_num,
                    block.first_log_id,
                    end_exclusive,
                ));
                if bucket_start == last_bucket_start {
                    break;
                }
                bucket_start = bucket_start.saturating_add(LOG_DIRECTORY_BUCKET_SIZE);
            }
        }

        for (bucket_start, entries) in bucket_entries {
            let start_block = entries
                .first()
                .map(|(block_num, _, _)| *block_num)
                .expect("bucket must contain at least one block");
            let mut first_primary_ids = entries
                .iter()
                .map(|(_, first_log_id, _)| *first_log_id)
                .collect::<Vec<_>>();
            first_primary_ids.push(
                entries
                    .last()
                    .map(|(_, _, end_exclusive)| *end_exclusive)
                    .expect("bucket must contain final sentinel"),
            );
            put_meta_record(
                meta_store,
                LogDirBucketSpec::TABLE,
                &LogDirBucketSpec::key(bucket_start),
                DirBucket {
                    start_block,
                    first_primary_ids,
                }
                .encode(),
            )
            .await;
        }
    });
}

pub fn seed_publication_state(
    meta_store: &InMemoryMetaStore,
    indexed_finalized_head: u64,
    _next_log_id: u64,
) {
    block_on(async {
        put_meta_record(
            meta_store,
            PUBLICATION_STATE_TABLE,
            PUBLICATION_STATE_SUFFIX,
            PublicationState {
                owner_id: DEFAULT_WRITER_ID,
                session_id: [0u8; 16],
                indexed_finalized_head,
                lease_valid_through_block: u64::MAX,
            }
            .encode(),
        )
        .await;
    });
}

pub fn stream_for_address(address: [u8; 20], shard: u64) -> String {
    finalized_history_query::kernel::sharded_streams::sharded_stream_id("addr", &address, shard)
}

pub fn materializer<'a>(
    meta_store: &'a InMemoryMetaStore,
    blob_store: &'a InMemoryBlobStore,
) -> LogMaterializer<'a, InMemoryMetaStore, InMemoryBlobStore> {
    let tables = Box::leak(Box::new(Tables::without_cache(
        meta_store.clone(),
        blob_store.clone(),
    )));
    LogMaterializer::new(tables)
}

async fn put_meta_record(
    meta_store: &InMemoryMetaStore,
    family: TableId,
    key: &[u8],
    value: Bytes,
) {
    meta_store
        .put(family, key, value, PutCond::Any)
        .await
        .expect("put meta record");
}

pub fn spanning_bucket_blocks() -> Vec<SeededLogBlock> {
    let first_log_id = LOG_DIRECTORY_BUCKET_SIZE - 3;
    vec![
        SeededLogBlock {
            block_num: 700,
            first_log_id,
            logs: (0..10).map(|idx| mk_log(9, 9, 9, 700, 0, idx)).collect(),
        },
        SeededLogBlock {
            block_num: 701,
            first_log_id: first_log_id + 10,
            logs: (0..4).map(|idx| mk_log(10, 10, 10, 701, 0, idx)).collect(),
        },
    ]
}

pub fn high_shard_blocks() -> Vec<SeededLogBlock> {
    let first_log_id = compose_log_id(
        LogShard::new(HIGH_SHARD).expect("valid high shard"),
        LogLocalId::new(MAX_LOCAL_ID - 32).expect("valid high local"),
    )
    .get();
    vec![SeededLogBlock {
        block_num: 9_001,
        first_log_id,
        logs: (0..16).map(|idx| mk_log(42, 7, 9, 9_001, 0, idx)).collect(),
    }]
}
