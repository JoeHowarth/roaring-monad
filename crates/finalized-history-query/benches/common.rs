#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use finalized_history_query::api::{
    ExecutionBudget, FinalizedHistoryService, QueryLogsRequest, QueryOrder,
};
use finalized_history_query::cache::NoopBytesCache;
use finalized_history_query::codec::finalized_state::{
    encode_block_meta, encode_publication_state,
};
use finalized_history_query::codec::log::validate_log;
use finalized_history_query::codec::log::{
    encode_block_log_header, encode_log, encode_log_directory_bucket,
};
use finalized_history_query::config::Config;
use finalized_history_query::core::execution::{PrimaryMaterializer, ShardBitmapSet};
use finalized_history_query::core::ids::{
    LogId, LogLocalId, LogShard, PrimaryIdRange, compose_log_id,
};
use finalized_history_query::core::refs::BlockRef;
use finalized_history_query::domain::keys::{
    MAX_LOCAL_ID, PUBLICATION_STATE_KEY, block_log_header_key, block_logs_blob_key, block_meta_key,
    log_directory_bucket_key, log_directory_bucket_start, stream_id,
};
use finalized_history_query::domain::types::{
    Block, BlockMeta, Log, LogDirectoryBucket, PublicationState,
};
use finalized_history_query::logs::materialize::LogMaterializer;
use finalized_history_query::store::blob::InMemoryBlobStore;
use finalized_history_query::store::meta::InMemoryMetaStore;
use finalized_history_query::store::traits::{BlobStore, FenceToken, MetaStore, PutCond};
use finalized_history_query::{Clause, LeaseAuthority, LogFilter, QueryPage, Result};
use futures::executor::block_on;
use roaring::RoaringBitmap;

pub const HIGH_SHARD: u64 = 0x1_0000_0000;
pub const DEFAULT_WRITER_EPOCH: u64 = 1;
static NOOP_BYTES_CACHE: NoopBytesCache = NoopBytesCache;

fn static_observed_finalized_block() -> Option<u64> {
    Some(u64::MAX / 4)
}

pub type BenchService = FinalizedHistoryService<
    LeaseAuthority<InMemoryMetaStore>,
    InMemoryMetaStore,
    InMemoryBlobStore,
>;

#[derive(Debug, Clone)]
pub struct SeededLogBlock {
    pub block_num: u64,
    pub first_log_id: u64,
    pub logs: Vec<Log>,
}

#[derive(Debug, Clone)]
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

impl PrimaryMaterializer for PassThroughMaterializer {
    type Primary = StubPrimary;
    type Filter = ();

    async fn load_by_id(&mut self, id: LogId) -> Result<Option<Self::Primary>> {
        let block_num = (id.get() / self.block_span).saturating_add(1);
        Ok(Some(StubPrimary {
            id,
            block_ref: BlockRef {
                number: block_num,
                hash: [block_num as u8; 32],
                parent_hash: [block_num.saturating_sub(1) as u8; 32],
            },
        }))
    }

    async fn block_ref_for(&mut self, item: &Self::Primary) -> Result<BlockRef> {
        Ok(item.block_ref)
    }

    fn exact_match(&self, _item: &Self::Primary, _filter: &Self::Filter) -> bool {
        true
    }
}

pub fn build_service(target_entries_per_chunk: u32) -> BenchService {
    FinalizedHistoryService::new_reader_writer(
        Config {
            observe_upstream_finalized_block: Arc::new(static_observed_finalized_block),
            target_entries_per_chunk,
            planner_max_or_terms: 256,
            ..Config::default()
        },
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        DEFAULT_WRITER_EPOCH,
    )
}

pub fn build_service_with_stores(
    target_entries_per_chunk: u32,
    meta_store: InMemoryMetaStore,
    blob_store: InMemoryBlobStore,
) -> BenchService {
    FinalizedHistoryService::new_reader_writer(
        Config {
            observe_upstream_finalized_block: Arc::new(static_observed_finalized_block),
            target_entries_per_chunk,
            planner_max_or_terms: 256,
            ..Config::default()
        },
        meta_store,
        blob_store,
        DEFAULT_WRITER_EPOCH,
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

pub fn mk_block(block_num: u64, parent_hash: [u8; 32], logs: Vec<Log>) -> Block {
    Block {
        block_num,
        block_hash: [block_num as u8; 32],
        parent_hash,
        logs,
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

pub fn query_page(
    svc: &BenchService,
    from_block: u64,
    to_block: u64,
    filter: LogFilter,
    limit: usize,
    resume_log_id: Option<u64>,
) -> QueryPage<Log> {
    block_on(svc.query_logs(
        QueryLogsRequest {
            from_block,
            to_block,
            order: QueryOrder::Ascending,
            resume_log_id,
            limit,
            filter,
        },
        ExecutionBudget::default(),
    ))
    .expect("query")
}

pub fn query_len(
    svc: &BenchService,
    from_block: u64,
    to_block: u64,
    filter: LogFilter,
    limit: usize,
) -> usize {
    query_page(svc, from_block, to_block, filter, limit, None)
        .items
        .len()
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
) -> PrimaryIdRange {
    PrimaryIdRange::new(
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
        .map(|&shard| {
            (
                LogShard::new(shard).expect("valid log shard"),
                bitmap.clone(),
            )
        })
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
            let block_hash = [block.block_num as u8; 32];
            let parent_hash = [block.block_num.saturating_sub(1) as u8; 32];
            let mut offsets = Vec::with_capacity(block.logs.len().saturating_add(1));
            let mut payload = Vec::new();
            offsets.push(0);

            for log in &block.logs {
                assert!(validate_log(log), "synthetic log must be valid");
                let encoded = encode_log(log);
                payload.extend_from_slice(encoded.as_ref());
                offsets.push(u32::try_from(payload.len()).expect("payload length fits in u32"));
            }

            put_meta_record(
                meta_store,
                &block_meta_key(block.block_num),
                encode_block_meta(&BlockMeta {
                    block_hash,
                    parent_hash,
                    first_log_id: block.first_log_id,
                    count: u32::try_from(block.logs.len()).expect("block log count fits in u32"),
                }),
            )
            .await;

            if !block.logs.is_empty() {
                put_meta_record(
                    meta_store,
                    &block_log_header_key(block.block_num),
                    encode_block_log_header(
                        &finalized_history_query::domain::types::BlockLogHeader { offsets },
                    ),
                )
                .await;
                blob_store
                    .put_blob(&block_logs_blob_key(block.block_num), Bytes::from(payload))
                    .await
                    .expect("put block blob");
            }

            let count = block.logs.len() as u64;
            let end_exclusive = block.first_log_id.saturating_add(count);
            let last_covered_id = end_exclusive.saturating_sub(1);
            let mut bucket_start = log_directory_bucket_start(LogId::new(block.first_log_id));
            let last_bucket_start = log_directory_bucket_start(LogId::new(last_covered_id));
            loop {
                bucket_entries.entry(bucket_start).or_default().push((
                    block.block_num,
                    block.first_log_id,
                    end_exclusive,
                ));
                if bucket_start == last_bucket_start {
                    break;
                }
                bucket_start = bucket_start.saturating_add(
                    finalized_history_query::domain::keys::LOG_DIRECTORY_BUCKET_SIZE,
                );
            }
        }

        for (bucket_start, entries) in bucket_entries {
            let start_block = entries
                .first()
                .map(|(block_num, _, _)| *block_num)
                .expect("bucket must contain at least one block");
            let mut first_log_ids = entries
                .iter()
                .map(|(_, first_log_id, _)| *first_log_id)
                .collect::<Vec<_>>();
            first_log_ids.push(
                entries
                    .last()
                    .map(|(_, _, end_exclusive)| *end_exclusive)
                    .expect("bucket must contain final sentinel"),
            );
            put_meta_record(
                meta_store,
                &log_directory_bucket_key(bucket_start),
                encode_log_directory_bucket(&LogDirectoryBucket {
                    start_block,
                    first_log_ids,
                }),
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
            PUBLICATION_STATE_KEY,
            encode_publication_state(&PublicationState {
                owner_id: DEFAULT_WRITER_EPOCH,
                session_id: [0u8; 16],
                epoch: DEFAULT_WRITER_EPOCH,
                indexed_finalized_head,
                lease_valid_through_block: u64::MAX,
            }),
        )
        .await;
    });
}

pub fn stream_for_address(address: [u8; 20], shard: u64) -> String {
    stream_id(
        "addr",
        &address,
        LogShard::new(shard).expect("valid log shard"),
    )
}

pub fn materializer<'a>(
    meta_store: &'a InMemoryMetaStore,
    blob_store: &'a InMemoryBlobStore,
) -> LogMaterializer<'a, InMemoryMetaStore, InMemoryBlobStore, NoopBytesCache> {
    LogMaterializer::new(meta_store, blob_store, &NOOP_BYTES_CACHE)
}

async fn put_meta_record(meta_store: &InMemoryMetaStore, key: &[u8], value: Bytes) {
    meta_store
        .put(key, value, PutCond::Any, FenceToken(DEFAULT_WRITER_EPOCH))
        .await
        .expect("put meta record");
}

pub fn spanning_bucket_blocks() -> Vec<SeededLogBlock> {
    let first_log_id = finalized_history_query::domain::keys::LOG_DIRECTORY_BUCKET_SIZE - 3;
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
