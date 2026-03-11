use std::collections::BTreeMap;

use futures::stream::{FuturesUnordered, StreamExt};
use roaring::RoaringBitmap;

use crate::api::query_logs::{ExecutionBudget, QueryLogsRequest};
use crate::codec::log::decode_stream_bitmap_meta;
use crate::config::Config;
use crate::core::execution::{MatchedPrimary, PrimaryMaterializer, ShardBitmapSet};
use crate::core::ids::{LogId, LogLocalId, LogShard, compose_log_id};
use crate::core::page::{QueryPage, QueryPageMeta};
use crate::core::range::{RangeResolver, ResolvedBlockRange};
use crate::domain::keys::{
    STREAM_PAGE_LOCAL_ID_SPAN, local_range_for_shard, log_shard, stream_fragment_blob_key,
    stream_fragment_meta_prefix, stream_id, stream_page_blob_key, stream_page_meta_key,
    stream_page_start_local,
};
use crate::error::{Error, Result};
use crate::logs::filter::LogFilter;
use crate::logs::index_spec::{
    ClauseKind, clause_values_20, clause_values_32, is_full_shard_range, is_too_broad,
};
use crate::logs::materialize::LogMaterializer;
use crate::logs::types::Log;
use crate::logs::window::LogWindowResolver;
use crate::store::traits::{BlobStore, MetaStore};
use crate::streams::chunk::decode_chunk;

const STREAM_LOAD_CONCURRENCY: usize = 32;

#[derive(Debug, Clone)]
struct IndexedClauseSpec {
    kind: ClauseKind,
    stream_kind: &'static str,
    values: Vec<Vec<u8>>,
}

#[derive(Debug, Clone)]
struct PreparedShardClause {
    kind: ClauseKind,
    stream_ids: Vec<String>,
    estimated_count: u64,
}

#[derive(Debug, Clone)]
pub struct LogsQueryEngine {
    max_or_terms: usize,
    range_resolver: RangeResolver,
    window_resolver: LogWindowResolver,
}

impl LogsQueryEngine {
    pub fn from_config(config: &Config) -> Self {
        Self {
            max_or_terms: config.planner_max_or_terms,
            range_resolver: RangeResolver,
            window_resolver: LogWindowResolver,
        }
    }

    pub async fn query_logs<M: MetaStore, B: BlobStore>(
        &self,
        meta_store: &M,
        blob_store: &B,
        request: QueryLogsRequest,
        budget: ExecutionBudget,
    ) -> Result<QueryPage<Log>> {
        if request.limit == 0 {
            return Err(Error::InvalidParams("limit must be at least 1"));
        }
        if !request.filter.has_indexed_clause() {
            return Err(Error::InvalidParams(
                "query must include at least one indexed address or topic clause",
            ));
        }

        let effective_limit = match budget.max_results {
            Some(0) => {
                return Err(Error::InvalidParams(
                    "budget.max_results must be at least 1 when set",
                ));
            }
            Some(max_results) => request.limit.min(max_results),
            None => request.limit,
        };

        let block_range = self
            .range_resolver
            .resolve(
                meta_store,
                request.from_block,
                request.to_block,
                request.order,
            )
            .await?;
        if block_range.is_empty() {
            return Ok(self.empty_page(&block_range));
        }

        let Some(mut log_window) = self
            .window_resolver
            .resolve(meta_store, &block_range)
            .await?
        else {
            return Ok(self.empty_page(&block_range));
        };

        if let Some(resume_log_id) = request.resume_log_id.map(LogId::new) {
            if !log_window.contains(resume_log_id) {
                return Err(Error::InvalidParams(
                    "resume_log_id outside resolved block window",
                ));
            }

            let Some(resumed_window) = log_window.resume_strictly_after(resume_log_id) else {
                return Ok(self.empty_page(&block_range));
            };
            log_window = resumed_window;
        }

        if is_too_broad(&request.filter, self.max_or_terms) {
            let actual = request.filter.max_or_terms();
            return Err(Error::QueryTooBroad {
                actual,
                max: self.max_or_terms,
            });
        }

        let take = effective_limit.saturating_add(1);
        let matched = self
            .execute_indexed_query(
                meta_store,
                blob_store,
                &request.filter,
                log_window.start,
                log_window.end_inclusive,
                take,
            )
            .await?;

        Ok(self.build_page(block_range, effective_limit, matched))
    }

    fn empty_page(&self, block_range: &ResolvedBlockRange) -> QueryPage<Log> {
        QueryPage {
            items: Vec::new(),
            meta: QueryPageMeta {
                resolved_from_block: block_range.resolved_from_ref,
                resolved_to_block: block_range.resolved_to_ref,
                cursor_block: block_range.examined_endpoint_ref,
                has_more: false,
                next_resume_log_id: None,
            },
        }
    }

    fn build_page(
        &self,
        block_range: ResolvedBlockRange,
        effective_limit: usize,
        mut matched: Vec<MatchedPrimary<Log>>,
    ) -> QueryPage<Log> {
        let has_more = matched.len() > effective_limit;
        if has_more {
            matched.truncate(effective_limit);
        }

        let next_resume_log_id = if has_more {
            matched.last().map(|matched_log| matched_log.id.get())
        } else {
            None
        };
        let cursor_block = matched
            .last()
            .map(|matched_log| matched_log.block_ref)
            .unwrap_or(block_range.examined_endpoint_ref);
        let items = matched
            .into_iter()
            .map(|matched_log| matched_log.item)
            .collect();

        QueryPage {
            items,
            meta: QueryPageMeta {
                resolved_from_block: block_range.resolved_from_ref,
                resolved_to_block: block_range.resolved_to_ref,
                cursor_block,
                has_more,
                next_resume_log_id,
            },
        }
    }

    async fn execute_indexed_query<M: MetaStore, B: BlobStore>(
        &self,
        meta_store: &M,
        blob_store: &B,
        filter: &LogFilter,
        from_log_id: LogId,
        to_log_id_inclusive: LogId,
        take: usize,
    ) -> Result<Vec<MatchedPrimary<Log>>> {
        let clause_specs = build_clause_specs(filter);
        let mut matched = Vec::new();
        let mut materializer = LogMaterializer::new(meta_store, blob_store);

        for shard_raw in from_log_id.shard().get()..=to_log_id_inclusive.shard().get() {
            let shard = LogShard::new(shard_raw).expect("shard derived from LogId range");
            let (local_from, local_to) =
                local_range_for_shard(from_log_id, to_log_id_inclusive, shard);
            let shard_clauses =
                prepare_shard_clauses(meta_store, &clause_specs, shard, local_from, local_to)
                    .await?;

            if shard_clauses.is_empty() {
                continue;
            }

            let mut shard_accumulator: Option<RoaringBitmap> = None;
            for prepared_clause in shard_clauses {
                let clause_bitmap = load_prepared_clause_bitmap(
                    meta_store,
                    blob_store,
                    &prepared_clause,
                    local_from.get(),
                    local_to.get(),
                )
                .await?;
                if clause_bitmap.is_empty() {
                    shard_accumulator = Some(RoaringBitmap::new());
                    break;
                }

                match shard_accumulator.as_mut() {
                    Some(accumulator) => {
                        *accumulator &= &clause_bitmap;
                        if accumulator.is_empty() {
                            break;
                        }
                    }
                    None => shard_accumulator = Some(clause_bitmap),
                }
            }

            let Some(shard_accumulator) = shard_accumulator else {
                continue;
            };
            if shard_accumulator.is_empty() {
                continue;
            }

            for local_raw in shard_accumulator {
                let local = LogLocalId::new(local_raw).expect("bitmap values must fit local-id");
                let id = compose_log_id(shard, local);
                let Some(item) = materializer.load_by_id(id).await? else {
                    continue;
                };
                if !materializer.exact_match(&item, filter) {
                    continue;
                }

                let block_ref = materializer.block_ref_for(&item).await?;
                matched.push(MatchedPrimary {
                    id,
                    item,
                    block_ref,
                });
                if matched.len() >= take {
                    return Ok(matched);
                }
            }
        }

        Ok(matched)
    }
}

async fn load_clause_sets<M: MetaStore, B: BlobStore>(
    meta_store: &M,
    blob_store: &B,
    clause_specs: &[IndexedClauseSpec],
    from_log_id: LogId,
    to_log_id_inclusive: LogId,
) -> Result<Vec<ShardBitmapSet>> {
    let mut clause_sets = Vec::new();

    for clause_spec in clause_specs {
        let set = fetch_union_log_level(
            meta_store,
            blob_store,
            clause_spec.stream_kind,
            &clause_spec.values,
            from_log_id,
            to_log_id_inclusive,
        )
        .await?;
        clause_sets.push(set);
    }

    Ok(clause_sets)
}

#[doc(hidden)]
pub async fn load_clause_sets_for_benchmark<M: MetaStore, B: BlobStore>(
    meta_store: &M,
    blob_store: &B,
    filter: &LogFilter,
    from_log_id: LogId,
    to_log_id_inclusive: LogId,
) -> Result<Vec<ShardBitmapSet>> {
    load_clause_sets(
        meta_store,
        blob_store,
        &build_clause_specs(filter),
        from_log_id,
        to_log_id_inclusive,
    )
    .await
}

fn build_clause_specs(filter: &LogFilter) -> Vec<IndexedClauseSpec> {
    let mut clauses = Vec::new();

    if let Some(clause) = &filter.address {
        let values = clause_values_20(clause);
        if !values.is_empty() {
            clauses.push(IndexedClauseSpec {
                kind: ClauseKind::Address,
                stream_kind: "addr",
                values,
            });
        }
    }

    if let Some(clause) = &filter.topic1 {
        let values = clause_values_32(clause);
        if !values.is_empty() {
            clauses.push(IndexedClauseSpec {
                kind: ClauseKind::Topic1,
                stream_kind: "topic1",
                values,
            });
        }
    }

    if let Some(clause) = &filter.topic2 {
        let values = clause_values_32(clause);
        if !values.is_empty() {
            clauses.push(IndexedClauseSpec {
                kind: ClauseKind::Topic2,
                stream_kind: "topic2",
                values,
            });
        }
    }

    if let Some(clause) = &filter.topic3 {
        let values = clause_values_32(clause);
        if !values.is_empty() {
            clauses.push(IndexedClauseSpec {
                kind: ClauseKind::Topic3,
                stream_kind: "topic3",
                values,
            });
        }
    }

    if let Some(clause) = &filter.topic0 {
        let values = clause_values_32(clause);
        if !values.is_empty() {
            clauses.push(IndexedClauseSpec {
                kind: ClauseKind::Topic0,
                stream_kind: "topic0",
                values,
            });
        }
    }

    clauses
}

async fn prepare_shard_clauses<M: MetaStore>(
    meta_store: &M,
    clause_specs: &[IndexedClauseSpec],
    shard: LogShard,
    local_from: LogLocalId,
    local_to: LogLocalId,
) -> Result<Vec<PreparedShardClause>> {
    let mut prepared = Vec::with_capacity(clause_specs.len());

    for clause_spec in clause_specs {
        let clause =
            prepare_shard_clause(meta_store, clause_spec, shard, local_from, local_to).await?;
        prepared.push(clause);
    }

    prepared.sort_by_key(|clause| (clause.estimated_count, clause_kind_rank(clause.kind)));
    Ok(prepared)
}

async fn prepare_shard_clause<M: MetaStore>(
    meta_store: &M,
    clause_spec: &IndexedClauseSpec,
    shard: LogShard,
    local_from: LogLocalId,
    local_to: LogLocalId,
) -> Result<PreparedShardClause> {
    let mut stream_ids = Vec::with_capacity(clause_spec.values.len());
    let mut estimated_count = 0u64;

    for value in &clause_spec.values {
        let stream = stream_id(clause_spec.stream_kind, value, shard);
        estimated_count = estimated_count.saturating_add(
            estimate_stream_overlap(meta_store, &stream, local_from.get(), local_to.get()).await?,
        );
        stream_ids.push(stream);
    }

    Ok(PreparedShardClause {
        kind: clause_spec.kind,
        stream_ids,
        estimated_count,
    })
}

async fn estimate_stream_overlap<M: MetaStore>(
    meta_store: &M,
    stream_id: &str,
    local_from: u32,
    local_to: u32,
) -> Result<u64> {
    let mut estimated = 0u64;
    let mut page_start = stream_page_start_local(local_from);
    let last_page_start = stream_page_start_local(local_to);

    loop {
        if let Some(record) = meta_store
            .get(&stream_page_meta_key(stream_id, page_start))
            .await?
        {
            let meta = decode_stream_bitmap_meta(&record.value)?;
            if overlaps(meta.min_local, meta.max_local, local_from, local_to) {
                estimated = estimated.saturating_add(u64::from(meta.count));
            }
        } else {
            let page = meta_store
                .list_prefix(
                    &stream_fragment_meta_prefix(stream_id, page_start),
                    None,
                    usize::MAX,
                )
                .await?;
            for key in page.keys {
                let Some(record) = meta_store.get(&key).await? else {
                    continue;
                };
                let meta = decode_stream_bitmap_meta(&record.value)?;
                if overlaps(meta.min_local, meta.max_local, local_from, local_to) {
                    estimated = estimated.saturating_add(u64::from(meta.count));
                }
            }
        }

        if page_start == last_page_start {
            break;
        }
        page_start = page_start.saturating_add(STREAM_PAGE_LOCAL_ID_SPAN);
    }

    Ok(estimated)
}

async fn load_prepared_clause_bitmap<M: MetaStore, B: BlobStore>(
    meta_store: &M,
    blob_store: &B,
    prepared_clause: &PreparedShardClause,
    local_from: u32,
    local_to: u32,
) -> Result<RoaringBitmap> {
    let mut out = RoaringBitmap::new();
    let mut in_flight = FuturesUnordered::new();

    for stream_id in &prepared_clause.stream_ids {
        in_flight.push(async move {
            load_stream_entries(meta_store, blob_store, stream_id, local_from, local_to).await
        });
        if in_flight.len() >= STREAM_LOAD_CONCURRENCY
            && let Some(result) = in_flight.next().await
        {
            out |= &result?;
        }
    }

    while let Some(result) = in_flight.next().await {
        out |= &result?;
    }

    Ok(out)
}

fn clause_kind_rank(kind: ClauseKind) -> u8 {
    match kind {
        ClauseKind::Address => 0,
        ClauseKind::Topic1 => 1,
        ClauseKind::Topic2 => 2,
        ClauseKind::Topic3 => 3,
        ClauseKind::Topic0 => 4,
    }
}

async fn fetch_union_log_level<M: MetaStore, B: BlobStore>(
    meta_store: &M,
    blob_store: &B,
    kind: &str,
    values: &[Vec<u8>],
    from_log_id: LogId,
    to_log_id_inclusive: LogId,
) -> Result<ShardBitmapSet> {
    let mut out = BTreeMap::new();
    let from_shard = log_shard(from_log_id);
    let to_shard = log_shard(to_log_id_inclusive);
    let mut in_flight = FuturesUnordered::new();

    for value in values {
        for shard_raw in from_shard.get()..=to_shard.get() {
            let shard = LogShard::new(shard_raw).expect("shard derived from LogId range");
            let stream = stream_id(kind, value, shard);
            let (local_from, local_to) =
                local_range_for_shard(from_log_id, to_log_id_inclusive, shard);
            in_flight.push(async move {
                let entries = load_stream_entries(
                    meta_store,
                    blob_store,
                    &stream,
                    local_from.get(),
                    local_to.get(),
                )
                .await?;
                Ok::<(LogShard, RoaringBitmap), Error>((shard, entries))
            });
            if in_flight.len() >= STREAM_LOAD_CONCURRENCY
                && let Some(result) = in_flight.next().await
            {
                merge_union_entries(&mut out, result?);
            }
        }
    }

    while let Some(result) = in_flight.next().await {
        merge_union_entries(&mut out, result?);
    }

    Ok(out)
}

fn merge_union_entries(out: &mut ShardBitmapSet, (shard, entries): (LogShard, RoaringBitmap)) {
    if entries.is_empty() {
        return;
    }
    out.entry(shard)
        .and_modify(|existing| *existing |= &entries)
        .or_insert(entries);
}

async fn load_stream_entries<M: MetaStore, B: BlobStore>(
    meta_store: &M,
    blob_store: &B,
    stream: &str,
    local_from: u32,
    local_to: u32,
) -> Result<RoaringBitmap> {
    let mut out = RoaringBitmap::new();
    let mut page_start = stream_page_start_local(local_from);
    let last_page_start = stream_page_start_local(local_to);

    loop {
        if let Some(record) = meta_store
            .get(&stream_page_meta_key(stream, page_start))
            .await?
        {
            let meta = decode_stream_bitmap_meta(&record.value)?;
            if overlaps(meta.min_local, meta.max_local, local_from, local_to) {
                maybe_merge_bitmap_blob(
                    blob_store,
                    &stream_page_blob_key(stream, page_start),
                    &mut out,
                    local_from,
                    local_to,
                )
                .await?;
            }
        } else {
            let page = meta_store
                .list_prefix(
                    &stream_fragment_meta_prefix(stream, page_start),
                    None,
                    usize::MAX,
                )
                .await?;
            for key in page.keys {
                let Some(record) = meta_store.get(&key).await? else {
                    continue;
                };
                let meta = decode_stream_bitmap_meta(&record.value)?;
                if !overlaps(meta.min_local, meta.max_local, local_from, local_to) {
                    continue;
                }
                let block_num = read_u64_suffix(&key)?;
                maybe_merge_bitmap_blob(
                    blob_store,
                    &stream_fragment_blob_key(stream, page_start, block_num),
                    &mut out,
                    local_from,
                    local_to,
                )
                .await?;
            }
        }

        if page_start == last_page_start {
            break;
        }
        page_start = page_start.saturating_add(STREAM_PAGE_LOCAL_ID_SPAN);
    }

    Ok(out)
}

async fn maybe_merge_bitmap_blob<B: BlobStore>(
    blob_store: &B,
    key: &[u8],
    out: &mut RoaringBitmap,
    local_from: u32,
    local_to: u32,
) -> Result<()> {
    let Some(bytes) = blob_store.get_blob(key).await? else {
        return Ok(());
    };
    let chunk = decode_chunk(&bytes)?;
    if is_full_shard_range(local_from, local_to)
        || (chunk.min_local >= local_from && chunk.max_local <= local_to)
    {
        *out |= &chunk.bitmap;
        return Ok(());
    }

    for value in chunk.bitmap {
        if value >= local_from && value <= local_to {
            out.insert(value);
        }
    }
    Ok(())
}

fn overlaps(min_local: u32, max_local: u32, local_from: u32, local_to: u32) -> bool {
    min_local <= local_to && max_local >= local_from
}

fn read_u64_suffix(key: &[u8]) -> Result<u64> {
    if key.len() < 8 {
        return Err(Error::Decode("short key suffix"));
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&key[key.len() - 8..]);
    Ok(u64::from_be_bytes(bytes))
}
