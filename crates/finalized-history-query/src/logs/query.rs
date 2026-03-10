use std::collections::BTreeMap;

use futures::stream::{FuturesUnordered, StreamExt};
use roaring::RoaringBitmap;

use crate::api::query_logs::{ExecutionBudget, QueryLogsRequest};
use crate::config::{BroadQueryPolicy, Config};
use crate::core::execution::{MatchedPrimary, PrimaryMaterializer, ShardBitmapSet};
use crate::core::ids::{LogId, LogLocalId, LogShard, compose_log_id};
use crate::core::page::{QueryPage, QueryPageMeta};
use crate::core::range::{RangeResolver, ResolvedBlockRange};
use crate::domain::keys::{
    chunk_blob_key, local_range_for_shard, log_shard, manifest_key, stream_id, tail_key,
};
use crate::error::{Error, Result};
use crate::logs::block_scan::LogBlockScanner;
use crate::logs::filter::LogFilter;
use crate::logs::index_spec::{
    ClauseKind, clause_values_20, clause_values_32, is_full_shard_range, overlapping_chunk_refs,
    should_error_too_broad, should_force_block_scan,
};
use crate::logs::materialize::LogMaterializer;
use crate::logs::types::Log;
use crate::logs::window::LogWindowResolver;
use crate::store::traits::{BlobStore, MetaStore};
use crate::streams::chunk::decode_chunk;
use crate::streams::manifest::{ChunkRef, Manifest, decode_manifest, decode_tail};

const STREAM_LOAD_CONCURRENCY: usize = 32;

#[derive(Debug, Clone)]
struct IndexedClauseSpec {
    kind: ClauseKind,
    stream_kind: &'static str,
    values: Vec<Vec<u8>>,
}

#[derive(Debug, Clone)]
struct PreparedShardStream {
    stream_id: String,
    manifest: Option<Manifest>,
    overlapping_chunk_refs: Vec<ChunkRef>,
    sealed_estimate: u64,
}

#[derive(Debug, Clone)]
struct PreparedShardClause {
    kind: ClauseKind,
    prepared_streams: Vec<PreparedShardStream>,
    sealed_estimate: u64,
}

#[derive(Debug, Clone)]
pub struct LogsQueryEngine {
    max_or_terms: usize,
    broad_query_policy: BroadQueryPolicy,
    range_resolver: RangeResolver,
    window_resolver: LogWindowResolver,
    block_scanner: LogBlockScanner,
}

impl LogsQueryEngine {
    pub fn from_config(config: &Config) -> Self {
        Self {
            max_or_terms: config.planner_max_or_terms,
            broad_query_policy: config.planner_broad_query_policy,
            range_resolver: RangeResolver,
            window_resolver: LogWindowResolver,
            block_scanner: LogBlockScanner,
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

        if should_error_too_broad(&request.filter, self.max_or_terms, self.broad_query_policy) {
            let actual = request.filter.max_or_terms();
            return Err(Error::QueryTooBroad {
                actual,
                max: self.max_or_terms,
            });
        }

        let take = effective_limit.saturating_add(1);
        let clause_specs = build_clause_specs(&request.filter);
        let matched =
            if should_force_block_scan(&request.filter, self.max_or_terms, self.broad_query_policy)
            {
                self.block_scanner
                    .execute(
                        meta_store,
                        blob_store,
                        &block_range,
                        log_window,
                        &request.filter,
                        take,
                    )
                    .await?
            } else if clause_specs.is_empty() {
                self.execute_clause_free_query(
                    meta_store,
                    blob_store,
                    log_window.start,
                    log_window.end_inclusive,
                    &request.filter,
                    take,
                )
                .await?
            } else {
                self.execute_indexed_query(
                    meta_store,
                    blob_store,
                    &request.filter,
                    log_window.start,
                    log_window.end_inclusive,
                    take,
                )
                .await?
            };

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

    async fn execute_clause_free_query<M: MetaStore, B: BlobStore>(
        &self,
        meta_store: &M,
        blob_store: &B,
        from_log_id: LogId,
        to_log_id_inclusive: LogId,
        filter: &LogFilter,
        take: usize,
    ) -> Result<Vec<MatchedPrimary<Log>>> {
        let mut materializer = LogMaterializer::new(meta_store, blob_store);
        let mut matched = Vec::new();

        for shard_raw in from_log_id.shard().get()..=to_log_id_inclusive.shard().get() {
            let shard = LogShard::new(shard_raw).expect("shard derived from LogId range");
            let (local_from, local_to) =
                local_range_for_shard(from_log_id, to_log_id_inclusive, shard);

            for local_raw in local_from.get()..=local_to.get() {
                let local = LogLocalId::new(local_raw).expect("local derived from shard range");
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
                    prepared_clause,
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

    prepared.sort_by_key(|clause| (clause.sealed_estimate, clause_kind_rank(clause.kind)));
    Ok(prepared)
}

async fn prepare_shard_clause<M: MetaStore>(
    meta_store: &M,
    clause_spec: &IndexedClauseSpec,
    shard: LogShard,
    local_from: LogLocalId,
    local_to: LogLocalId,
) -> Result<PreparedShardClause> {
    let mut prepared_streams = Vec::with_capacity(clause_spec.values.len());
    let mut in_flight = FuturesUnordered::new();

    for value in &clause_spec.values {
        let stream = stream_id(clause_spec.stream_kind, value, shard);
        in_flight.push(async move {
            prepare_shard_stream(meta_store, &stream, local_from.get(), local_to.get()).await
        });
        if in_flight.len() >= STREAM_LOAD_CONCURRENCY
            && let Some(result) = in_flight.next().await
        {
            prepared_streams.push(result?);
        }
    }

    while let Some(result) = in_flight.next().await {
        prepared_streams.push(result?);
    }

    let sealed_estimate = prepared_streams
        .iter()
        .map(|stream| stream.sealed_estimate)
        .sum();
    Ok(PreparedShardClause {
        kind: clause_spec.kind,
        prepared_streams,
        sealed_estimate,
    })
}

async fn prepare_shard_stream<M: MetaStore>(
    meta_store: &M,
    stream_id: &str,
    local_from: u32,
    local_to: u32,
) -> Result<PreparedShardStream> {
    let manifest = meta_store
        .get(&manifest_key(stream_id))
        .await?
        .map(|record| decode_manifest(&record.value))
        .transpose()?;
    let overlapping_chunk_refs = manifest
        .as_ref()
        .map(|manifest| overlapping_chunk_refs(&manifest.chunk_refs, local_from, local_to).to_vec())
        .unwrap_or_default();
    let sealed_estimate = if let Some(manifest) = &manifest {
        if is_full_shard_range(local_from, local_to) {
            manifest.approx_count
        } else {
            overlapping_chunk_refs
                .iter()
                .map(|chunk_ref| u64::from(chunk_ref.count))
                .sum()
        }
    } else {
        0
    };

    Ok(PreparedShardStream {
        stream_id: stream_id.to_owned(),
        manifest,
        overlapping_chunk_refs,
        sealed_estimate,
    })
}

async fn load_prepared_clause_bitmap<M: MetaStore, B: BlobStore>(
    meta_store: &M,
    blob_store: &B,
    prepared_clause: PreparedShardClause,
    local_from: u32,
    local_to: u32,
) -> Result<RoaringBitmap> {
    let mut out = RoaringBitmap::new();
    let mut in_flight = FuturesUnordered::new();

    for prepared_stream in prepared_clause.prepared_streams {
        in_flight.push(async move {
            let entries = load_prepared_stream_entries(
                meta_store,
                blob_store,
                &prepared_stream,
                local_from,
                local_to,
            )
            .await?;
            Ok::<RoaringBitmap, Error>(entries)
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

async fn load_prepared_stream_entries<M: MetaStore, B: BlobStore>(
    meta_store: &M,
    blob_store: &B,
    prepared_stream: &PreparedShardStream,
    local_from: u32,
    local_to: u32,
) -> Result<RoaringBitmap> {
    let mut out = RoaringBitmap::new();

    if prepared_stream.manifest.is_some() {
        for chunk_ref in &prepared_stream.overlapping_chunk_refs {
            maybe_merge_chunk(blob_store, &prepared_stream.stream_id, chunk_ref, &mut out).await?;
        }
    }

    if let Some(record) = meta_store
        .get(&tail_key(&prepared_stream.stream_id))
        .await?
    {
        let tail = decode_tail(&record.value)?;
        if is_full_shard_range(local_from, local_to) {
            out |= &tail;
        } else {
            for value in &tail {
                if value >= local_from && value <= local_to {
                    out.insert(value);
                }
            }
        }
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

    if let Some(record) = meta_store.get(&manifest_key(stream)).await? {
        let manifest = decode_manifest(&record.value)?;
        for chunk_ref in overlapping_chunk_refs(&manifest.chunk_refs, local_from, local_to) {
            maybe_merge_chunk(blob_store, stream, chunk_ref, &mut out).await?;
        }
    }

    if let Some(record) = meta_store.get(&tail_key(stream)).await? {
        let tail = decode_tail(&record.value)?;
        if is_full_shard_range(local_from, local_to) {
            out |= &tail;
        } else {
            for value in &tail {
                if value >= local_from && value <= local_to {
                    out.insert(value);
                }
            }
        }
    }

    Ok(out)
}

async fn maybe_merge_chunk<B: BlobStore>(
    blob_store: &B,
    stream: &str,
    chunk_ref: &ChunkRef,
    out: &mut RoaringBitmap,
) -> Result<()> {
    let Some(bytes) = blob_store
        .get_blob(&chunk_blob_key(stream, chunk_ref.chunk_seq))
        .await?
    else {
        return Ok(());
    };

    let chunk = decode_chunk(&bytes)?;
    if chunk.count != chunk_ref.count {
        return Err(Error::Decode("chunk count mismatch against manifest"));
    }

    *out |= &chunk.bitmap;
    Ok(())
}
