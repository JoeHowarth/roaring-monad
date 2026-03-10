use std::collections::BTreeSet;

use futures::stream::{FuturesUnordered, StreamExt};

use crate::api::query_logs::{ExecutionBudget, QueryLogsRequest};
use crate::codec::chunk::decode_chunk;
use crate::codec::manifest::{ChunkRef, decode_manifest, decode_tail};
use crate::config::{BroadQueryPolicy, Config};
use crate::core::execution::{MatchedPrimary, execute_candidates};
use crate::core::page::{QueryPage, QueryPageMeta};
use crate::core::range::{RangeResolver, ResolvedBlockRange};
use crate::domain::keys::{
    chunk_blob_key, compose_global_log_id, local_range_for_shard, log_shard, manifest_key,
    stream_id, tail_key,
};
use crate::error::{Error, Result};
use crate::logs::block_scan::LogBlockScanner;
use crate::logs::filter::LogFilter;
use crate::logs::index_spec::{
    ClauseKind, build_clause_order, clause_values_20, clause_values_32, is_full_shard_range,
    overlapping_chunk_refs, should_error_too_broad, should_force_block_scan,
};
use crate::logs::materialize::LogMaterializer;
use crate::logs::types::Log;
use crate::logs::window::LogWindowResolver;
use crate::store::traits::{BlobStore, MetaStore};

const STREAM_LOAD_CONCURRENCY: usize = 32;

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

        if let Some(resume_log_id) = request.resume_log_id {
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
            } else {
                let clause_order = build_clause_order(
                    meta_store,
                    &request.filter,
                    log_window.start,
                    log_window.end_inclusive,
                )
                .await?;
                let clause_sets = load_clause_sets(
                    meta_store,
                    blob_store,
                    &request.filter,
                    &clause_order,
                    log_window.start,
                    log_window.end_inclusive,
                )
                .await?;
                let mut materializer = LogMaterializer::new(meta_store, blob_store);
                execute_candidates(
                    clause_sets,
                    log_window,
                    &request.filter,
                    &mut materializer,
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
            matched.last().map(|matched_log| matched_log.id)
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
}

async fn load_clause_sets<M: MetaStore, B: BlobStore>(
    meta_store: &M,
    blob_store: &B,
    filter: &LogFilter,
    clause_order: &[ClauseKind],
    from_log_id: u64,
    to_log_id_inclusive: u64,
) -> Result<Vec<BTreeSet<u64>>> {
    let mut clause_sets = Vec::new();

    for clause_kind in clause_order {
        let set = match clause_kind {
            ClauseKind::Address => {
                let Some(clause) = &filter.address else {
                    continue;
                };
                let values = clause_values_20(clause);
                fetch_union_log_level(
                    meta_store,
                    blob_store,
                    "addr",
                    &values,
                    from_log_id,
                    to_log_id_inclusive,
                )
                .await?
            }
            ClauseKind::Topic0 => {
                let Some(clause) = &filter.topic0 else {
                    continue;
                };
                let values = clause_values_32(clause);
                fetch_union_log_level(
                    meta_store,
                    blob_store,
                    "topic0",
                    &values,
                    from_log_id,
                    to_log_id_inclusive,
                )
                .await?
            }
            ClauseKind::Topic1 => {
                let Some(clause) = &filter.topic1 else {
                    continue;
                };
                let values = clause_values_32(clause);
                fetch_union_log_level(
                    meta_store,
                    blob_store,
                    "topic1",
                    &values,
                    from_log_id,
                    to_log_id_inclusive,
                )
                .await?
            }
            ClauseKind::Topic2 => {
                let Some(clause) = &filter.topic2 else {
                    continue;
                };
                let values = clause_values_32(clause);
                fetch_union_log_level(
                    meta_store,
                    blob_store,
                    "topic2",
                    &values,
                    from_log_id,
                    to_log_id_inclusive,
                )
                .await?
            }
            ClauseKind::Topic3 => {
                let Some(clause) = &filter.topic3 else {
                    continue;
                };
                let values = clause_values_32(clause);
                fetch_union_log_level(
                    meta_store,
                    blob_store,
                    "topic3",
                    &values,
                    from_log_id,
                    to_log_id_inclusive,
                )
                .await?
            }
        };
        clause_sets.push(set);
    }

    Ok(clause_sets)
}

async fn fetch_union_log_level<M: MetaStore, B: BlobStore>(
    meta_store: &M,
    blob_store: &B,
    kind: &str,
    values: &[Vec<u8>],
    from_log_id: u64,
    to_log_id_inclusive: u64,
) -> Result<BTreeSet<u64>> {
    let mut out = BTreeSet::new();
    let from_shard = log_shard(from_log_id);
    let to_shard = log_shard(to_log_id_inclusive);
    let mut in_flight = FuturesUnordered::new();

    for value in values {
        for shard in from_shard..=to_shard {
            let stream = stream_id(kind, value, shard);
            let (local_from, local_to) =
                local_range_for_shard(from_log_id, to_log_id_inclusive, shard);
            in_flight.push(async move {
                let entries =
                    load_stream_entries(meta_store, blob_store, &stream, local_from, local_to)
                        .await?;
                Ok::<(u32, BTreeSet<u32>), Error>((shard, entries))
            });
            if in_flight.len() >= STREAM_LOAD_CONCURRENCY
                && let Some(result) = in_flight.next().await
            {
                let (shard, entries) = result?;
                for local in entries {
                    out.insert(compose_global_log_id(shard, local));
                }
            }
        }
    }

    while let Some(result) = in_flight.next().await {
        let (shard, entries) = result?;
        for local in entries {
            out.insert(compose_global_log_id(shard, local));
        }
    }

    Ok(out)
}

async fn load_stream_entries<M: MetaStore, B: BlobStore>(
    meta_store: &M,
    blob_store: &B,
    stream: &str,
    local_from: u32,
    local_to: u32,
) -> Result<BTreeSet<u32>> {
    let mut out = BTreeSet::new();

    if let Some(record) = meta_store.get(&manifest_key(stream)).await? {
        let manifest = decode_manifest(&record.value)?;
        for chunk_ref in overlapping_chunk_refs(&manifest.chunk_refs, local_from, local_to) {
            maybe_merge_chunk(blob_store, stream, chunk_ref, &mut out).await?;
        }
    }

    if let Some(record) = meta_store.get(&tail_key(stream)).await? {
        let tail = decode_tail(&record.value)?;
        if is_full_shard_range(local_from, local_to) {
            for value in &tail {
                out.insert(value);
            }
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
    out: &mut BTreeSet<u32>,
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

    for value in chunk.bitmap {
        out.insert(value);
    }
    Ok(())
}
