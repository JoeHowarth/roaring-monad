use roaring::RoaringBitmap;

use super::clause::{build_clause_specs, is_too_broad, prepare_shard_clauses};
use super::stream_bitmap::load_prepared_clause_bitmap;
use crate::api::{ExecutionBudget, QueryTracesRequest};
use crate::config::Config;
use crate::core::ids::{TraceId, TraceLocalId, TraceShard, compose_trace_id};
use crate::core::page::{QueryPage, QueryPageMeta};
use crate::core::range::{ResolvedBlockRange, resolve_block_range};
use crate::core::state::load_block_num_by_hash;
use crate::error::{Error, Result};
use crate::store::publication::PublicationStore;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;
use crate::traces::filter::TraceFilter;
use crate::traces::keys::trace_local_range_for_shard;
use crate::traces::materialize::{ResolvedTraceLocation, TraceMaterializer};
use crate::traces::state::resolve_trace_window;
use crate::traces::types::Trace;

#[derive(Debug, Clone, PartialEq, Eq)]
struct MatchedTrace {
    id: TraceId,
    item: Trace,
    block_ref: crate::core::refs::BlockRef,
}

#[allow(async_fn_in_trait)]
trait TraceLocationResolver {
    async fn resolve_trace_id(&mut self, id: TraceId) -> Result<Option<ResolvedTraceLocation>>;
}

impl<M: MetaStore, B: BlobStore> TraceLocationResolver for TraceMaterializer<'_, M, B> {
    async fn resolve_trace_id(&mut self, id: TraceId) -> Result<Option<ResolvedTraceLocation>> {
        TraceMaterializer::resolve_trace_id(self, id).await
    }
}

#[derive(Debug, Clone)]
pub struct TracesQueryEngine {
    max_or_terms: usize,
}

impl TracesQueryEngine {
    pub fn from_config(config: &Config) -> Self {
        Self {
            max_or_terms: config.planner_max_or_terms,
        }
    }

    pub async fn query_traces<M: MetaStore, P: PublicationStore, B: BlobStore>(
        &self,
        tables: &Tables<M, B>,
        publication_store: &P,
        request: QueryTracesRequest,
        budget: ExecutionBudget,
    ) -> Result<QueryPage<Trace>> {
        if request.limit == 0 {
            return Err(Error::InvalidParams("limit must be at least 1"));
        }
        if !request.filter.has_indexed_clause() {
            return Err(Error::InvalidParams(
                "query must include at least one indexed trace predicate",
            ));
        }
        if is_too_broad(&request.filter, self.max_or_terms) {
            return Err(Error::QueryTooBroad {
                actual: request.filter.max_or_terms(),
                max: self.max_or_terms,
            });
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

        let (from_block, to_block) = resolve_request_block_bounds(tables, &request).await?;
        let block_range = resolve_block_range(
            tables,
            publication_store,
            from_block,
            to_block,
            request.order,
        )
        .await?;
        if block_range.is_empty() {
            return Ok(self.empty_page(&block_range));
        }

        let Some(mut trace_window) = resolve_trace_window(tables, &block_range).await? else {
            return Ok(self.empty_page(&block_range));
        };

        if let Some(resume_trace_id) = request.resume_trace_id.map(TraceId::new) {
            if !trace_window.contains(resume_trace_id) {
                return Err(Error::InvalidParams(
                    "resume_trace_id outside resolved block window",
                ));
            }
            let Some(resumed_window) = trace_window.resume_strictly_after(resume_trace_id) else {
                return Ok(self.empty_page(&block_range));
            };
            trace_window = resumed_window;
        }

        let matched = self
            .execute_indexed_query(
                tables,
                &request.filter,
                (trace_window.start, trace_window.end_inclusive),
                effective_limit.saturating_add(1),
            )
            .await?;

        Ok(self.build_page(block_range, effective_limit, matched))
    }

    fn empty_page(&self, block_range: &ResolvedBlockRange) -> QueryPage<Trace> {
        QueryPage {
            items: Vec::new(),
            meta: QueryPageMeta {
                resolved_from_block: block_range.resolved_from_ref,
                resolved_to_block: block_range.resolved_to_ref,
                cursor_block: block_range.examined_endpoint_ref,
                has_more: false,
                next_resume_id: None,
            },
        }
    }

    fn build_page(
        &self,
        block_range: ResolvedBlockRange,
        effective_limit: usize,
        mut matched: Vec<MatchedTrace>,
    ) -> QueryPage<Trace> {
        let has_more = matched.len() > effective_limit;
        if has_more {
            matched.truncate(effective_limit);
        }

        let next_resume_id = if has_more {
            matched.last().map(|matched_trace| matched_trace.id.get())
        } else {
            None
        };
        let cursor_block = matched
            .last()
            .map(|matched_trace| matched_trace.block_ref)
            .unwrap_or(block_range.examined_endpoint_ref);
        let items = matched
            .into_iter()
            .map(|matched_trace| matched_trace.item)
            .collect();

        QueryPage {
            items,
            meta: QueryPageMeta {
                resolved_from_block: block_range.resolved_from_ref,
                resolved_to_block: block_range.resolved_to_ref,
                cursor_block,
                has_more,
                next_resume_id,
            },
        }
    }

    async fn execute_indexed_query<M: MetaStore, B: BlobStore>(
        &self,
        tables: &Tables<M, B>,
        filter: &TraceFilter,
        id_window: (TraceId, TraceId),
        take: usize,
    ) -> Result<Vec<MatchedTrace>> {
        let (from_trace_id, to_trace_id_inclusive) = id_window;
        let clause_specs = build_clause_specs(filter);
        let mut matched = Vec::new();
        let mut materializer = TraceMaterializer::new(tables);

        for shard_raw in from_trace_id.shard().get()..=to_trace_id_inclusive.shard().get() {
            let shard = TraceShard::new(shard_raw).expect("shard derived from TraceId range");
            let (local_from, local_to) =
                trace_local_range_for_shard(from_trace_id, to_trace_id_inclusive, shard);
            let shard_clauses =
                prepare_shard_clauses(tables, &clause_specs, shard, local_from, local_to).await?;

            if shard_clauses.is_empty() {
                continue;
            }

            let mut shard_accumulator: Option<RoaringBitmap> = None;
            for prepared_clause in shard_clauses {
                let clause_bitmap = load_prepared_clause_bitmap(
                    tables,
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

            let mut locals = shard_accumulator.into_iter().peekable();
            while let Some(local_raw) = locals.next() {
                let local = TraceLocalId::new(local_raw).expect("bitmap values must fit local-id");
                let id = compose_trace_id(shard, local);
                let Some(location) = materializer.resolve_trace_id(id).await? else {
                    continue;
                };

                let run = collect_contiguous_chunk(
                    &mut locals,
                    shard,
                    (id, location),
                    remaining_needed_for_chunk(take, matched.len()),
                    &mut materializer,
                )
                .await?;

                for (run_id, run_location) in run {
                    let Some(item) = materializer
                        .load_trace_at(run_location.block_num, run_location.local_ordinal)
                        .await?
                    else {
                        continue;
                    };
                    if !materializer.exact_match_trace(&item, filter) {
                        continue;
                    }

                    let block_ref = materializer.block_ref_for_trace(&item).await?;
                    matched.push(MatchedTrace {
                        id: run_id,
                        item,
                        block_ref,
                    });
                    if matched.len() >= take {
                        return Ok(matched);
                    }
                }
            }
        }

        Ok(matched)
    }
}

async fn resolve_request_block_bounds<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    request: &QueryTracesRequest,
) -> Result<(u64, u64)> {
    let from_block = match (request.from_block, request.from_block_hash) {
        (Some(number), None) => number,
        (None, Some(hash)) => load_block_num_by_hash(tables, &hash)
            .await?
            .ok_or(Error::InvalidParams("unknown from_block_hash"))?,
        _ => {
            return Err(Error::InvalidParams(
                "exactly one of from_block or from_block_hash is required",
            ));
        }
    };
    let to_block = match (request.to_block, request.to_block_hash) {
        (Some(number), None) => number,
        (None, Some(hash)) => load_block_num_by_hash(tables, &hash)
            .await?
            .ok_or(Error::InvalidParams("unknown to_block_hash"))?,
        _ => {
            return Err(Error::InvalidParams(
                "exactly one of to_block or to_block_hash is required",
            ));
        }
    };
    Ok((from_block, to_block))
}

async fn collect_contiguous_chunk<I, R: TraceLocationResolver>(
    locals: &mut std::iter::Peekable<I>,
    shard: TraceShard,
    first: (TraceId, ResolvedTraceLocation),
    max_len: usize,
    materializer: &mut R,
) -> Result<Vec<(TraceId, ResolvedTraceLocation)>>
where
    I: Iterator<Item = u32>,
{
    let target_len = max_len.max(1);
    let mut run = vec![first];
    while run.len() < target_len {
        let Some(&next_local_raw) = locals.peek() else {
            break;
        };
        let next_local =
            TraceLocalId::new(next_local_raw).expect("bitmap values must fit local-id width");
        let next_id = compose_trace_id(shard, next_local);
        let Some(next_location) = materializer.resolve_trace_id(next_id).await? else {
            let _ = locals.next();
            continue;
        };
        let previous = run.last().expect("run must be non-empty").1;
        if next_location.block_num != previous.block_num
            || next_location.local_ordinal != previous.local_ordinal + 1
        {
            break;
        }

        let _ = locals.next();
        run.push((next_id, next_location));
    }
    Ok(run)
}

fn remaining_needed_for_chunk(take: usize, matched_len: usize) -> usize {
    take.saturating_sub(matched_len).max(1)
}
