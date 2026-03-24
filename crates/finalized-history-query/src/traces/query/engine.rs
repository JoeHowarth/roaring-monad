use super::clause::{
    ClauseKind, IndexedClauseSpec, TracesStreamFamily, build_clause_specs, is_too_broad,
    prepare_shard_clauses,
};
use crate::api::{ExecutionBudget, QueryTracesRequest};
use crate::config::Config;
use crate::core::ids::{TraceId, TraceLocalId, TraceShard};
use crate::core::page::QueryPage;
use crate::core::range::resolve_block_range;
use crate::core::state::load_block_num_by_hash;
use crate::error::{Error, Result};
use crate::query::bitmap::load_prepared_clause_bitmap;
use crate::query::normalized::{effective_limit, normalize_query};
use crate::query::planner::PreparedClause;
use crate::query::runner::{QueryDescriptor, build_page, empty_page, execute_indexed_query};
use crate::store::publication::PublicationStore;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;
use crate::traces::filter::TraceFilter;
use crate::traces::keys::trace_local_range_for_shard;
use crate::traces::materialize::TraceMaterializer;
use crate::traces::state::resolve_trace_window;
use crate::traces::types::Trace;

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
        let effective_limit = effective_limit(request.limit, budget)?;

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
            return Ok(empty_page(&block_range));
        }

        let Some(trace_window) = resolve_trace_window(tables, &block_range).await? else {
            return Ok(empty_page(&block_range));
        };

        let Some(normalized) = normalize_query(
            &block_range,
            trace_window,
            request.resume_trace_id.map(TraceId::new),
            effective_limit,
            "resume_trace_id outside resolved block window",
        )?
        else {
            return Ok(empty_page(&block_range));
        };

        let descriptor = TracesQueryDescriptor;
        let mut materializer = TraceMaterializer::new(tables);
        let matched = execute_indexed_query(
            tables,
            &descriptor,
            &request.filter,
            (normalized.id_range.start, normalized.id_range.end_inclusive),
            normalized.take,
            &mut materializer,
        )
        .await?;

        Ok(build_page::<TraceMaterializer<'_, M, B>>(
            normalized.block_range,
            normalized.effective_limit,
            matched,
        ))
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

struct TracesQueryDescriptor;

impl QueryDescriptor for TracesQueryDescriptor {
    type Id = TraceId;
    type ClauseKind = ClauseKind;
    type ClauseSpec = IndexedClauseSpec;
    type Filter = TraceFilter;

    fn build_clause_specs(&self, filter: &Self::Filter) -> Vec<Self::ClauseSpec> {
        build_clause_specs(filter)
    }

    fn local_range_for_shard(
        &self,
        from: Self::Id,
        to_inclusive: Self::Id,
        shard_raw: u64,
    ) -> (u32, u32) {
        let shard = TraceShard::new(shard_raw).expect("shard derived from TraceId range");
        let (local_from, local_to) = trace_local_range_for_shard(from, to_inclusive, shard);
        (local_from.get(), local_to.get())
    }

    async fn prepare_shard_clauses<M: MetaStore, B: BlobStore>(
        &self,
        tables: &Tables<M, B>,
        clause_specs: &[Self::ClauseSpec],
        shard_raw: u64,
        local_from: u32,
        local_to: u32,
    ) -> Result<Vec<PreparedClause<Self::ClauseKind>>> {
        let shard = TraceShard::new(shard_raw).expect("shard derived from TraceId range");
        prepare_shard_clauses(
            tables,
            clause_specs,
            shard,
            TraceLocalId::new(local_from).expect("local range start must fit trace local-id"),
            TraceLocalId::new(local_to).expect("local range end must fit trace local-id"),
        )
        .await
    }

    async fn load_prepared_clause_bitmap<M: MetaStore, B: BlobStore>(
        &self,
        tables: &Tables<M, B>,
        prepared_clause: &PreparedClause<Self::ClauseKind>,
        local_from: u32,
        local_to: u32,
    ) -> Result<roaring::RoaringBitmap> {
        load_prepared_clause_bitmap::<M, B, _, TracesStreamFamily>(
            tables,
            prepared_clause,
            local_from,
            local_to,
        )
        .await
    }
}
