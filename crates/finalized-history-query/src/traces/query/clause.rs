use crate::core::clause::Clause;
use crate::core::ids::{TraceLocalId, TraceShard};
use crate::error::Result;
use crate::kernel::sharded_streams::page_start_local;
use crate::query::planner::{
    IndexedClause, PreparedClause, StreamSelector, clause_values, indexed_clause,
    prepare_shard_clauses as prepare_query_shard_clauses, single_selector_clause,
};
use crate::query::stream_family::StreamIndexFamily;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;
use crate::traces::filter::TraceFilter;
use crate::traces::keys::{
    MAX_TRACE_LOCAL_ID, TRACE_STREAM_PAGE_LOCAL_ID_SPAN, has_value_stream_id,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::traces) enum ClauseKind {
    From,
    To,
    Selector,
    HasValue,
}

pub(in crate::traces) type IndexedClauseSpec = IndexedClause<ClauseKind>;

pub(in crate::traces) type PreparedShardClause = PreparedClause<ClauseKind>;

pub(in crate::traces) struct TracesStreamFamily;

pub(crate) fn is_too_broad(filter: &TraceFilter, max_or_terms: usize) -> bool {
    filter.max_or_terms() > max_or_terms
}

pub(in crate::traces) fn build_clause_specs(filter: &TraceFilter) -> Vec<IndexedClauseSpec> {
    let mut clauses = Vec::new();

    if let Some(clause) = &filter.from
        && let Some(clause) = indexed_clause(ClauseKind::From, "from", clause_values_20(clause))
    {
        clauses.push(clause);
    }

    if let Some(clause) = &filter.to
        && let Some(clause) = indexed_clause(ClauseKind::To, "to", clause_values_20(clause))
    {
        clauses.push(clause);
    }

    if let Some(clause) = &filter.selector
        && let Some(clause) =
            indexed_clause(ClauseKind::Selector, "selector", clause_values_4(clause))
    {
        clauses.push(clause);
    }

    if filter.has_value == Some(true) {
        clauses.push(single_selector_clause(
            ClauseKind::HasValue,
            "has_value",
            vec![1],
        ));
    }

    clauses
}

pub(in crate::traces) async fn prepare_shard_clauses<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    clause_specs: &[IndexedClauseSpec],
    shard: TraceShard,
    local_from: TraceLocalId,
    local_to: TraceLocalId,
) -> Result<Vec<PreparedShardClause>> {
    prepare_query_shard_clauses::<M, B, TracesStreamFamily>(
        tables,
        clause_specs,
        shard,
        local_from.get(),
        local_to.get(),
    )
    .await
}

impl StreamIndexFamily for TracesStreamFamily {
    type Shard = TraceShard;
    type ClauseKind = ClauseKind;
    type BitmapMeta = crate::traces::types::StreamBitmapMeta;

    fn stream_tables<M: MetaStore, B: BlobStore>(
        tables: &Tables<M, B>,
    ) -> &crate::tables::StreamTables<M, B, Self::BitmapMeta> {
        tables.trace_streams()
    }

    fn stream_id(selector: &StreamSelector, shard: Self::Shard) -> String {
        match selector.stream_kind {
            "has_value" => has_value_stream_id(shard),
            _ => crate::traces::keys::stream_id(selector.stream_kind, &selector.value, shard),
        }
    }

    fn clause_sort_rank(kind: Self::ClauseKind) -> u8 {
        match kind {
            ClauseKind::From => 0,
            ClauseKind::To => 1,
            ClauseKind::Selector => 2,
            ClauseKind::HasValue => 3,
        }
    }

    fn first_page_start(local_from: u32) -> u32 {
        page_start_local(local_from, TRACE_STREAM_PAGE_LOCAL_ID_SPAN)
    }

    fn last_page_start(local_to: u32) -> u32 {
        page_start_local(local_to, TRACE_STREAM_PAGE_LOCAL_ID_SPAN)
    }

    fn next_page_start(page_start: u32) -> u32 {
        page_start.saturating_add(TRACE_STREAM_PAGE_LOCAL_ID_SPAN)
    }

    fn is_full_shard_range(local_from: u32, local_to: u32) -> bool {
        local_from == 0 && local_to == MAX_TRACE_LOCAL_ID
    }

    fn meta_overlaps(meta: &Self::BitmapMeta, local_from: u32, local_to: u32) -> bool {
        crate::kernel::sharded_streams::overlaps(
            meta.min_local,
            meta.max_local,
            local_from,
            local_to,
        )
    }

    fn meta_count(meta: &Self::BitmapMeta) -> u32 {
        meta.count
    }
}

fn clause_values_20(clause: &Clause<[u8; 20]>) -> Vec<Vec<u8>> {
    clause_values(clause)
}

fn clause_values_4(clause: &Clause<[u8; 4]>) -> Vec<Vec<u8>> {
    clause_values(clause)
}
