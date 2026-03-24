use crate::core::clause::Clause;
use crate::core::ids::{TraceLocalId, TraceShard};
use crate::error::Result;
use crate::kernel::sharded_streams::sharded_stream_id;
use crate::query::bitmap;
use crate::query::planner::{
    IndexedClause, PreparedClause, clause_values, indexed_clause, single_selector_clause,
};
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;
use crate::traces::filter::TraceFilter;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::traces) enum ClauseKind {
    From,
    To,
    Selector,
    HasValue,
}

pub(in crate::traces) type IndexedClauseSpec = IndexedClause<ClauseKind>;

pub(in crate::traces) type PreparedShardClause = PreparedClause<ClauseKind>;

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
    let mut prepared = Vec::with_capacity(clause_specs.len());
    for clause_spec in clause_specs {
        let mut stream_ids = Vec::with_capacity(clause_spec.selectors.len());
        let mut estimated_count = 0u64;

        for selector in &clause_spec.selectors {
            let stream_id = match selector.stream_kind {
                "has_value" => sharded_stream_id("has_value", b"\x01", shard.get()),
                _ => sharded_stream_id(selector.stream_kind, &selector.value, shard.get()),
            };
            estimated_count = estimated_count.saturating_add(
                bitmap::estimate_stream_overlap(
                    &tables.trace_streams,
                    &stream_id,
                    local_from.get(),
                    local_to.get(),
                )
                .await?,
            );
            stream_ids.push(stream_id);
        }

        prepared.push(PreparedClause {
            kind: clause_spec.kind,
            stream_ids,
            estimated_count,
        });
    }
    prepared.sort_by_key(|clause| (clause.estimated_count, clause_sort_rank(clause.kind)));
    Ok(prepared)
}

pub(in crate::traces) async fn load_prepared_clause_bitmap<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    prepared_clause: &PreparedShardClause,
    local_from: TraceLocalId,
    local_to: TraceLocalId,
) -> Result<roaring::RoaringBitmap> {
    bitmap::load_prepared_clause_bitmap(
        &tables.trace_streams,
        prepared_clause,
        local_from.get(),
        local_to.get(),
    )
    .await
}

fn clause_sort_rank(kind: ClauseKind) -> u8 {
    match kind {
        ClauseKind::From => 0,
        ClauseKind::To => 1,
        ClauseKind::Selector => 2,
        ClauseKind::HasValue => 3,
    }
}

fn clause_values_20(clause: &Clause<[u8; 20]>) -> Vec<Vec<u8>> {
    clause_values(clause)
}

fn clause_values_4(clause: &Clause<[u8; 4]>) -> Vec<Vec<u8>> {
    clause_values(clause)
}
