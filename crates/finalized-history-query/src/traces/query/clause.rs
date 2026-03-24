use crate::core::clause::Clause;
use crate::core::ids::{TraceLocalId, TraceShard};
use crate::error::Result;
use crate::query::bitmap;
use crate::query::planner::{
    IndexedClause, PreparedClause, clause_values, indexed_clause,
    prepare_shard_clauses as prepare_indexed_shard_clauses, single_selector_clause,
};
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;
use crate::traces::filter::TraceFilter;

pub(in crate::traces) type IndexedClauseSpec = IndexedClause;

pub(in crate::traces) type PreparedShardClause = PreparedClause;

pub(in crate::traces) fn build_clause_specs(filter: &TraceFilter) -> Vec<IndexedClauseSpec> {
    let mut clauses = Vec::new();

    if let Some(clause) = &filter.from
        && let Some(clause) = indexed_clause("from", clause_values_20(clause))
    {
        clauses.push(clause);
    }

    if let Some(clause) = &filter.to
        && let Some(clause) = indexed_clause("to", clause_values_20(clause))
    {
        clauses.push(clause);
    }

    if let Some(clause) = &filter.selector
        && let Some(clause) = indexed_clause("selector", clause_values_4(clause))
    {
        clauses.push(clause);
    }

    if filter.has_value == Some(true) {
        clauses.push(single_selector_clause("has_value", vec![1]));
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
    prepare_indexed_shard_clauses(
        &tables.trace_streams,
        clause_specs,
        shard.get(),
        local_from.get(),
        local_to.get(),
    )
    .await
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

fn clause_values_20(clause: &Clause<[u8; 20]>) -> Vec<Vec<u8>> {
    clause_values(clause)
}

fn clause_values_4(clause: &Clause<[u8; 4]>) -> Vec<Vec<u8>> {
    clause_values(clause)
}
