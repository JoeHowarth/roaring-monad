use crate::core::clause::Clause;
use crate::core::ids::{LogLocalId, LogShard};
use crate::error::Result;
use crate::logs::filter::LogFilter;
use crate::query::bitmap;
use crate::query::planner::{
    IndexedClause, PreparedClause, clause_values, indexed_clause,
    prepare_shard_clauses as prepare_indexed_shard_clauses,
};
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;

pub(in crate::logs) type IndexedClauseSpec = IndexedClause;

pub(in crate::logs) type PreparedShardClause = PreparedClause;

pub(in crate::logs) fn build_clause_specs(filter: &LogFilter) -> Vec<IndexedClauseSpec> {
    let mut clauses = Vec::new();

    if let Some(clause) = &filter.address
        && let Some(clause) = indexed_clause("addr", clause_values_20(clause))
    {
        clauses.push(clause);
    }

    if let Some(clause) = &filter.topic1
        && let Some(clause) = indexed_clause("topic1", clause_values_32(clause))
    {
        clauses.push(clause);
    }

    if let Some(clause) = &filter.topic2
        && let Some(clause) = indexed_clause("topic2", clause_values_32(clause))
    {
        clauses.push(clause);
    }

    if let Some(clause) = &filter.topic3
        && let Some(clause) = indexed_clause("topic3", clause_values_32(clause))
    {
        clauses.push(clause);
    }

    if let Some(clause) = &filter.topic0
        && let Some(clause) = indexed_clause("topic0", clause_values_32(clause))
    {
        clauses.push(clause);
    }

    clauses
}

pub(in crate::logs) async fn prepare_shard_clauses<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    clause_specs: &[IndexedClauseSpec],
    shard: LogShard,
    local_from: LogLocalId,
    local_to: LogLocalId,
) -> Result<Vec<PreparedShardClause>> {
    prepare_indexed_shard_clauses(
        &tables.log_streams,
        clause_specs,
        shard.get(),
        local_from.get(),
        local_to.get(),
    )
    .await
}

pub(in crate::logs) async fn load_prepared_clause_bitmap<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    prepared_clause: &PreparedShardClause,
    _local_from: LogLocalId,
    _local_to: LogLocalId,
) -> Result<roaring::RoaringBitmap> {
    bitmap::load_prepared_clause_bitmap(
        &tables.log_streams,
        prepared_clause,
        _local_from.get(),
        _local_to.get(),
    )
    .await
}

fn clause_values_20(clause: &Clause<[u8; 20]>) -> Vec<Vec<u8>> {
    clause_values(clause)
}

fn clause_values_32(clause: &Clause<[u8; 32]>) -> Vec<Vec<u8>> {
    clause_values(clause)
}
