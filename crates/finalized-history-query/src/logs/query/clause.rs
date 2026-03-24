use crate::core::clause::Clause;
use crate::core::ids::{LogLocalId, LogShard};
use crate::error::Result;
use crate::kernel::sharded_streams::sharded_stream_id;
use crate::logs::filter::LogFilter;
use crate::query::bitmap;
use crate::query::planner::{IndexedClause, PreparedClause, clause_values, indexed_clause};
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClauseKind {
    Address,
    Topic0,
    Topic1,
    Topic2,
    Topic3,
}

pub(in crate::logs) type IndexedClauseSpec = IndexedClause<ClauseKind>;

pub(in crate::logs) type PreparedShardClause = PreparedClause<ClauseKind>;

pub(in crate::logs) fn build_clause_specs(filter: &LogFilter) -> Vec<IndexedClauseSpec> {
    let mut clauses = Vec::new();

    if let Some(clause) = &filter.address
        && let Some(clause) = indexed_clause(ClauseKind::Address, "addr", clause_values_20(clause))
    {
        clauses.push(clause);
    }

    if let Some(clause) = &filter.topic1
        && let Some(clause) = indexed_clause(ClauseKind::Topic1, "topic1", clause_values_32(clause))
    {
        clauses.push(clause);
    }

    if let Some(clause) = &filter.topic2
        && let Some(clause) = indexed_clause(ClauseKind::Topic2, "topic2", clause_values_32(clause))
    {
        clauses.push(clause);
    }

    if let Some(clause) = &filter.topic3
        && let Some(clause) = indexed_clause(ClauseKind::Topic3, "topic3", clause_values_32(clause))
    {
        clauses.push(clause);
    }

    if let Some(clause) = &filter.topic0
        && let Some(clause) = indexed_clause(ClauseKind::Topic0, "topic0", clause_values_32(clause))
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
    let mut prepared = Vec::with_capacity(clause_specs.len());
    for clause_spec in clause_specs {
        let mut stream_ids = Vec::with_capacity(clause_spec.selectors.len());
        let mut estimated_count = 0u64;

        for selector in &clause_spec.selectors {
            let stream_id = sharded_stream_id(selector.stream_kind, &selector.value, shard.get());
            estimated_count = estimated_count.saturating_add(
                bitmap::estimate_stream_overlap(
                    &tables.log_streams,
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

fn clause_sort_rank(kind: ClauseKind) -> u8 {
    match kind {
        ClauseKind::Address => 0,
        ClauseKind::Topic1 => 1,
        ClauseKind::Topic2 => 2,
        ClauseKind::Topic3 => 3,
        ClauseKind::Topic0 => 4,
    }
}

fn clause_values_20(clause: &Clause<[u8; 20]>) -> Vec<Vec<u8>> {
    clause_values(clause)
}

fn clause_values_32(clause: &Clause<[u8; 32]>) -> Vec<Vec<u8>> {
    clause_values(clause)
}
