use crate::core::clause::Clause;
use crate::core::ids::{LogLocalId, LogShard};
use crate::core::layout::MAX_LOCAL_ID;
use crate::error::Result;
use crate::kernel::sharded_streams::{page_start_local, sharded_stream_id};
use crate::logs::filter::LogFilter;
use crate::logs::keys::STREAM_PAGE_LOCAL_ID_SPAN;
use crate::query::planner::{
    IndexedClause, PreparedClause, StreamSelector, clause_values, indexed_clause,
    prepare_shard_clauses as prepare_query_shard_clauses,
};
use crate::query::stream_family::StreamIndexFamily;
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

pub(crate) struct LogsStreamFamily;

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
    prepare_query_shard_clauses::<M, B, LogsStreamFamily>(
        tables,
        clause_specs,
        shard,
        local_from.get(),
        local_to.get(),
    )
    .await
}

impl StreamIndexFamily for LogsStreamFamily {
    type Shard = LogShard;
    type ClauseKind = ClauseKind;
    type BitmapMeta = crate::logs::types::StreamBitmapMeta;

    fn stream_tables<M: MetaStore, B: BlobStore>(
        tables: &Tables<M, B>,
    ) -> &crate::tables::StreamTables<M, B, Self::BitmapMeta> {
        &tables.log_streams
    }

    fn stream_id(selector: &StreamSelector, shard: Self::Shard) -> String {
        sharded_stream_id(selector.stream_kind, &selector.value, shard.get())
    }

    fn clause_sort_rank(kind: Self::ClauseKind) -> u8 {
        match kind {
            ClauseKind::Address => 0,
            ClauseKind::Topic1 => 1,
            ClauseKind::Topic2 => 2,
            ClauseKind::Topic3 => 3,
            ClauseKind::Topic0 => 4,
        }
    }

    fn first_page_start(local_from: u32) -> u32 {
        page_start_local(local_from, STREAM_PAGE_LOCAL_ID_SPAN)
    }

    fn last_page_start(local_to: u32) -> u32 {
        page_start_local(local_to, STREAM_PAGE_LOCAL_ID_SPAN)
    }

    fn next_page_start(page_start: u32) -> u32 {
        page_start.saturating_add(STREAM_PAGE_LOCAL_ID_SPAN)
    }

    fn is_full_shard_range(local_from: u32, local_to: u32) -> bool {
        local_from == 0 && local_to == MAX_LOCAL_ID
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

fn clause_values_32(clause: &Clause<[u8; 32]>) -> Vec<Vec<u8>> {
    clause_values(clause)
}
