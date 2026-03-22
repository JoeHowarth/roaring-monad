use crate::core::clause::Clause;
use crate::core::ids::{LogLocalId, LogShard};
use crate::error::Result;
use crate::logs::filter::LogFilter;
use crate::logs::keys::{MAX_LOCAL_ID, STREAM_PAGE_LOCAL_ID_SPAN};
use crate::logs::table_specs;
use crate::store::traits::{BlobStore, MetaStore};
use crate::streams::decode_bitmap_blob;
use crate::tables::Tables;

use super::stream_bitmap::{load_bitmap_page_meta, overlaps};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::logs) enum ClauseKind {
    Address,
    Topic0,
    Topic1,
    Topic2,
    Topic3,
}

#[derive(Debug, Clone)]
pub(in crate::logs) struct IndexedClauseSpec {
    pub(in crate::logs) kind: ClauseKind,
    pub(in crate::logs) stream_kind: &'static str,
    pub(in crate::logs) values: Vec<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub(in crate::logs) struct PreparedShardClause {
    pub(in crate::logs) kind: ClauseKind,
    pub(in crate::logs) stream_ids: Vec<String>,
    pub(in crate::logs) estimated_count: u64,
}

pub(crate) fn is_too_broad(filter: &LogFilter, max_or_terms: usize) -> bool {
    filter.max_or_terms() > max_or_terms
}

pub(crate) fn is_full_shard_range(local_from: u32, local_to: u32) -> bool {
    local_from == 0 && local_to == MAX_LOCAL_ID
}

pub(in crate::logs) fn build_clause_specs(filter: &LogFilter) -> Vec<IndexedClauseSpec> {
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

pub(in crate::logs) async fn prepare_shard_clauses<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    clause_specs: &[IndexedClauseSpec],
    shard: LogShard,
    local_from: LogLocalId,
    local_to: LogLocalId,
) -> Result<Vec<PreparedShardClause>> {
    let mut prepared = Vec::with_capacity(clause_specs.len());

    for clause_spec in clause_specs {
        let clause = prepare_shard_clause(tables, clause_spec, shard, local_from, local_to).await?;
        prepared.push(clause);
    }

    prepared.sort_by_key(|clause| (clause.estimated_count, clause_kind_rank(clause.kind)));
    Ok(prepared)
}

async fn prepare_shard_clause<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    clause_spec: &IndexedClauseSpec,
    shard: LogShard,
    local_from: LogLocalId,
    local_to: LogLocalId,
) -> Result<PreparedShardClause> {
    let mut stream_ids = Vec::with_capacity(clause_spec.values.len());
    let mut estimated_count = 0u64;

    for value in &clause_spec.values {
        let stream = table_specs::stream_id(clause_spec.stream_kind, value, shard);
        estimated_count = estimated_count.saturating_add(
            estimate_stream_overlap(tables, &stream, local_from.get(), local_to.get()).await?,
        );
        stream_ids.push(stream);
    }

    Ok(PreparedShardClause {
        kind: clause_spec.kind,
        stream_ids,
        estimated_count,
    })
}

async fn estimate_stream_overlap<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    stream_id: &str,
    local_from: u32,
    local_to: u32,
) -> Result<u64> {
    let mut estimated = 0u64;
    let mut page_start = table_specs::stream_page_start_local(local_from);
    let last_page_start = table_specs::stream_page_start_local(local_to);

    loop {
        if let Some(meta) = load_bitmap_page_meta(tables, stream_id, page_start).await? {
            if overlaps(meta.min_local, meta.max_local, local_from, local_to) {
                estimated = estimated.saturating_add(u64::from(meta.count));
            }
        } else {
            for bytes in tables
                .bitmap_by_block()
                .load_page_fragments(stream_id, page_start)
                .await?
            {
                let meta = decode_bitmap_blob(&bytes)?;
                if overlaps(meta.min_local, meta.max_local, local_from, local_to) {
                    estimated = estimated.saturating_add(u64::from(meta.count));
                }
            }
        }

        if page_start == last_page_start {
            break;
        }
        page_start = page_start.saturating_add(STREAM_PAGE_LOCAL_ID_SPAN);
    }

    Ok(estimated)
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

fn clause_values_20(clause: &Clause<[u8; 20]>) -> Vec<Vec<u8>> {
    match clause {
        Clause::Any => Vec::new(),
        Clause::One(value) => vec![value.to_vec()],
        Clause::Or(values) => values.iter().map(|value| value.to_vec()).collect(),
    }
}

fn clause_values_32(clause: &Clause<[u8; 32]>) -> Vec<Vec<u8>> {
    match clause {
        Clause::Any => Vec::new(),
        Clause::One(value) => vec![value.to_vec()],
        Clause::Or(values) => values.iter().map(|value| value.to_vec()).collect(),
    }
}
