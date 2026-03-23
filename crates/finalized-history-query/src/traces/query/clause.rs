use crate::core::clause::Clause;
use crate::core::ids::{TraceLocalId, TraceShard};
use crate::error::Result;
use crate::kernel::sharded_streams::overlaps;
use crate::store::traits::{BlobStore, MetaStore};
use crate::streams::decode_bitmap_blob;
use crate::tables::Tables;
use crate::traces::filter::TraceFilter;
use crate::traces::keys::{
    MAX_TRACE_LOCAL_ID, TRACE_STREAM_PAGE_LOCAL_ID_SPAN, has_value_stream_id,
};
use crate::traces::table_specs;

use super::stream_bitmap::load_trace_bitmap_page_meta;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::traces) enum ClauseKind {
    From,
    To,
    Selector,
    HasValue,
}

#[derive(Debug, Clone)]
pub(in crate::traces) struct IndexedClauseSpec {
    pub(in crate::traces) kind: ClauseKind,
    pub(in crate::traces) stream_kind: &'static str,
    pub(in crate::traces) values: Vec<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub(in crate::traces) struct PreparedShardClause {
    pub(in crate::traces) kind: ClauseKind,
    pub(in crate::traces) stream_ids: Vec<String>,
    pub(in crate::traces) estimated_count: u64,
}

pub(crate) fn is_too_broad(filter: &TraceFilter, max_or_terms: usize) -> bool {
    filter.max_or_terms() > max_or_terms
}

pub(crate) fn is_full_shard_range(local_from: u32, local_to: u32) -> bool {
    local_from == 0 && local_to == MAX_TRACE_LOCAL_ID
}

pub(in crate::traces) fn build_clause_specs(filter: &TraceFilter) -> Vec<IndexedClauseSpec> {
    let mut clauses = Vec::new();

    if let Some(clause) = &filter.from {
        let values = clause_values_20(clause);
        if !values.is_empty() {
            clauses.push(IndexedClauseSpec {
                kind: ClauseKind::From,
                stream_kind: "from",
                values,
            });
        }
    }

    if let Some(clause) = &filter.to {
        let values = clause_values_20(clause);
        if !values.is_empty() {
            clauses.push(IndexedClauseSpec {
                kind: ClauseKind::To,
                stream_kind: "to",
                values,
            });
        }
    }

    if let Some(clause) = &filter.selector {
        let values = clause_values_4(clause);
        if !values.is_empty() {
            clauses.push(IndexedClauseSpec {
                kind: ClauseKind::Selector,
                stream_kind: "selector",
                values,
            });
        }
    }

    if filter.has_value == Some(true) {
        clauses.push(IndexedClauseSpec {
            kind: ClauseKind::HasValue,
            stream_kind: "has_value",
            values: vec![vec![1]],
        });
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
        prepared
            .push(prepare_shard_clause(tables, clause_spec, shard, local_from, local_to).await?);
    }

    prepared.sort_by_key(|clause| (clause.estimated_count, clause_kind_rank(clause.kind)));
    Ok(prepared)
}

async fn prepare_shard_clause<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    clause_spec: &IndexedClauseSpec,
    shard: TraceShard,
    local_from: TraceLocalId,
    local_to: TraceLocalId,
) -> Result<PreparedShardClause> {
    let mut stream_ids = Vec::with_capacity(clause_spec.values.len());
    let mut estimated_count = 0u64;

    for value in &clause_spec.values {
        let stream_id = match clause_spec.kind {
            ClauseKind::HasValue => has_value_stream_id(shard),
            _ => crate::traces::keys::stream_id(clause_spec.stream_kind, value, shard),
        };
        estimated_count = estimated_count.saturating_add(
            estimate_stream_overlap(tables, &stream_id, local_from.get(), local_to.get()).await?,
        );
        stream_ids.push(stream_id);
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
        if let Some(meta) = load_trace_bitmap_page_meta(tables, stream_id, page_start).await? {
            if overlaps(meta.min_local, meta.max_local, local_from, local_to) {
                estimated = estimated.saturating_add(u64::from(meta.count));
            }
        } else {
            for bytes in tables
                .trace_bitmap_by_block()
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
        page_start = page_start.saturating_add(TRACE_STREAM_PAGE_LOCAL_ID_SPAN);
    }

    Ok(estimated)
}

fn clause_kind_rank(kind: ClauseKind) -> u8 {
    match kind {
        ClauseKind::From => 0,
        ClauseKind::To => 1,
        ClauseKind::Selector => 2,
        ClauseKind::HasValue => 3,
    }
}

fn clause_values_20(clause: &Clause<[u8; 20]>) -> Vec<Vec<u8>> {
    match clause {
        Clause::Any => Vec::new(),
        Clause::One(value) => vec![value.to_vec()],
        Clause::Or(values) => values.iter().map(|value| value.to_vec()).collect(),
    }
}

fn clause_values_4(clause: &Clause<[u8; 4]>) -> Vec<Vec<u8>> {
    match clause {
        Clause::Any => Vec::new(),
        Clause::One(value) => vec![value.to_vec()],
        Clause::Or(values) => values.iter().map(|value| value.to_vec()).collect(),
    }
}
