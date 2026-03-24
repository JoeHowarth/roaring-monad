use crate::core::clause::Clause;
use crate::core::ids::{TraceLocalId, TraceShard};
use crate::error::Result;
use crate::query::planner::{
    IndexedClause, PreparedClause, StreamPlanner, StreamSelector, clause_values,
    prepare_shard_clauses as prepare_query_shard_clauses,
};
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;
use crate::traces::filter::TraceFilter;
use crate::traces::keys::{
    MAX_TRACE_LOCAL_ID, TRACE_STREAM_PAGE_LOCAL_ID_SPAN, has_value_stream_id,
};
use crate::traces::table_specs;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::traces) enum ClauseKind {
    From,
    To,
    Selector,
    HasValue,
}

#[derive(Debug, Clone)]
pub(in crate::traces) struct IndexedClauseSpec {
    pub(in crate::traces) inner: IndexedClause<ClauseKind>,
}

pub(in crate::traces) type PreparedShardClause = PreparedClause<ClauseKind>;

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
                inner: IndexedClause {
                    kind: ClauseKind::From,
                    selectors: values
                        .into_iter()
                        .map(|value| StreamSelector {
                            stream_kind: "from",
                            value,
                        })
                        .collect(),
                },
            });
        }
    }

    if let Some(clause) = &filter.to {
        let values = clause_values_20(clause);
        if !values.is_empty() {
            clauses.push(IndexedClauseSpec {
                inner: IndexedClause {
                    kind: ClauseKind::To,
                    selectors: values
                        .into_iter()
                        .map(|value| StreamSelector {
                            stream_kind: "to",
                            value,
                        })
                        .collect(),
                },
            });
        }
    }

    if let Some(clause) = &filter.selector {
        let values = clause_values_4(clause);
        if !values.is_empty() {
            clauses.push(IndexedClauseSpec {
                inner: IndexedClause {
                    kind: ClauseKind::Selector,
                    selectors: values
                        .into_iter()
                        .map(|value| StreamSelector {
                            stream_kind: "selector",
                            value,
                        })
                        .collect(),
                },
            });
        }
    }

    if filter.has_value == Some(true) {
        clauses.push(IndexedClauseSpec {
            inner: IndexedClause {
                kind: ClauseKind::HasValue,
                selectors: vec![StreamSelector {
                    stream_kind: "has_value",
                    value: vec![1],
                }],
            },
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
    let planner = TracesStreamPlanner;
    let shared_specs = clause_specs
        .iter()
        .map(|spec| spec.inner.clone())
        .collect::<Vec<_>>();
    prepare_query_shard_clauses(
        tables,
        &planner,
        &shared_specs,
        shard,
        local_from.get(),
        local_to.get(),
    )
    .await
}

fn clause_kind_rank(kind: ClauseKind) -> u8 {
    match kind {
        ClauseKind::From => 0,
        ClauseKind::To => 1,
        ClauseKind::Selector => 2,
        ClauseKind::HasValue => 3,
    }
}

struct TracesStreamPlanner;

impl StreamPlanner for TracesStreamPlanner {
    type Shard = TraceShard;
    type ClauseKind = ClauseKind;
    type BitmapMeta = crate::traces::types::StreamBitmapMeta;

    fn stream_id(&self, selector: &StreamSelector, shard: Self::Shard) -> String {
        match selector.stream_kind {
            "has_value" => has_value_stream_id(shard),
            _ => crate::traces::keys::stream_id(selector.stream_kind, &selector.value, shard),
        }
    }

    fn clause_sort_rank(&self, kind: Self::ClauseKind) -> u8 {
        clause_kind_rank(kind)
    }

    fn first_page_start(&self, local_from: u32) -> u32 {
        table_specs::stream_page_start_local(local_from)
    }

    fn last_page_start(&self, local_to: u32) -> u32 {
        table_specs::stream_page_start_local(local_to)
    }

    fn next_page_start(&self, page_start: u32) -> u32 {
        page_start.saturating_add(TRACE_STREAM_PAGE_LOCAL_ID_SPAN)
    }

    fn meta_overlaps(&self, meta: &Self::BitmapMeta, local_from: u32, local_to: u32) -> bool {
        crate::kernel::sharded_streams::overlaps(
            meta.min_local,
            meta.max_local,
            local_from,
            local_to,
        )
    }

    fn meta_count(&self, meta: &Self::BitmapMeta) -> u32 {
        meta.count
    }

    async fn load_bitmap_page_meta<M: MetaStore, B: BlobStore>(
        &self,
        tables: &Tables<M, B>,
        stream_id: &str,
        page_start: u32,
    ) -> Result<Option<Self::BitmapMeta>> {
        super::stream_bitmap::load_trace_bitmap_page_meta(tables, stream_id, page_start).await
    }

    async fn load_page_fragments<M: MetaStore, B: BlobStore>(
        &self,
        tables: &Tables<M, B>,
        stream_id: &str,
        page_start: u32,
    ) -> Result<Vec<bytes::Bytes>> {
        tables
            .trace_streams()
            .load_page_fragments(stream_id, page_start)
            .await
    }
}

fn clause_values_20(clause: &Clause<[u8; 20]>) -> Vec<Vec<u8>> {
    clause_values(clause)
}

fn clause_values_4(clause: &Clause<[u8; 4]>) -> Vec<Vec<u8>> {
    clause_values(clause)
}
