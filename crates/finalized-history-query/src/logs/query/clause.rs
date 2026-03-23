use crate::core::clause::Clause;
use crate::core::ids::{LogLocalId, LogShard};
use crate::core::layout::MAX_LOCAL_ID;
use crate::error::Result;
use crate::logs::filter::LogFilter;
use crate::logs::keys::STREAM_PAGE_LOCAL_ID_SPAN;
use crate::logs::table_specs;
use crate::query::planner::{
    IndexedClause, PreparedClause, StreamPlanner, StreamSelector, clause_values,
    prepare_shard_clauses as prepare_query_shard_clauses,
};
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;

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
    pub(in crate::logs) inner: IndexedClause<ClauseKind>,
}

pub(in crate::logs) type PreparedShardClause = PreparedClause<ClauseKind>;

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
                inner: IndexedClause {
                    kind: ClauseKind::Address,
                    selectors: values
                        .into_iter()
                        .map(|value| StreamSelector {
                            stream_kind: "addr",
                            value,
                        })
                        .collect(),
                },
            });
        }
    }

    if let Some(clause) = &filter.topic1 {
        let values = clause_values_32(clause);
        if !values.is_empty() {
            clauses.push(IndexedClauseSpec {
                inner: IndexedClause {
                    kind: ClauseKind::Topic1,
                    selectors: values
                        .into_iter()
                        .map(|value| StreamSelector {
                            stream_kind: "topic1",
                            value,
                        })
                        .collect(),
                },
            });
        }
    }

    if let Some(clause) = &filter.topic2 {
        let values = clause_values_32(clause);
        if !values.is_empty() {
            clauses.push(IndexedClauseSpec {
                inner: IndexedClause {
                    kind: ClauseKind::Topic2,
                    selectors: values
                        .into_iter()
                        .map(|value| StreamSelector {
                            stream_kind: "topic2",
                            value,
                        })
                        .collect(),
                },
            });
        }
    }

    if let Some(clause) = &filter.topic3 {
        let values = clause_values_32(clause);
        if !values.is_empty() {
            clauses.push(IndexedClauseSpec {
                inner: IndexedClause {
                    kind: ClauseKind::Topic3,
                    selectors: values
                        .into_iter()
                        .map(|value| StreamSelector {
                            stream_kind: "topic3",
                            value,
                        })
                        .collect(),
                },
            });
        }
    }

    if let Some(clause) = &filter.topic0 {
        let values = clause_values_32(clause);
        if !values.is_empty() {
            clauses.push(IndexedClauseSpec {
                inner: IndexedClause {
                    kind: ClauseKind::Topic0,
                    selectors: values
                        .into_iter()
                        .map(|value| StreamSelector {
                            stream_kind: "topic0",
                            value,
                        })
                        .collect(),
                },
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
    let planner = LogsStreamPlanner;
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
        ClauseKind::Address => 0,
        ClauseKind::Topic1 => 1,
        ClauseKind::Topic2 => 2,
        ClauseKind::Topic3 => 3,
        ClauseKind::Topic0 => 4,
    }
}

struct LogsStreamPlanner;

impl StreamPlanner for LogsStreamPlanner {
    type Shard = LogShard;
    type ClauseKind = ClauseKind;
    type BitmapMeta = crate::logs::types::StreamBitmapMeta;

    fn stream_id(&self, selector: &StreamSelector, shard: Self::Shard) -> String {
        table_specs::stream_id(selector.stream_kind, &selector.value, shard)
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
        page_start.saturating_add(STREAM_PAGE_LOCAL_ID_SPAN)
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
        super::stream_bitmap::load_bitmap_page_meta(tables, stream_id, page_start).await
    }

    async fn load_page_fragments<M: MetaStore, B: BlobStore>(
        &self,
        tables: &Tables<M, B>,
        stream_id: &str,
        page_start: u32,
    ) -> Result<Vec<bytes::Bytes>> {
        tables
            .bitmap_by_block()
            .load_page_fragments(stream_id, page_start)
            .await
    }
}

fn clause_values_20(clause: &Clause<[u8; 20]>) -> Vec<Vec<u8>> {
    clause_values(clause)
}

fn clause_values_32(clause: &Clause<[u8; 32]>) -> Vec<Vec<u8>> {
    clause_values(clause)
}
