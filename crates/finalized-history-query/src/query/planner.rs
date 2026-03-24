use crate::core::clause::Clause;
use crate::error::Result;
use crate::kernel::sharded_streams::sharded_stream_id;
use crate::query::bitmap;
use crate::store::traits::{BlobStore, MetaStore};
use crate::streams::StreamBitmapMeta;
use crate::tables::StreamTables;

#[derive(Debug, Clone)]
pub(crate) struct StreamSelector {
    pub stream_kind: &'static str,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone)]
pub(crate) struct IndexedClause {
    pub selectors: Vec<StreamSelector>,
}

pub(crate) fn indexed_clause(
    stream_kind: &'static str,
    values: Vec<Vec<u8>>,
) -> Option<IndexedClause> {
    (!values.is_empty()).then(|| IndexedClause {
        selectors: values
            .into_iter()
            .map(|value| StreamSelector { stream_kind, value })
            .collect(),
    })
}

pub(crate) fn build_indexed_clause<T>(
    stream_kind: &'static str,
    clause: &Option<Clause<T>>,
) -> Option<IndexedClause>
where
    T: Copy + Into<Vec<u8>>,
{
    clause
        .as_ref()
        .and_then(|clause| indexed_clause(stream_kind, clause.indexed_values()))
}

pub(crate) fn single_selector_clause(stream_kind: &'static str, value: Vec<u8>) -> IndexedClause {
    IndexedClause {
        selectors: vec![StreamSelector { stream_kind, value }],
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedClause {
    pub stream_ids: Vec<String>,
    pub estimated_count: u64,
}

pub(crate) async fn prepare_shard_clauses<M: MetaStore, B: BlobStore>(
    stream_tables: &StreamTables<M, B, StreamBitmapMeta>,
    clause_specs: &[IndexedClause],
    shard_raw: u64,
    local_from: u32,
    local_to: u32,
) -> Result<Vec<PreparedClause>> {
    let mut prepared = Vec::with_capacity(clause_specs.len());
    for clause_spec in clause_specs {
        let mut stream_ids = Vec::with_capacity(clause_spec.selectors.len());
        let mut estimated_count = 0u64;

        for selector in &clause_spec.selectors {
            let stream_id = sharded_stream_id(selector.stream_kind, &selector.value, shard_raw);
            estimated_count = estimated_count.saturating_add(
                bitmap::estimate_stream_overlap(stream_tables, &stream_id, local_from, local_to)
                    .await?,
            );
            stream_ids.push(stream_id);
        }

        prepared.push(PreparedClause {
            stream_ids,
            estimated_count,
        });
    }

    prepared.sort_by(|left, right| {
        left.estimated_count
            .cmp(&right.estimated_count)
            .then_with(|| left.stream_ids.cmp(&right.stream_ids))
    });
    Ok(prepared)
}
