use crate::core::clause::Clause;
use crate::error::Result;
use crate::kernel::sharded_streams::overlaps;
use crate::store::traits::{BlobStore, MetaStore};
use crate::streams::decode_bitmap_blob;
use crate::tables::Tables;

use super::stream_family::StreamIndexFamily;

#[derive(Debug, Clone)]
pub(crate) struct StreamSelector {
    pub stream_kind: &'static str,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone)]
pub(crate) struct IndexedClause<K> {
    pub kind: K,
    pub selectors: Vec<StreamSelector>,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedClause<K> {
    pub kind: K,
    pub stream_ids: Vec<String>,
    pub estimated_count: u64,
}

pub(crate) async fn prepare_shard_clauses<M: MetaStore, B: BlobStore, F: StreamIndexFamily>(
    tables: &Tables<M, B>,
    clause_specs: &[IndexedClause<F::ClauseKind>],
    shard: F::Shard,
    local_from: u32,
    local_to: u32,
) -> Result<Vec<PreparedClause<F::ClauseKind>>> {
    let mut prepared = Vec::with_capacity(clause_specs.len());

    for clause_spec in clause_specs {
        let mut stream_ids = Vec::with_capacity(clause_spec.selectors.len());
        let mut estimated_count = 0u64;

        for selector in &clause_spec.selectors {
            let stream_id = F::stream_id(selector, shard);
            estimated_count = estimated_count.saturating_add(
                estimate_stream_overlap::<M, B, F>(tables, &stream_id, local_from, local_to)
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

    prepared.sort_by_key(|clause| (clause.estimated_count, F::clause_sort_rank(clause.kind)));
    Ok(prepared)
}

async fn estimate_stream_overlap<M: MetaStore, B: BlobStore, F: StreamIndexFamily>(
    tables: &Tables<M, B>,
    stream_id: &str,
    local_from: u32,
    local_to: u32,
) -> Result<u64> {
    let mut estimated = 0u64;
    let mut page_start = F::first_page_start(local_from);
    let last_page_start = F::last_page_start(local_to);
    let stream_tables = F::stream_tables(tables);

    loop {
        if let Some(meta) = stream_tables.get_page_meta(stream_id, page_start).await? {
            if F::meta_overlaps(&meta, local_from, local_to) {
                estimated = estimated.saturating_add(u64::from(F::meta_count(&meta)));
            }
        } else {
            for bytes in stream_tables
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
        page_start = F::next_page_start(page_start);
    }

    Ok(estimated)
}

pub(crate) fn clause_values<T>(clause: &Clause<T>) -> Vec<Vec<u8>>
where
    T: Copy + Into<Vec<u8>>,
{
    match clause {
        Clause::Any => Vec::new(),
        Clause::One(value) => vec![(*value).into()],
        Clause::Or(values) => values.iter().copied().map(Into::into).collect(),
    }
}
