use crate::cache::{BytesCache, NoopBytesCache};
use crate::codec::log::decode_stream_bitmap_meta;
use crate::core::execution::ShardBitmapSet;
use crate::core::ids::{LogId, LogLocalId, LogShard};
use crate::domain::keys::{
    STREAM_PAGE_LOCAL_ID_SPAN, bitmap_by_block_meta_prefix, stream_id, stream_page_start_local,
};
use crate::error::Result;
use crate::logs::filter::LogFilter;
use crate::logs::index_spec::{ClauseKind, clause_values_20, clause_values_32};
use crate::store::traits::{BlobStore, MetaStore};

use super::stream_bitmap::{fetch_union_log_level_with_cache, load_bitmap_page_meta, overlaps};

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

pub(in crate::logs) async fn prepare_shard_clauses<M: MetaStore>(
    meta_store: &M,
    cache: &impl BytesCache,
    clause_specs: &[IndexedClauseSpec],
    shard: LogShard,
    local_from: LogLocalId,
    local_to: LogLocalId,
) -> Result<Vec<PreparedShardClause>> {
    let mut prepared = Vec::with_capacity(clause_specs.len());

    for clause_spec in clause_specs {
        let clause =
            prepare_shard_clause(meta_store, cache, clause_spec, shard, local_from, local_to)
                .await?;
        prepared.push(clause);
    }

    prepared.sort_by_key(|clause| (clause.estimated_count, clause_kind_rank(clause.kind)));
    Ok(prepared)
}

async fn prepare_shard_clause<M: MetaStore>(
    meta_store: &M,
    cache: &impl BytesCache,
    clause_spec: &IndexedClauseSpec,
    shard: LogShard,
    local_from: LogLocalId,
    local_to: LogLocalId,
) -> Result<PreparedShardClause> {
    let mut stream_ids = Vec::with_capacity(clause_spec.values.len());
    let mut estimated_count = 0u64;

    for value in &clause_spec.values {
        let stream = stream_id(clause_spec.stream_kind, value, shard);
        estimated_count = estimated_count.saturating_add(
            estimate_stream_overlap(meta_store, cache, &stream, local_from.get(), local_to.get())
                .await?,
        );
        stream_ids.push(stream);
    }

    Ok(PreparedShardClause {
        kind: clause_spec.kind,
        stream_ids,
        estimated_count,
    })
}

async fn estimate_stream_overlap<M: MetaStore>(
    meta_store: &M,
    cache: &impl BytesCache,
    stream_id: &str,
    local_from: u32,
    local_to: u32,
) -> Result<u64> {
    let mut estimated = 0u64;
    let mut page_start = stream_page_start_local(local_from);
    let last_page_start = stream_page_start_local(local_to);

    loop {
        if let Some(meta) = load_bitmap_page_meta(meta_store, cache, stream_id, page_start).await? {
            if overlaps(meta.min_local, meta.max_local, local_from, local_to) {
                estimated = estimated.saturating_add(u64::from(meta.count));
            }
        } else {
            let page = meta_store
                .list_prefix(
                    &bitmap_by_block_meta_prefix(stream_id, page_start),
                    None,
                    usize::MAX,
                )
                .await?;
            for key in page.keys {
                let Some(record) = meta_store.get(&key).await? else {
                    continue;
                };
                let meta = decode_stream_bitmap_meta(&record.value)?;
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

pub(in crate::logs) fn clause_kind_rank(kind: ClauseKind) -> u8 {
    match kind {
        ClauseKind::Address => 0,
        ClauseKind::Topic1 => 1,
        ClauseKind::Topic2 => 2,
        ClauseKind::Topic3 => 3,
        ClauseKind::Topic0 => 4,
    }
}

async fn load_clause_sets<M: MetaStore, B: BlobStore>(
    meta_store: &M,
    blob_store: &B,
    clause_specs: &[IndexedClauseSpec],
    from_log_id: LogId,
    to_log_id_inclusive: LogId,
) -> Result<Vec<ShardBitmapSet>> {
    load_clause_sets_with_cache(
        meta_store,
        blob_store,
        &NoopBytesCache,
        clause_specs,
        from_log_id,
        to_log_id_inclusive,
    )
    .await
}

async fn load_clause_sets_with_cache<M: MetaStore, B: BlobStore, C: BytesCache>(
    meta_store: &M,
    blob_store: &B,
    cache: &C,
    clause_specs: &[IndexedClauseSpec],
    from_log_id: LogId,
    to_log_id_inclusive: LogId,
) -> Result<Vec<ShardBitmapSet>> {
    let mut clause_sets = Vec::new();

    for clause_spec in clause_specs {
        let set = fetch_union_log_level_with_cache(
            meta_store,
            blob_store,
            cache,
            clause_spec.stream_kind,
            &clause_spec.values,
            from_log_id,
            to_log_id_inclusive,
        )
        .await?;
        clause_sets.push(set);
    }

    Ok(clause_sets)
}

#[doc(hidden)]
pub async fn load_clause_sets_for_benchmark<M: MetaStore, B: BlobStore>(
    meta_store: &M,
    blob_store: &B,
    filter: &LogFilter,
    from_log_id: LogId,
    to_log_id_inclusive: LogId,
) -> Result<Vec<ShardBitmapSet>> {
    load_clause_sets(
        meta_store,
        blob_store,
        &build_clause_specs(filter),
        from_log_id,
        to_log_id_inclusive,
    )
    .await
}
