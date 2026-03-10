use crate::core::clause::Clause;
use crate::core::ids::LogId;
use crate::domain::keys::{
    MAX_LOCAL_ID, local_range_for_shard, log_shard, manifest_key, stream_id, tail_key,
};
use crate::error::Result;
use crate::logs::filter::LogFilter;
use crate::store::traits::MetaStore;
use crate::streams::manifest::{ChunkRef, decode_manifest, decode_tail};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClauseKind {
    Address,
    Topic0,
    Topic1,
    Topic2,
    Topic3,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ClauseEstimate {
    pub estimated: u64,
}

pub async fn build_clause_order<M: MetaStore>(
    meta_store: &M,
    filter: &LogFilter,
    from_log_id: LogId,
    to_log_id_inclusive: LogId,
) -> Result<Vec<ClauseKind>> {
    let mut clauses = Vec::<(ClauseKind, ClauseEstimate)>::new();

    if let Some(clause) = &filter.address {
        let values = clause_values_20(clause);
        if !values.is_empty() {
            let estimated = estimate_for_values(
                meta_store,
                "addr",
                &values,
                from_log_id,
                to_log_id_inclusive,
            )
            .await?;
            clauses.push((ClauseKind::Address, ClauseEstimate { estimated }));
        }
    }

    if let Some(clause) = &filter.topic1 {
        let values = clause_values_32(clause);
        if !values.is_empty() {
            let estimated = estimate_for_values(
                meta_store,
                "topic1",
                &values,
                from_log_id,
                to_log_id_inclusive,
            )
            .await?;
            clauses.push((ClauseKind::Topic1, ClauseEstimate { estimated }));
        }
    }

    if let Some(clause) = &filter.topic2 {
        let values = clause_values_32(clause);
        if !values.is_empty() {
            let estimated = estimate_for_values(
                meta_store,
                "topic2",
                &values,
                from_log_id,
                to_log_id_inclusive,
            )
            .await?;
            clauses.push((ClauseKind::Topic2, ClauseEstimate { estimated }));
        }
    }

    if let Some(clause) = &filter.topic3 {
        let values = clause_values_32(clause);
        if !values.is_empty() {
            let estimated = estimate_for_values(
                meta_store,
                "topic3",
                &values,
                from_log_id,
                to_log_id_inclusive,
            )
            .await?;
            clauses.push((ClauseKind::Topic3, ClauseEstimate { estimated }));
        }
    }

    if let Some(clause) = &filter.topic0 {
        let values = clause_values_32(clause);
        if !values.is_empty() {
            let estimated = estimate_for_values(
                meta_store,
                "topic0",
                &values,
                from_log_id,
                to_log_id_inclusive,
            )
            .await?;
            clauses.push((ClauseKind::Topic0, ClauseEstimate { estimated }));
        }
    }

    clauses.sort_by_key(|(_, estimate)| estimate.estimated);
    Ok(clauses.into_iter().map(|(kind, _)| kind).collect())
}

pub fn is_too_broad(filter: &LogFilter, max_or_terms: usize) -> bool {
    filter.max_or_terms() > max_or_terms
}

async fn estimate_for_values<M: MetaStore>(
    meta_store: &M,
    kind: &str,
    values: &[Vec<u8>],
    from_log_id: LogId,
    to_log_id_inclusive: LogId,
) -> Result<u64> {
    let from_shard = log_shard(from_log_id);
    let to_shard = log_shard(to_log_id_inclusive);
    let mut sum = 0u64;

    for value in values {
        for shard_raw in from_shard.get()..=to_shard.get() {
            let shard =
                crate::core::ids::LogShard::new(shard_raw).expect("shard derived from LogId range");
            let stream = stream_id(kind, value, shard);
            sum = sum.saturating_add(
                estimate_stream_overlap(
                    meta_store,
                    &stream,
                    from_log_id,
                    to_log_id_inclusive,
                    shard,
                )
                .await?,
            );
        }
    }

    Ok(sum)
}

async fn estimate_stream_overlap<M: MetaStore>(
    meta_store: &M,
    stream_id: &str,
    from_log_id: LogId,
    to_log_id_inclusive: LogId,
    shard: crate::core::ids::LogShard,
) -> Result<u64> {
    let (local_from, local_to) = local_range_for_shard(from_log_id, to_log_id_inclusive, shard);
    let mut count = 0u64;

    if let Some(record) = meta_store.get(&manifest_key(stream_id)).await? {
        let manifest = decode_manifest(&record.value)?;
        if is_full_shard_range(local_from.get(), local_to.get()) {
            count = count.saturating_add(manifest.approx_count);
        } else {
            for chunk_ref in
                overlapping_chunk_refs(&manifest.chunk_refs, local_from.get(), local_to.get())
            {
                count = count.saturating_add(chunk_ref.count as u64);
            }
        }
    }

    if let Some(record) = meta_store.get(&tail_key(stream_id)).await? {
        let tail = decode_tail(&record.value)?;
        for value in &tail {
            if is_full_shard_range(local_from.get(), local_to.get())
                || (value >= local_from.get() && value <= local_to.get())
            {
                count = count.saturating_add(1);
            }
        }
    }

    Ok(count)
}

pub(crate) fn overlapping_chunk_refs(
    chunk_refs: &[ChunkRef],
    local_from: u32,
    local_to: u32,
) -> &[ChunkRef] {
    if is_full_shard_range(local_from, local_to) {
        return chunk_refs;
    }
    let start = chunk_refs.partition_point(|chunk_ref| chunk_ref.max_local < local_from);
    let end = chunk_refs.partition_point(|chunk_ref| chunk_ref.min_local <= local_to);
    &chunk_refs[start..end]
}

pub(crate) fn is_full_shard_range(local_from: u32, local_to: u32) -> bool {
    local_from == 0 && local_to == MAX_LOCAL_ID
}

pub fn clause_values_20(clause: &Clause<[u8; 20]>) -> Vec<Vec<u8>> {
    match clause {
        Clause::Any => Vec::new(),
        Clause::One(value) => vec![value.to_vec()],
        Clause::Or(values) => values.iter().map(|value| value.to_vec()).collect(),
    }
}

pub fn clause_values_32(clause: &Clause<[u8; 32]>) -> Vec<Vec<u8>> {
    match clause {
        Clause::Any => Vec::new(),
        Clause::One(value) => vec![value.to_vec()],
        Clause::Or(values) => values.iter().map(|value| value.to_vec()).collect(),
    }
}
