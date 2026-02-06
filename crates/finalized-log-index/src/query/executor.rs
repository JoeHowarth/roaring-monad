use std::collections::{BTreeSet, HashMap};

use crate::codec::chunk::decode_chunk;
use crate::codec::log::{decode_block_meta, decode_log, decode_log_locator};
use crate::codec::manifest::{ChunkRef, decode_manifest, decode_tail};
use crate::domain::filter::{Clause, LogFilter};
use crate::domain::keys::{
    block_meta_key, chunk_blob_key, log_locator_key, manifest_key, stream_id, tail_key,
};
use crate::domain::types::{Log, Topic32};
use crate::error::{Error, Result};
use crate::query::planner::{ClauseKind, QueryPlan, clause_values_20, clause_values_32};
use crate::store::traits::{BlobStore, MetaStore};

pub async fn execute_plan<M: MetaStore, B: BlobStore>(
    meta_store: &M,
    blob_store: &B,
    plan: QueryPlan,
) -> Result<Vec<Log>> {
    let mut log_pack_cache: HashMap<Vec<u8>, bytes::Bytes> = HashMap::new();
    if plan.force_block_scan {
        return execute_block_scan(meta_store, blob_store, &plan, &mut log_pack_cache).await;
    }

    let mut clause_sets: Vec<BTreeSet<u64>> = Vec::new();
    for kind in &plan.clause_order {
        let set = match kind {
            ClauseKind::Address => {
                let Some(clause) = &plan.filter.address else {
                    continue;
                };
                let values = clause_values_20(clause);
                fetch_union_log_level(
                    meta_store,
                    blob_store,
                    "addr",
                    &values,
                    plan.from_log_id,
                    plan.to_log_id_inclusive,
                )
                .await?
            }
            ClauseKind::Topic1 => {
                let Some(clause) = &plan.filter.topic1 else {
                    continue;
                };
                let values = clause_values_32(clause);
                fetch_union_log_level(
                    meta_store,
                    blob_store,
                    "topic1",
                    &values,
                    plan.from_log_id,
                    plan.to_log_id_inclusive,
                )
                .await?
            }
            ClauseKind::Topic2 => {
                let Some(clause) = &plan.filter.topic2 else {
                    continue;
                };
                let values = clause_values_32(clause);
                fetch_union_log_level(
                    meta_store,
                    blob_store,
                    "topic2",
                    &values,
                    plan.from_log_id,
                    plan.to_log_id_inclusive,
                )
                .await?
            }
            ClauseKind::Topic3 => {
                let Some(clause) = &plan.filter.topic3 else {
                    continue;
                };
                let values = clause_values_32(clause);
                fetch_union_log_level(
                    meta_store,
                    blob_store,
                    "topic3",
                    &values,
                    plan.from_log_id,
                    plan.to_log_id_inclusive,
                )
                .await?
            }
            ClauseKind::Topic0Log => {
                let Some(clause) = &plan.filter.topic0 else {
                    continue;
                };
                let values = clause_values_32(clause);
                fetch_union_log_level(
                    meta_store,
                    blob_store,
                    "topic0_log",
                    &values,
                    plan.from_log_id,
                    plan.to_log_id_inclusive,
                )
                .await?
            }
        };
        clause_sets.push(set);
    }

    let candidates = intersect_sets(clause_sets, plan.from_log_id, plan.to_log_id_inclusive);

    let topic0_blocks = if let Some(topic_clause) = &plan.filter.topic0 {
        let values = clause_values_32(topic_clause);
        if values.is_empty() {
            None
        } else {
            Some(
                fetch_union_block_level(
                    meta_store,
                    blob_store,
                    "topic0_block",
                    &values,
                    plan.clipped_from_block,
                    plan.clipped_to_block,
                )
                .await?,
            )
        }
    } else {
        None
    };

    let mut out = Vec::new();
    for id in candidates {
        let Some(log) = load_log_by_id(meta_store, blob_store, id, &mut log_pack_cache).await?
        else {
            continue;
        };

        if log.block_num < plan.clipped_from_block || log.block_num > plan.clipped_to_block {
            continue;
        }

        if let Some(blocks) = &topic0_blocks
            && !blocks.contains(&(log.block_num as u32))
        {
            continue;
        }

        if exact_match(&log, &plan.filter) {
            out.push(log);
            if let Some(max) = plan.options.max_results
                && out.len() >= max
            {
                break;
            }
        }
    }

    out.sort_by_key(|l| (l.block_num, l.tx_idx, l.log_idx));
    Ok(out)
}

async fn execute_block_scan<M: MetaStore, B: BlobStore>(
    meta_store: &M,
    blob_store: &B,
    plan: &QueryPlan,
    log_pack_cache: &mut HashMap<Vec<u8>, bytes::Bytes>,
) -> Result<Vec<Log>> {
    let mut out = Vec::new();

    for b in plan.clipped_from_block..=plan.clipped_to_block {
        let Some(m) = meta_store.get(&block_meta_key(b)).await? else {
            continue;
        };
        let meta = decode_block_meta(&m.value)?;
        let start = meta.first_log_id;
        let end = start + meta.count as u64;

        for id in start..end {
            let Some(log) = load_log_by_id(meta_store, blob_store, id, log_pack_cache).await?
            else {
                continue;
            };
            if exact_match(&log, &plan.filter) {
                out.push(log);
                if let Some(max) = plan.options.max_results
                    && out.len() >= max
                {
                    out.sort_by_key(|l| (l.block_num, l.tx_idx, l.log_idx));
                    return Ok(out);
                }
            }
        }
    }

    out.sort_by_key(|l| (l.block_num, l.tx_idx, l.log_idx));
    Ok(out)
}

async fn load_log_by_id<M: MetaStore, B: BlobStore>(
    meta_store: &M,
    blob_store: &B,
    log_id: u64,
    log_pack_cache: &mut HashMap<Vec<u8>, bytes::Bytes>,
) -> Result<Option<Log>> {
    let locator_rec = match meta_store.get(&log_locator_key(log_id)).await? {
        Some(r) => r,
        None => return Ok(None),
    };
    let locator = decode_log_locator(&locator_rec.value)?;
    if !log_pack_cache.contains_key(&locator.blob_key) {
        let Some(blob) = blob_store.get_blob(&locator.blob_key).await? else {
            return Ok(None);
        };
        log_pack_cache.insert(locator.blob_key.clone(), blob);
    }
    let Some(blob) = log_pack_cache.get(&locator.blob_key) else {
        return Ok(None);
    };
    let start = locator.byte_offset as usize;
    let end = start.saturating_add(locator.byte_len as usize);
    if end > blob.len() || start > end {
        return Err(Error::Decode("invalid log locator span"));
    }
    let log = decode_log(&blob[start..end])?;
    Ok(Some(log))
}

fn intersect_sets(sets: Vec<BTreeSet<u64>>, from: u64, to_inclusive: u64) -> BTreeSet<u64> {
    if sets.is_empty() {
        return (from..=to_inclusive).collect();
    }

    let mut it = sets.into_iter();
    let mut acc = it.next().unwrap_or_default();
    for s in it {
        acc = acc.intersection(&s).copied().collect();
        if acc.is_empty() {
            break;
        }
    }
    acc
}

fn exact_match(log: &Log, filter: &LogFilter) -> bool {
    if !match_address(&log.address, &filter.address) {
        return false;
    }

    let t = &log.topics;
    if !match_topic(t.first().copied(), &filter.topic0) {
        return false;
    }
    if !match_topic(t.get(1).copied(), &filter.topic1) {
        return false;
    }
    if !match_topic(t.get(2).copied(), &filter.topic2) {
        return false;
    }
    if !match_topic(t.get(3).copied(), &filter.topic3) {
        return false;
    }
    true
}

fn match_address(addr: &[u8; 20], clause: &Option<Clause<[u8; 20]>>) -> bool {
    match clause {
        None => true,
        Some(Clause::Any) => true,
        Some(Clause::One(v)) => v == addr,
        Some(Clause::Or(vs)) => vs.iter().any(|v| v == addr),
    }
}

fn match_topic(topic: Option<Topic32>, clause: &Option<Clause<Topic32>>) -> bool {
    match clause {
        None => true,
        Some(Clause::Any) => true,
        Some(Clause::One(v)) => topic.as_ref() == Some(v),
        Some(Clause::Or(vs)) => topic
            .as_ref()
            .map(|t| vs.iter().any(|v| v == t))
            .unwrap_or(false),
    }
}

async fn fetch_union_log_level<M: MetaStore, B: BlobStore>(
    meta_store: &M,
    blob_store: &B,
    kind: &str,
    values: &[Vec<u8>],
    from_log_id: u64,
    to_log_id_inclusive: u64,
) -> Result<BTreeSet<u64>> {
    let mut out = BTreeSet::new();
    let from_shard = (from_log_id >> 32) as u32;
    let to_shard = (to_log_id_inclusive >> 32) as u32;

    for value in values {
        for shard in from_shard..=to_shard {
            let sid = stream_id(kind, value, shard);
            let entries = load_stream_entries(meta_store, blob_store, &sid).await?;
            let local_from = if shard == from_shard {
                from_log_id as u32
            } else {
                0
            };
            let local_to = if shard == to_shard {
                to_log_id_inclusive as u32
            } else {
                u32::MAX
            };

            for local in entries.range(local_from..=local_to) {
                out.insert(((shard as u64) << 32) | (*local as u64));
            }
        }
    }
    Ok(out)
}

async fn fetch_union_block_level<M: MetaStore, B: BlobStore>(
    meta_store: &M,
    blob_store: &B,
    kind: &str,
    values: &[Vec<u8>],
    from_block: u64,
    to_block: u64,
) -> Result<BTreeSet<u32>> {
    let mut out = BTreeSet::new();
    let from_shard = (from_block >> 32) as u32;
    let to_shard = (to_block >> 32) as u32;

    for value in values {
        for shard in from_shard..=to_shard {
            let sid = stream_id(kind, value, shard);
            let entries = load_stream_entries(meta_store, blob_store, &sid).await?;
            let local_from = if shard == from_shard {
                from_block as u32
            } else {
                0
            };
            let local_to = if shard == to_shard {
                to_block as u32
            } else {
                u32::MAX
            };
            for local in entries.range(local_from..=local_to) {
                out.insert(*local);
            }
        }
    }
    Ok(out)
}

async fn load_stream_entries<M: MetaStore, B: BlobStore>(
    meta_store: &M,
    blob_store: &B,
    stream: &str,
) -> Result<BTreeSet<u32>> {
    let mut out = BTreeSet::new();

    let manifest = meta_store.get(&manifest_key(stream)).await?;
    if let Some(mrec) = manifest {
        let m = decode_manifest(&mrec.value)?;
        for cref in m.chunk_refs {
            maybe_merge_chunk(blob_store, stream, &cref, &mut out).await?;
        }
    }

    if let Some(trec) = meta_store.get(&tail_key(stream)).await? {
        let tail = decode_tail(&trec.value)?;
        for v in &tail {
            out.insert(v);
        }
    }

    Ok(out)
}

async fn maybe_merge_chunk<B: BlobStore>(
    blob_store: &B,
    stream: &str,
    cref: &ChunkRef,
    out: &mut BTreeSet<u32>,
) -> Result<()> {
    let bytes = match blob_store
        .get_blob(&chunk_blob_key(stream, cref.chunk_seq))
        .await?
    {
        Some(b) => b,
        None => return Ok(()),
    };

    let chunk = decode_chunk(&bytes)?;
    if chunk.count != cref.count {
        return Err(Error::Decode("chunk count mismatch against manifest"));
    }

    for v in chunk.bitmap {
        out.insert(v);
    }
    Ok(())
}
