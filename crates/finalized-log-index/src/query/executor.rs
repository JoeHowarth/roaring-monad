use std::collections::{BTreeSet, HashMap};

use futures::stream::{FuturesUnordered, StreamExt};

use crate::codec::chunk::decode_chunk;
use crate::codec::log::{
    decode_block_meta, decode_log, decode_log_locator, decode_log_locator_page,
};
use crate::codec::manifest::{ChunkRef, decode_manifest, decode_tail};
use crate::domain::filter::{Clause, LogFilter};
use crate::domain::keys::{
    block_local, block_meta_key, block_range_for_shard, block_shard, chunk_blob_key,
    compose_global_log_id, local_range_for_shard, log_locator_key, log_locator_page_key,
    log_locator_page_start, log_shard, manifest_key, stream_id, tail_key,
};
use crate::domain::types::{Log, LogLocator, Topic32};
use crate::error::{Error, Result};
use crate::query::planner::{
    ClauseKind, QueryPlan, clause_values_20, clause_values_32, overlapping_chunk_refs,
};
use crate::store::traits::{BlobStore, MetaStore};

const STREAM_LOAD_CONCURRENCY: usize = 32;

pub async fn execute_plan<M: MetaStore, B: BlobStore>(
    meta_store: &M,
    blob_store: &B,
    plan: QueryPlan,
) -> Result<Vec<Log>> {
    let mut log_pack_cache: HashMap<Vec<u8>, bytes::Bytes> = HashMap::new();
    let mut locator_page_cache: HashMap<u64, HashMap<u16, LogLocator>> = HashMap::new();
    if plan.force_block_scan {
        return execute_block_scan(
            meta_store,
            blob_store,
            &plan,
            &mut locator_page_cache,
            &mut log_pack_cache,
        )
        .await;
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
        let Some(log) = load_log_by_id(
            meta_store,
            blob_store,
            id,
            &mut locator_page_cache,
            &mut log_pack_cache,
        )
        .await?
        else {
            continue;
        };

        if log.block_num < plan.clipped_from_block || log.block_num > plan.clipped_to_block {
            continue;
        }

        if let Some(blocks) = &topic0_blocks
            && !blocks.contains(&block_local(log.block_num))
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
    locator_page_cache: &mut HashMap<u64, HashMap<u16, LogLocator>>,
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
            let Some(log) = load_log_by_id(
                meta_store,
                blob_store,
                id,
                locator_page_cache,
                log_pack_cache,
            )
            .await?
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
    locator_page_cache: &mut HashMap<u64, HashMap<u16, LogLocator>>,
    log_pack_cache: &mut HashMap<Vec<u8>, bytes::Bytes>,
) -> Result<Option<Log>> {
    let page_start = log_locator_page_start(log_id);
    if let std::collections::hash_map::Entry::Vacant(entry) = locator_page_cache.entry(page_start) {
        let page = match meta_store.get(&log_locator_page_key(page_start)).await? {
            Some(rec) => {
                let (decoded_page_start, entries) = decode_log_locator_page(&rec.value)?;
                if decoded_page_start != page_start {
                    return Err(Error::Decode("log locator page start mismatch"));
                }
                entries
            }
            None => HashMap::new(),
        };
        entry.insert(page);
    }

    let mut locator = locator_page_cache.get(&page_start).and_then(|page| {
        let slot = u16::try_from(log_id - page_start).ok()?;
        page.get(&slot).cloned()
    });
    if locator.is_none() {
        locator = match meta_store.get(&log_locator_key(log_id)).await? {
            Some(rec) => Some(decode_log_locator(&rec.value)?),
            None => None,
        };
    }
    let Some(locator) = locator else {
        return Ok(None);
    };

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
    let from_shard = log_shard(from_log_id);
    let to_shard = log_shard(to_log_id_inclusive);
    let mut in_flight = FuturesUnordered::new();

    for value in values {
        for shard in from_shard..=to_shard {
            let sid = stream_id(kind, value, shard);
            let (local_from, local_to) =
                local_range_for_shard(from_log_id, to_log_id_inclusive, shard);
            in_flight.push(async move {
                let entries =
                    load_stream_entries(meta_store, blob_store, &sid, local_from, local_to).await?;
                Ok::<(u32, u32, u32, BTreeSet<u32>), Error>((shard, local_from, local_to, entries))
            });
            if in_flight.len() >= STREAM_LOAD_CONCURRENCY
                && let Some(res) = in_flight.next().await
            {
                let (shard, _, _, entries) = res?;
                for local in entries {
                    out.insert(compose_global_log_id(shard, local));
                }
            }
        }
    }
    while let Some(res) = in_flight.next().await {
        let (shard, _, _, entries) = res?;
        for local in entries {
            out.insert(compose_global_log_id(shard, local));
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
    let from_shard = block_shard(from_block);
    let to_shard = block_shard(to_block);
    let mut in_flight = FuturesUnordered::new();

    for value in values {
        for shard in from_shard..=to_shard {
            let sid = stream_id(kind, value, shard);
            let (local_from, local_to) = block_range_for_shard(from_block, to_block, shard);
            in_flight.push(async move {
                let entries =
                    load_stream_entries(meta_store, blob_store, &sid, local_from, local_to).await?;
                Ok::<(u32, u32, BTreeSet<u32>), Error>((local_from, local_to, entries))
            });
            if in_flight.len() >= STREAM_LOAD_CONCURRENCY
                && let Some(res) = in_flight.next().await
            {
                let (_, _, entries) = res?;
                for local in entries {
                    out.insert(local);
                }
            }
        }
    }
    while let Some(res) = in_flight.next().await {
        let (_, _, entries) = res?;
        for local in entries {
            out.insert(local);
        }
    }
    Ok(out)
}

async fn load_stream_entries<M: MetaStore, B: BlobStore>(
    meta_store: &M,
    blob_store: &B,
    stream: &str,
    local_from: u32,
    local_to: u32,
) -> Result<BTreeSet<u32>> {
    let mut out = BTreeSet::new();

    let manifest = meta_store.get(&manifest_key(stream)).await?;
    if let Some(mrec) = manifest {
        let m = decode_manifest(&mrec.value)?;
        for cref in overlapping_chunk_refs(&m.chunk_refs, local_from, local_to) {
            maybe_merge_chunk(blob_store, stream, cref, &mut out).await?;
        }
    }

    if let Some(trec) = meta_store.get(&tail_key(stream)).await? {
        let tail = decode_tail(&trec.value)?;
        for v in &tail {
            if v >= local_from && v <= local_to {
                out.insert(v);
            }
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
