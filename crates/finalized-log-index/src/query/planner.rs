use crate::codec::manifest::{ChunkRef, decode_manifest, decode_tail};
use crate::config::BroadQueryPolicy;
use crate::domain::filter::{Clause, LogFilter, QueryOptions};
use crate::domain::keys::{manifest_key, stream_id, tail_key};
use crate::error::Result;
use crate::store::traits::MetaStore;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClauseKind {
    Address,
    Topic0Log,
    Topic1,
    Topic2,
    Topic3,
}

#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub filter: LogFilter,
    pub options: QueryOptions,
    pub clipped_from_block: u64,
    pub clipped_to_block: u64,
    pub from_log_id: u64,
    pub to_log_id_inclusive: u64,
    pub clause_order: Vec<ClauseKind>,
    pub force_block_scan: bool,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ClauseEstimate {
    pub estimated: u64,
}

pub async fn build_clause_order<M: MetaStore>(
    meta_store: &M,
    filter: &LogFilter,
    from_log_id: u64,
    to_log_id_inclusive: u64,
) -> Result<Vec<ClauseKind>> {
    let mut clauses = Vec::<(ClauseKind, ClauseEstimate)>::new();

    if let Some(c) = &filter.address {
        let values = clause_values_20(c);
        if !values.is_empty() {
            let estimate = estimate_for_values(
                meta_store,
                "addr",
                &values,
                from_log_id,
                to_log_id_inclusive,
            )
            .await?;
            clauses.push((
                ClauseKind::Address,
                ClauseEstimate {
                    estimated: estimate,
                },
            ));
        }
    }

    if let Some(c) = &filter.topic1 {
        let values = clause_values_32(c);
        if !values.is_empty() {
            let estimate = estimate_for_values(
                meta_store,
                "topic1",
                &values,
                from_log_id,
                to_log_id_inclusive,
            )
            .await?;
            clauses.push((
                ClauseKind::Topic1,
                ClauseEstimate {
                    estimated: estimate,
                },
            ));
        }
    }

    if let Some(c) = &filter.topic2 {
        let values = clause_values_32(c);
        if !values.is_empty() {
            let estimate = estimate_for_values(
                meta_store,
                "topic2",
                &values,
                from_log_id,
                to_log_id_inclusive,
            )
            .await?;
            clauses.push((
                ClauseKind::Topic2,
                ClauseEstimate {
                    estimated: estimate,
                },
            ));
        }
    }

    if let Some(c) = &filter.topic3 {
        let values = clause_values_32(c);
        if !values.is_empty() {
            let estimate = estimate_for_values(
                meta_store,
                "topic3",
                &values,
                from_log_id,
                to_log_id_inclusive,
            )
            .await?;
            clauses.push((
                ClauseKind::Topic3,
                ClauseEstimate {
                    estimated: estimate,
                },
            ));
        }
    }

    if let Some(c) = &filter.topic0 {
        let values = clause_values_32(c);
        if !values.is_empty() {
            let estimate = estimate_for_values(
                meta_store,
                "topic0_log",
                &values,
                from_log_id,
                to_log_id_inclusive,
            )
            .await?;
            if estimate > 0 {
                clauses.push((
                    ClauseKind::Topic0Log,
                    ClauseEstimate {
                        estimated: estimate,
                    },
                ));
            }
        }
    }

    clauses.sort_by_key(|(_, est)| est.estimated);
    Ok(clauses.into_iter().map(|(k, _)| k).collect())
}

pub fn should_force_block_scan(
    filter: &LogFilter,
    max_or_terms: usize,
    policy: BroadQueryPolicy,
) -> bool {
    let max_terms = filter.max_or_terms();
    max_terms > max_or_terms && matches!(policy, BroadQueryPolicy::BlockScan)
}

pub fn should_error_too_broad(
    filter: &LogFilter,
    max_or_terms: usize,
    policy: BroadQueryPolicy,
) -> bool {
    let max_terms = filter.max_or_terms();
    max_terms > max_or_terms && matches!(policy, BroadQueryPolicy::Error)
}

async fn estimate_for_values<M: MetaStore>(
    meta_store: &M,
    kind: &str,
    values: &[Vec<u8>],
    from_log_id: u64,
    to_log_id_inclusive: u64,
) -> Result<u64> {
    let from_shard = (from_log_id >> 32) as u32;
    let to_shard = (to_log_id_inclusive >> 32) as u32;

    let mut sum = 0u64;
    for value in values {
        for shard in from_shard..=to_shard {
            let sid = stream_id(kind, value, shard);
            sum = sum.saturating_add(
                estimate_stream_overlap(meta_store, &sid, from_log_id, to_log_id_inclusive, shard)
                    .await?,
            );
        }
    }
    Ok(sum)
}

async fn estimate_stream_overlap<M: MetaStore>(
    meta_store: &M,
    stream_id: &str,
    from_log_id: u64,
    to_log_id_inclusive: u64,
    shard: u32,
) -> Result<u64> {
    let local_from = if shard == (from_log_id >> 32) as u32 {
        from_log_id as u32
    } else {
        0
    };
    let local_to = if shard == (to_log_id_inclusive >> 32) as u32 {
        to_log_id_inclusive as u32
    } else {
        u32::MAX
    };

    let mut count = 0u64;
    if let Some(m) = meta_store.get(&manifest_key(stream_id)).await? {
        let manifest = decode_manifest(&m.value)?;
        for c in manifest.chunk_refs {
            if overlaps(&c, local_from, local_to) {
                count = count.saturating_add(c.count as u64);
            }
        }
    }

    if let Some(t) = meta_store.get(&tail_key(stream_id)).await? {
        let tail = decode_tail(&t.value)?;
        for v in &tail {
            if v >= local_from && v <= local_to {
                count = count.saturating_add(1);
            }
        }
    }

    Ok(count)
}

fn overlaps(c: &ChunkRef, local_from: u32, local_to: u32) -> bool {
    c.max_local >= local_from && c.min_local <= local_to
}

pub fn clause_values_20(clause: &Clause<[u8; 20]>) -> Vec<Vec<u8>> {
    match clause {
        Clause::Any => Vec::new(),
        Clause::One(v) => vec![v.to_vec()],
        Clause::Or(vs) => vs.iter().map(|v| v.to_vec()).collect(),
    }
}

pub fn clause_values_32(clause: &Clause<[u8; 32]>) -> Vec<Vec<u8>> {
    match clause {
        Clause::Any => Vec::new(),
        Clause::One(v) => vec![v.to_vec()],
        Clause::Or(vs) => vs.iter().map(|v| v.to_vec()).collect(),
    }
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;
    use roaring::RoaringBitmap;

    use super::*;
    use crate::codec::manifest::{ChunkRef, Manifest, encode_manifest, encode_tail};
    use crate::domain::keys::{manifest_key, stream_id, tail_key};
    use crate::store::meta::InMemoryMetaStore;
    use crate::store::traits::{FenceToken, MetaStore, PutCond};

    #[test]
    fn broad_policy_helpers() {
        let filter = LogFilter {
            from_block: None,
            to_block: None,
            block_hash: None,
            address: Some(Clause::Or(vec![[1; 20], [2; 20], [3; 20]])),
            topic0: None,
            topic1: None,
            topic2: None,
            topic3: None,
        };
        assert!(should_error_too_broad(&filter, 2, BroadQueryPolicy::Error));
        assert!(should_force_block_scan(
            &filter,
            2,
            BroadQueryPolicy::BlockScan
        ));
    }

    #[test]
    fn clause_order_prefers_lower_estimate() {
        block_on(async {
            let store = InMemoryMetaStore::default();

            let addr_value = [1u8; 20];
            let topic1_value = [2u8; 32];
            let from = 1u64 << 32;
            let to = from + 1000;
            let shard = (from >> 32) as u32;

            let addr_sid = stream_id("addr", &addr_value, shard);
            let topic_sid = stream_id("topic1", &topic1_value, shard);

            let addr_manifest = Manifest {
                version: 1,
                last_chunk_seq: 1,
                approx_count: 100,
                last_seal_unix_sec: 0,
                chunk_refs: vec![ChunkRef {
                    chunk_seq: 1,
                    min_local: 0,
                    max_local: 500,
                    count: 100,
                }],
            };
            let topic_manifest = Manifest {
                version: 1,
                last_chunk_seq: 1,
                approx_count: 2,
                last_seal_unix_sec: 0,
                chunk_refs: vec![ChunkRef {
                    chunk_seq: 1,
                    min_local: 0,
                    max_local: 500,
                    count: 2,
                }],
            };

            let _ = store
                .put(
                    &manifest_key(&addr_sid),
                    encode_manifest(&addr_manifest),
                    PutCond::Any,
                    FenceToken(0),
                )
                .await
                .expect("put addr manifest");
            let _ = store
                .put(
                    &manifest_key(&topic_sid),
                    encode_manifest(&topic_manifest),
                    PutCond::Any,
                    FenceToken(0),
                )
                .await
                .expect("put topic manifest");

            let mut t = RoaringBitmap::new();
            t.insert(3);
            let _ = store
                .put(
                    &tail_key(&topic_sid),
                    encode_tail(&t).expect("encode tail"),
                    PutCond::Any,
                    FenceToken(0),
                )
                .await
                .expect("put topic tail");

            let filter = LogFilter {
                from_block: None,
                to_block: None,
                block_hash: None,
                address: Some(Clause::One(addr_value)),
                topic0: None,
                topic1: Some(Clause::One(topic1_value)),
                topic2: None,
                topic3: None,
            };

            let order = build_clause_order(&store, &filter, from, to)
                .await
                .expect("build order");
            assert_eq!(order, vec![ClauseKind::Topic1, ClauseKind::Address]);
        });
    }
}
