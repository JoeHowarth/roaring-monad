use std::collections::BTreeMap;

use roaring::RoaringBitmap;

use crate::core::ids::{LogId, LogLocalId, LogShard, PrimaryIdRange, compose_log_id};
use crate::core::refs::BlockRef;
use crate::domain::keys::MAX_LOCAL_ID;
use crate::error::Result;

pub type ShardBitmapSet = BTreeMap<LogShard, RoaringBitmap>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchedPrimary<T> {
    pub id: LogId,
    pub item: T,
    pub block_ref: BlockRef,
}

#[allow(async_fn_in_trait)]
pub trait PrimaryMaterializer {
    type Primary;
    type Filter;

    async fn load_by_id(&mut self, id: LogId) -> Result<Option<Self::Primary>>;
    async fn block_ref_for(&mut self, item: &Self::Primary) -> Result<BlockRef>;
    fn exact_match(&self, item: &Self::Primary, filter: &Self::Filter) -> bool;
}

pub async fn execute_candidates<M: PrimaryMaterializer>(
    clause_sets: Vec<ShardBitmapSet>,
    id_range: PrimaryIdRange,
    filter: &M::Filter,
    materializer: &mut M,
    take: usize,
) -> Result<Vec<MatchedPrimary<M::Primary>>> {
    let mut out = Vec::new();

    if clause_sets.is_empty() {
        for raw_id in id_range.start.get()..=id_range.end_inclusive.get() {
            let id = LogId::new(raw_id);
            let Some(item) = materializer.load_by_id(id).await? else {
                continue;
            };
            if !materializer.exact_match(&item, filter) {
                continue;
            }

            let block_ref = materializer.block_ref_for(&item).await?;
            out.push(MatchedPrimary {
                id,
                item,
                block_ref,
            });
            if out.len() >= take {
                break;
            }
        }
        return Ok(out);
    }

    for id in iter_log_ids(&intersect_sets(clause_sets, id_range)) {
        let Some(item) = materializer.load_by_id(id).await? else {
            continue;
        };
        if !materializer.exact_match(&item, filter) {
            continue;
        }

        let block_ref = materializer.block_ref_for(&item).await?;
        out.push(MatchedPrimary {
            id,
            item,
            block_ref,
        });
        if out.len() >= take {
            break;
        }
    }
    Ok(out)
}

fn intersect_sets(sets: Vec<ShardBitmapSet>, id_range: PrimaryIdRange) -> ShardBitmapSet {
    let mut it = sets.into_iter();
    let mut acc = it.next().unwrap_or_default();
    for set in it {
        acc.retain(|shard, bitmap| {
            let Some(other) = set.get(shard) else {
                return false;
            };
            *bitmap &= other;
            !bitmap.is_empty()
        });
        if acc.is_empty() {
            break;
        }
    }
    clip_shard_bitmaps_to_range(acc, id_range)
}

fn clip_shard_bitmaps_to_range(
    mut bitmaps: ShardBitmapSet,
    id_range: PrimaryIdRange,
) -> ShardBitmapSet {
    let from_shard = id_range.start.shard();
    let to_shard = id_range.end_inclusive.shard();
    bitmaps.retain(|shard, bitmap| {
        if *shard < from_shard || *shard > to_shard {
            return false;
        }
        if from_shard == to_shard {
            bitmap.remove_range(0..id_range.start.local().get());
            bitmap.remove_range(
                id_range.end_inclusive.local().get().saturating_add(1)
                    ..MAX_LOCAL_ID.saturating_add(1),
            );
        } else if *shard == from_shard {
            bitmap.remove_range(0..id_range.start.local().get());
        } else if *shard == to_shard {
            bitmap.remove_range(
                id_range.end_inclusive.local().get().saturating_add(1)
                    ..MAX_LOCAL_ID.saturating_add(1),
            );
        }
        !bitmap.is_empty()
    });
    bitmaps
}

fn iter_log_ids(bitmaps: &ShardBitmapSet) -> impl Iterator<Item = LogId> + '_ {
    bitmaps.iter().flat_map(|(shard, bitmap)| {
        bitmap.iter().map(|local| {
            compose_log_id(
                *shard,
                LogLocalId::new(local).expect("bitmap values must fit local-id width"),
            )
        })
    })
}
