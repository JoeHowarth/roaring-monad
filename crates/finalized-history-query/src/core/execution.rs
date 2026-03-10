use std::collections::BTreeSet;

use async_trait::async_trait;

use crate::core::ids::PrimaryIdRange;
use crate::core::refs::BlockRef;
use crate::error::Result;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchedPrimary<T> {
    pub id: u64,
    pub item: T,
    pub block_ref: BlockRef,
}

#[async_trait]
pub trait PrimaryMaterializer {
    type Primary;
    type Filter;

    async fn load_by_id(&mut self, id: u64) -> Result<Option<Self::Primary>>;
    async fn block_ref_for(&mut self, item: &Self::Primary) -> Result<BlockRef>;
    fn exact_match(&self, item: &Self::Primary, filter: &Self::Filter) -> bool;
}

pub async fn execute_candidates<M: PrimaryMaterializer>(
    clause_sets: Vec<BTreeSet<u64>>,
    id_range: PrimaryIdRange,
    filter: &M::Filter,
    materializer: &mut M,
    take: usize,
) -> Result<Vec<MatchedPrimary<M::Primary>>> {
    let candidates = intersect_sets(clause_sets, id_range);
    let mut out = Vec::new();
    for id in candidates {
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

fn intersect_sets(sets: Vec<BTreeSet<u64>>, id_range: PrimaryIdRange) -> BTreeSet<u64> {
    if sets.is_empty() {
        return (id_range.start..=id_range.end_inclusive).collect();
    }

    let mut it = sets.into_iter();
    let mut acc = it.next().unwrap_or_default();
    for set in it {
        acc = acc.intersection(&set).copied().collect();
        if acc.is_empty() {
            break;
        }
    }
    acc
}
