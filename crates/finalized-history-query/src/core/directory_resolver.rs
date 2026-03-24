use std::collections::HashMap;
use std::collections::hash_map::Entry;

use crate::core::directory::{PrimaryDirBucket, PrimaryDirFragment};
use crate::core::ids::FamilyIdValue;
use crate::core::layout::{DIRECTORY_BUCKET_SIZE, DIRECTORY_SUB_BUCKET_SIZE};
use crate::error::{Error, Result};
use crate::kernel::table_specs::aligned_u64_start;
use crate::store::traits::MetaStore;
use crate::tables::PrimaryDirTables;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedPrimaryLocation {
    pub block_num: u64,
    pub local_ordinal: usize,
}

pub async fn resolve_primary_id<M, I>(
    dir: &PrimaryDirTables<M>,
    fragment_cache: &mut HashMap<u64, Vec<PrimaryDirFragment>>,
    id: I,
) -> Result<Option<ResolvedPrimaryLocation>>
where
    M: MetaStore,
    I: FamilyIdValue,
{
    let bucket_start = aligned_u64_start(id.get(), DIRECTORY_BUCKET_SIZE);
    if let Some(bucket) = dir.buckets.get(bucket_start).await?
        && let Some(location) = resolved_location_from_bucket(&bucket, id)?
    {
        return Ok(Some(location));
    }

    let sub_bucket_start = aligned_u64_start(id.get(), DIRECTORY_SUB_BUCKET_SIZE);
    if let Some(bucket) = dir.sub_buckets.get(sub_bucket_start).await?
        && let Some(location) = resolved_location_from_bucket(&bucket, id)?
    {
        return Ok(Some(location));
    }

    let fragments = load_directory_fragments(dir, fragment_cache, sub_bucket_start).await?;
    let Some(fragment) = fragments.iter().find(|fragment| {
        id.get() >= fragment.first_primary_id && id.get() < fragment.end_primary_id_exclusive
    }) else {
        return Ok(None);
    };

    Ok(Some(ResolvedPrimaryLocation {
        block_num: fragment.block_num,
        local_ordinal: usize::try_from(id.get() - fragment.first_primary_id)
            .map_err(|_| Error::Decode("primary local ordinal overflow"))?,
    }))
}

pub async fn load_directory_fragments<'a, M: MetaStore>(
    dir: &PrimaryDirTables<M>,
    fragment_cache: &'a mut HashMap<u64, Vec<PrimaryDirFragment>>,
    sub_bucket_start: u64,
) -> Result<&'a [PrimaryDirFragment]> {
    if let Entry::Vacant(entry) = fragment_cache.entry(sub_bucket_start) {
        entry.insert(
            dir.fragments
                .load_sub_bucket_fragments(sub_bucket_start)
                .await?,
        );
    }
    Ok(fragment_cache
        .get(&sub_bucket_start)
        .map(Vec::as_slice)
        .unwrap_or(&[]))
}

fn resolved_location_from_bucket<I: FamilyIdValue>(
    bucket: &PrimaryDirBucket,
    id: I,
) -> Result<Option<ResolvedPrimaryLocation>> {
    let Some(entry_index) = containing_bucket_entry(bucket, id.get()) else {
        return Ok(None);
    };
    Ok(Some(ResolvedPrimaryLocation {
        block_num: bucket.start_block + entry_index as u64,
        local_ordinal: usize::try_from(id.get() - bucket.first_primary_ids[entry_index])
            .map_err(|_| Error::Decode("primary local ordinal overflow"))?,
    }))
}

fn containing_bucket_entry(bucket: &PrimaryDirBucket, id: u64) -> Option<usize> {
    if bucket.first_primary_ids.len() < 2 {
        return None;
    }
    let upper = bucket
        .first_primary_ids
        .partition_point(|first_primary_id| *first_primary_id <= id);
    if upper == 0 || upper >= bucket.first_primary_ids.len() {
        return None;
    }
    let entry_index = upper - 1;
    let end = bucket.first_primary_ids[upper];
    (id < end).then_some(entry_index)
}
