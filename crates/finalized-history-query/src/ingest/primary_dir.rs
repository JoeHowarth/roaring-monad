use crate::error::Result;
use crate::kernel::compaction::{
    compact_directory_bucket_from_sub_buckets, compact_directory_sub_bucket_from_fragments,
    sealed_ranges,
};
use crate::kernel::table_specs::aligned_u64_start;
use crate::store::traits::MetaStore;
use crate::tables::PrimaryDirTables;

pub async fn compact_newly_sealed_primary_directory<M: MetaStore>(
    dir: &PrimaryDirTables<M>,
    from_next_primary_id: u64,
    next_primary_id: u64,
) -> Result<()> {
    if next_primary_id <= from_next_primary_id {
        return Ok(());
    }

    for sub_bucket_start in sealed_ranges(
        from_next_primary_id,
        next_primary_id,
        crate::core::layout::DIRECTORY_SUB_BUCKET_SIZE,
        sub_bucket_start,
    ) {
        compact_primary_directory_sub_bucket(dir, sub_bucket_start).await?;
    }

    for bucket_start in sealed_ranges(
        from_next_primary_id,
        next_primary_id,
        crate::core::layout::DIRECTORY_BUCKET_SIZE,
        bucket_start,
    ) {
        compact_primary_directory_bucket(dir, bucket_start).await?;
    }

    Ok(())
}

pub async fn compact_sealed_primary_directory<M: MetaStore>(
    dir: &PrimaryDirTables<M>,
    first_primary_id: u64,
    count: u32,
    next_primary_id: u64,
) -> Result<()> {
    if count == 0 || next_primary_id <= first_primary_id {
        return Ok(());
    }

    compact_newly_sealed_primary_directory(dir, first_primary_id, next_primary_id).await
}

async fn compact_primary_directory_sub_bucket<M: MetaStore>(
    dir: &PrimaryDirTables<M>,
    sub_bucket_start: u64,
) -> Result<()> {
    let fragments = dir
        .fragments
        .load_sub_bucket_fragments(sub_bucket_start)
        .await?;
    if fragments.is_empty() {
        return Ok(());
    }

    let Some((start_block, first_primary_ids)) = compact_directory_sub_bucket_from_fragments(
        &fragments,
        |fragment| fragment.block_num,
        |fragment| fragment.first_primary_id,
        |fragment| fragment.end_primary_id_exclusive,
    ) else {
        return Ok(());
    };

    dir.sub_buckets
        .put(
            sub_bucket_start,
            &crate::core::directory::PrimaryDirBucket {
                start_block,
                first_primary_ids,
            },
        )
        .await
}

async fn compact_primary_directory_bucket<M: MetaStore>(
    dir: &PrimaryDirTables<M>,
    bucket_start: u64,
) -> Result<()> {
    let bucket_end = bucket_start.saturating_add(crate::core::layout::DIRECTORY_BUCKET_SIZE);
    let mut sub_bucket_start = bucket_start;
    let mut sub_buckets = Vec::new();

    while sub_bucket_start < bucket_end {
        let Some(sub_bucket) = dir.sub_buckets.get(sub_bucket_start).await? else {
            sub_bucket_start =
                sub_bucket_start.saturating_add(crate::core::layout::DIRECTORY_SUB_BUCKET_SIZE);
            continue;
        };
        sub_buckets.push(sub_bucket);
        sub_bucket_start =
            sub_bucket_start.saturating_add(crate::core::layout::DIRECTORY_SUB_BUCKET_SIZE);
    }

    let Some((start_block, first_primary_ids)) = compact_directory_bucket_from_sub_buckets(
        &sub_buckets,
        |sub_bucket| sub_bucket.start_block,
        |sub_bucket| sub_bucket.first_primary_ids.len(),
        |sub_bucket, index| sub_bucket.first_primary_ids[index],
        "directory bucket missing sentinel",
        "inconsistent directory bucket boundary across sub-buckets",
        "missing directory bucket start block",
    )?
    else {
        return Ok(());
    };

    dir.buckets
        .put(
            bucket_start,
            &crate::core::directory::PrimaryDirBucket {
                start_block,
                first_primary_ids,
            },
        )
        .await
}

fn sub_bucket_start(primary_id: u64) -> u64 {
    aligned_u64_start(primary_id, crate::core::layout::DIRECTORY_SUB_BUCKET_SIZE)
}

fn bucket_start(primary_id: u64) -> u64 {
    aligned_u64_start(primary_id, crate::core::layout::DIRECTORY_BUCKET_SIZE)
}
