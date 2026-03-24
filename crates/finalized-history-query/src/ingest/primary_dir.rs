use crate::error::Result;
use crate::kernel::compaction::{
    compact_directory_bucket_from_sub_buckets, compact_directory_sub_bucket_from_fragments,
    sealed_ranges,
};
use crate::store::traits::MetaStore;
use crate::tables::PrimaryDirTables;

#[derive(Clone, Copy)]
pub struct PrimaryDirCompactionLayout {
    pub sub_bucket_span: u64,
    pub bucket_span: u64,
    pub sub_bucket_start: fn(u64) -> u64,
    pub bucket_start: fn(u64) -> u64,
    pub missing_sentinel_error: &'static str,
    pub inconsistent_bucket_error: &'static str,
    pub missing_bucket_start_error: &'static str,
}

pub async fn compact_newly_sealed_primary_directory<M: MetaStore>(
    dir: &PrimaryDirTables<M>,
    from_next_primary_id: u64,
    next_primary_id: u64,
    layout: PrimaryDirCompactionLayout,
) -> Result<()> {
    if next_primary_id <= from_next_primary_id {
        return Ok(());
    }

    for sub_bucket_start in sealed_ranges(
        from_next_primary_id,
        next_primary_id,
        layout.sub_bucket_span,
        layout.sub_bucket_start,
    ) {
        compact_primary_directory_sub_bucket(dir, sub_bucket_start).await?;
    }

    for bucket_start in sealed_ranges(
        from_next_primary_id,
        next_primary_id,
        layout.bucket_span,
        layout.bucket_start,
    ) {
        compact_primary_directory_bucket(dir, bucket_start, layout).await?;
    }

    Ok(())
}

pub async fn compact_sealed_primary_directory<M: MetaStore>(
    dir: &PrimaryDirTables<M>,
    first_primary_id: u64,
    count: u32,
    next_primary_id: u64,
    layout: PrimaryDirCompactionLayout,
) -> Result<()> {
    if count == 0 || next_primary_id <= first_primary_id {
        return Ok(());
    }

    compact_newly_sealed_primary_directory(dir, first_primary_id, next_primary_id, layout).await
}

async fn compact_primary_directory_sub_bucket<M: MetaStore>(
    dir: &PrimaryDirTables<M>,
    sub_bucket_start: u64,
) -> Result<()> {
    let fragments = dir.load_sub_bucket_fragments(sub_bucket_start).await?;
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

    dir.put_sub_bucket(
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
    layout: PrimaryDirCompactionLayout,
) -> Result<()> {
    let bucket_end = bucket_start.saturating_add(layout.bucket_span);
    let mut sub_bucket_start = bucket_start;
    let mut sub_buckets = Vec::new();

    while sub_bucket_start < bucket_end {
        let Some(sub_bucket) = dir.get_sub_bucket(sub_bucket_start).await? else {
            sub_bucket_start = sub_bucket_start.saturating_add(layout.sub_bucket_span);
            continue;
        };
        sub_buckets.push(sub_bucket);
        sub_bucket_start = sub_bucket_start.saturating_add(layout.sub_bucket_span);
    }

    let Some((start_block, first_primary_ids)) = compact_directory_bucket_from_sub_buckets(
        &sub_buckets,
        |sub_bucket| sub_bucket.start_block,
        |sub_bucket| sub_bucket.first_primary_ids.len(),
        |sub_bucket, index| sub_bucket.first_primary_ids[index],
        layout.missing_sentinel_error,
        layout.inconsistent_bucket_error,
        layout.missing_bucket_start_error,
    )?
    else {
        return Ok(());
    };

    dir.put_bucket(
        bucket_start,
        &crate::core::directory::PrimaryDirBucket {
            start_block,
            first_primary_ids,
        },
    )
    .await
}
