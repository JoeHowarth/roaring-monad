use crate::error::Result;
use crate::kernel::compaction::{
    compact_directory_bucket_from_sub_buckets, compact_directory_sub_bucket_from_fragments,
    sealed_ranges,
};
use crate::logs::keys::LOG_DIRECTORY_SUB_BUCKET_SIZE;
use crate::logs::table_specs::{LogDirBucketSpec, LogDirSubBucketSpec};
use crate::logs::types::DirBucket;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;

pub async fn compact_sealed_directory<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    first_log_id: u64,
    count: u32,
    next_log_id: u64,
) -> Result<()> {
    let from_next_log_id = first_log_id;
    if count == 0 || next_log_id <= from_next_log_id {
        return Ok(());
    }

    compact_newly_sealed_directory(tables, from_next_log_id, next_log_id).await
}

pub async fn compact_newly_sealed_directory<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    from_next_log_id: u64,
    next_log_id: u64,
) -> Result<()> {
    if next_log_id <= from_next_log_id {
        return Ok(());
    }

    for sub_bucket_start in newly_sealed_directory_sub_bucket_starts(from_next_log_id, next_log_id)
    {
        compact_directory_sub_bucket(tables, sub_bucket_start).await?;
    }

    for bucket_start in newly_sealed_directory_bucket_starts(from_next_log_id, next_log_id) {
        compact_directory_bucket(tables, bucket_start).await?;
    }

    Ok(())
}

pub fn newly_sealed_directory_sub_bucket_starts(
    from_next_log_id: u64,
    to_next_log_id: u64,
) -> Vec<u64> {
    sealed_ranges(
        from_next_log_id,
        to_next_log_id,
        LOG_DIRECTORY_SUB_BUCKET_SIZE,
        LogDirSubBucketSpec::sub_bucket_start,
    )
}

pub fn newly_sealed_directory_bucket_starts(
    from_next_log_id: u64,
    to_next_log_id: u64,
) -> Vec<u64> {
    sealed_ranges(
        from_next_log_id,
        to_next_log_id,
        crate::logs::keys::LOG_DIRECTORY_BUCKET_SIZE,
        LogDirBucketSpec::bucket_start,
    )
}

async fn compact_directory_sub_bucket<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    sub_bucket_start: u64,
) -> Result<()> {
    let fragments = tables
        .log_dir()
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

    let bucket = DirBucket {
        start_block,
        first_primary_ids,
    };
    tables
        .log_dir()
        .put_sub_bucket(sub_bucket_start, &bucket)
        .await?;
    Ok(())
}

async fn compact_directory_bucket<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    bucket_start: u64,
) -> Result<()> {
    let bucket_end = bucket_start.saturating_add(crate::logs::keys::LOG_DIRECTORY_BUCKET_SIZE);
    let mut sub_bucket_start = bucket_start;
    let mut sub_buckets = Vec::new();

    while sub_bucket_start < bucket_end {
        let Some(sub_bucket) = tables.log_dir().get_sub_bucket(sub_bucket_start).await? else {
            sub_bucket_start = sub_bucket_start.saturating_add(LOG_DIRECTORY_SUB_BUCKET_SIZE);
            continue;
        };
        sub_buckets.push(sub_bucket);
        sub_bucket_start = sub_bucket_start.saturating_add(LOG_DIRECTORY_SUB_BUCKET_SIZE);
    }

    let Some((start_block, first_primary_ids)) = compact_directory_bucket_from_sub_buckets(
        &sub_buckets,
        |sub_bucket| sub_bucket.start_block,
        |sub_bucket| sub_bucket.first_primary_ids.len(),
        |sub_bucket, index| sub_bucket.first_primary_ids[index],
        "directory sub-bucket missing sentinel",
        "inconsistent directory bucket boundary across sub-buckets",
        "missing directory bucket start block",
    )?
    else {
        return Ok(());
    };

    tables
        .log_dir()
        .put_bucket(
            bucket_start,
            &DirBucket {
                start_block,
                first_primary_ids,
            },
        )
        .await?;
    Ok(())
}
