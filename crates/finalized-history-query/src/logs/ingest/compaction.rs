use std::collections::BTreeMap;

use crate::core::ids::LogId;
use crate::error::{Error, Result};
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
    sealed_directory_ranges(
        from_next_log_id,
        to_next_log_id,
        LOG_DIRECTORY_SUB_BUCKET_SIZE,
        |id| LogDirSubBucketSpec::sub_bucket_start(id.get()),
    )
}

pub fn newly_sealed_directory_bucket_starts(
    from_next_log_id: u64,
    to_next_log_id: u64,
) -> Vec<u64> {
    sealed_directory_ranges(
        from_next_log_id,
        to_next_log_id,
        crate::logs::keys::LOG_DIRECTORY_BUCKET_SIZE,
        |id| LogDirBucketSpec::bucket_start(id.get()),
    )
}

fn sealed_directory_ranges(
    from_next_log_id: u64,
    to_next_log_id: u64,
    span: u64,
    align: impl Fn(LogId) -> u64,
) -> Vec<u64> {
    if to_next_log_id <= from_next_log_id {
        return Vec::new();
    }

    let mut current = align(LogId::new(from_next_log_id));
    let mut out = Vec::new();
    loop {
        let end = current.saturating_add(span);
        if end > to_next_log_id {
            break;
        }
        if end > from_next_log_id {
            out.push(current);
        }
        current = current.saturating_add(span);
    }
    out
}

async fn compact_directory_sub_bucket<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    sub_bucket_start: u64,
) -> Result<()> {
    let fragments = tables
        .directory_fragments()
        .load_sub_bucket_fragments(sub_bucket_start)
        .await?;
    if fragments.is_empty() {
        return Ok(());
    }

    let mut first_log_ids = Vec::with_capacity(fragments.len() + 1);
    for fragment in &fragments {
        first_log_ids.push(fragment.first_log_id);
    }
    let sentinel = fragments
        .last()
        .map(|fragment| fragment.end_log_id_exclusive)
        .unwrap_or(sub_bucket_start);
    first_log_ids.push(sentinel);

    let bucket = DirBucket {
        start_block: fragments[0].block_num,
        first_log_ids,
    };
    tables
        .log_dir_sub_buckets()
        .put(sub_bucket_start, &bucket)
        .await?;
    Ok(())
}

async fn compact_directory_bucket<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    bucket_start: u64,
) -> Result<()> {
    let bucket_end = bucket_start.saturating_add(crate::logs::keys::LOG_DIRECTORY_BUCKET_SIZE);
    let mut sub_bucket_start = bucket_start;
    let mut boundaries = BTreeMap::<u64, u64>::new();
    let mut sentinel = None::<u64>;

    while sub_bucket_start < bucket_end {
        let Some(sub_bucket) = tables.log_dir_sub_buckets().get(sub_bucket_start).await? else {
            sub_bucket_start = sub_bucket_start.saturating_add(LOG_DIRECTORY_SUB_BUCKET_SIZE);
            continue;
        };
        for index in 0..sub_bucket.count().saturating_sub(1) {
            let block_num = sub_bucket.start_block() + index as u64;
            let first_log_id = sub_bucket.first_log_id(index);
            match boundaries.entry(block_num) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(first_log_id);
                }
                std::collections::btree_map::Entry::Occupied(entry) => {
                    if *entry.get() != first_log_id {
                        return Err(Error::Decode(
                            "inconsistent directory bucket boundary across sub-buckets",
                        ));
                    }
                }
            }
        }
        let last = sub_bucket.first_log_id(sub_bucket.count().saturating_sub(1));
        sentinel = Some(match sentinel {
            Some(current) => current.max(last),
            None => last,
        });
        sub_bucket_start = sub_bucket_start.saturating_add(LOG_DIRECTORY_SUB_BUCKET_SIZE);
    }

    let Some(sentinel) = sentinel else {
        return Ok(());
    };
    if boundaries.is_empty() {
        return Ok(());
    }

    let start_block = *boundaries
        .keys()
        .next()
        .ok_or(Error::Decode("missing directory bucket start block"))?;
    let mut first_log_ids = boundaries.into_values().collect::<Vec<_>>();
    first_log_ids.push(sentinel);

    tables
        .dir_buckets()
        .put(
            bucket_start,
            &DirBucket {
                start_block,
                first_log_ids,
            },
        )
        .await?;
    Ok(())
}
