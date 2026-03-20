use std::collections::BTreeMap;

use crate::codec::log::{decode_dir_by_block, encode_log_dir_bucket};
use crate::core::ids::LogId;
use crate::domain::keys::{
    LOG_DIRECTORY_SUB_BUCKET_SIZE, log_dir_bucket_key, log_dir_bucket_start,
    log_dir_by_block_prefix, log_dir_sub_bucket_key, log_dir_sub_bucket_start,
};
use crate::domain::types::DirBucket;
use crate::error::{Error, Result};
use crate::store::traits::MetaStore;

use super::artifact::put_artifact_meta;

pub async fn compact_sealed_directory<M: MetaStore>(
    meta_store: &M,
    first_log_id: u64,
    count: u32,
    next_log_id: u64,
    epoch: u64,
) -> Result<()> {
    let from_next_log_id = first_log_id;
    if count == 0 || next_log_id <= from_next_log_id {
        return Ok(());
    }

    compact_newly_sealed_directory(meta_store, from_next_log_id, next_log_id, epoch).await
}

pub async fn compact_newly_sealed_directory<M: MetaStore>(
    meta_store: &M,
    from_next_log_id: u64,
    next_log_id: u64,
    epoch: u64,
) -> Result<()> {
    if next_log_id <= from_next_log_id {
        return Ok(());
    }

    for sub_bucket_start in newly_sealed_directory_sub_bucket_starts(from_next_log_id, next_log_id)
    {
        compact_directory_sub_bucket(meta_store, sub_bucket_start, epoch).await?;
    }

    for bucket_start in newly_sealed_directory_bucket_starts(from_next_log_id, next_log_id) {
        compact_directory_bucket(meta_store, bucket_start, epoch).await?;
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
        log_dir_sub_bucket_start,
    )
}

pub fn newly_sealed_directory_bucket_starts(
    from_next_log_id: u64,
    to_next_log_id: u64,
) -> Vec<u64> {
    sealed_directory_ranges(
        from_next_log_id,
        to_next_log_id,
        crate::domain::keys::LOG_DIRECTORY_BUCKET_SIZE,
        log_dir_bucket_start,
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

async fn compact_directory_sub_bucket<M: MetaStore>(
    meta_store: &M,
    sub_bucket_start: u64,
    epoch: u64,
) -> Result<()> {
    let page = meta_store
        .list_prefix(&log_dir_by_block_prefix(sub_bucket_start), None, usize::MAX)
        .await?;
    let mut fragments = Vec::with_capacity(page.keys.len());
    for key in page.keys {
        let Some(record) = meta_store.get(&key).await? else {
            continue;
        };
        fragments.push(decode_dir_by_block(&record.value)?);
    }
    if fragments.is_empty() {
        return Ok(());
    }
    fragments.sort_by_key(|fragment| fragment.block_num);

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
    put_artifact_meta(
        meta_store,
        &log_dir_sub_bucket_key(sub_bucket_start),
        encode_log_dir_bucket(&bucket),
        epoch,
    )
    .await?;
    Ok(())
}

async fn compact_directory_bucket<M: MetaStore>(
    meta_store: &M,
    bucket_start: u64,
    epoch: u64,
) -> Result<()> {
    let bucket_end = bucket_start.saturating_add(crate::domain::keys::LOG_DIRECTORY_BUCKET_SIZE);
    let mut sub_bucket_start = bucket_start;
    let mut boundaries = BTreeMap::<u64, u64>::new();
    let mut sentinel = None::<u64>;

    while sub_bucket_start < bucket_end {
        let Some(record) = meta_store
            .get(&log_dir_sub_bucket_key(sub_bucket_start))
            .await?
        else {
            sub_bucket_start = sub_bucket_start.saturating_add(LOG_DIRECTORY_SUB_BUCKET_SIZE);
            continue;
        };
        let sub_bucket = crate::codec::log::decode_log_dir_bucket(&record.value)?;
        for (index, first_log_id) in sub_bucket
            .first_log_ids
            .iter()
            .enumerate()
            .take(sub_bucket.first_log_ids.len().saturating_sub(1))
        {
            let block_num = sub_bucket.start_block + index as u64;
            match boundaries.entry(block_num) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(*first_log_id);
                }
                std::collections::btree_map::Entry::Occupied(entry) => {
                    if *entry.get() != *first_log_id {
                        return Err(Error::Decode(
                            "inconsistent directory bucket boundary across sub-buckets",
                        ));
                    }
                }
            }
        }
        if let Some(last) = sub_bucket.first_log_ids.last().copied() {
            sentinel = Some(match sentinel {
                Some(current) => current.max(last),
                None => last,
            });
        }
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

    put_artifact_meta(
        meta_store,
        &log_dir_bucket_key(bucket_start),
        encode_log_dir_bucket(&DirBucket {
            start_block,
            first_log_ids,
        }),
        epoch,
    )
    .await?;
    Ok(())
}
