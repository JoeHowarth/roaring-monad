use std::collections::BTreeMap;

use roaring::RoaringBitmap;

use crate::error::{Error, Result};
use crate::store::traits::{BlobStore, MetaStore};
use crate::streams::{BitmapBlob, decode_bitmap_blob, encode_bitmap_blob};
use crate::tables::Tables;
use crate::traces::keys::{TRACE_DIRECTORY_BUCKET_SIZE, TRACE_DIRECTORY_SUB_BUCKET_SIZE};
use crate::traces::table_specs::{TraceDirBucketSpec, TraceDirSubBucketSpec};
use crate::traces::types::{DirBucket, StreamBitmapMeta};

pub async fn compact_trace_directory_for_range<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    first_trace_id: u64,
    count: u32,
) -> Result<()> {
    if count == 0 {
        return Ok(());
    }

    let end_trace_id_exclusive = first_trace_id.saturating_add(u64::from(count));
    let first_sub_bucket = TraceDirSubBucketSpec::sub_bucket_start(first_trace_id);
    let last_sub_bucket =
        TraceDirSubBucketSpec::sub_bucket_start(end_trace_id_exclusive.saturating_sub(1));
    let mut sub_bucket_start = first_sub_bucket;
    while sub_bucket_start <= last_sub_bucket {
        compact_trace_directory_sub_bucket(tables, sub_bucket_start).await?;
        sub_bucket_start = sub_bucket_start.saturating_add(TRACE_DIRECTORY_SUB_BUCKET_SIZE);
    }

    let first_bucket = TraceDirBucketSpec::bucket_start(first_trace_id);
    let last_bucket = TraceDirBucketSpec::bucket_start(end_trace_id_exclusive.saturating_sub(1));
    let mut bucket_start = first_bucket;
    while bucket_start <= last_bucket {
        compact_trace_directory_bucket(tables, bucket_start).await?;
        bucket_start = bucket_start.saturating_add(TRACE_DIRECTORY_BUCKET_SIZE);
    }

    Ok(())
}

pub async fn compact_trace_stream_pages<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    touched_pages: &[(String, u32)],
) -> Result<()> {
    for (stream_id, page_start) in touched_pages {
        compact_trace_stream_page(tables, stream_id, *page_start).await?;
    }
    Ok(())
}

async fn compact_trace_stream_page<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    stream_id: &str,
    page_start: u32,
) -> Result<bool> {
    let mut merged = RoaringBitmap::new();
    for bytes in tables
        .trace_bitmap_by_block()
        .load_page_fragments(stream_id, page_start)
        .await?
    {
        merged |= &decode_bitmap_blob(&bytes)?.bitmap;
    }
    if merged.is_empty() {
        return Ok(false);
    }

    let count = merged.len() as u32;
    let min_local = merged.min().unwrap_or(page_start);
    let max_local = merged.max().unwrap_or(page_start);
    let meta = StreamBitmapMeta {
        block_num: 0,
        count,
        min_local,
        max_local,
    };
    let bitmap_blob = BitmapBlob {
        min_local,
        max_local,
        count,
        crc32: 0,
        bitmap: merged,
    };

    tables
        .trace_bitmap_page_blobs()
        .put(stream_id, page_start, encode_bitmap_blob(&bitmap_blob)?)
        .await?;
    tables
        .trace_bitmap_page_meta()
        .put(stream_id, page_start, &meta)
        .await?;
    Ok(true)
}

async fn compact_trace_directory_sub_bucket<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    sub_bucket_start: u64,
) -> Result<()> {
    let fragments = tables
        .trace_directory_fragments()
        .load_sub_bucket_fragments(sub_bucket_start)
        .await?;
    if fragments.is_empty() {
        return Ok(());
    }

    let mut first_trace_ids = Vec::with_capacity(fragments.len() + 1);
    for fragment in &fragments {
        first_trace_ids.push(fragment.first_trace_id);
    }
    let sentinel = fragments
        .last()
        .map(|fragment| fragment.end_trace_id_exclusive)
        .unwrap_or(sub_bucket_start);
    first_trace_ids.push(sentinel);

    let bucket = DirBucket {
        start_block: fragments[0].block_num,
        first_trace_ids,
    };
    tables
        .trace_dir_sub_buckets()
        .put(sub_bucket_start, &bucket)
        .await?;
    Ok(())
}

async fn compact_trace_directory_bucket<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    bucket_start: u64,
) -> Result<()> {
    let bucket_end = bucket_start.saturating_add(TRACE_DIRECTORY_BUCKET_SIZE);
    let mut sub_bucket_start = bucket_start;
    let mut boundaries = BTreeMap::<u64, u64>::new();
    let mut sentinel = None::<u64>;

    while sub_bucket_start < bucket_end {
        let Some(sub_bucket) = tables.trace_dir_sub_buckets().get(sub_bucket_start).await? else {
            sub_bucket_start = sub_bucket_start.saturating_add(TRACE_DIRECTORY_SUB_BUCKET_SIZE);
            continue;
        };
        for index in 0..sub_bucket.first_trace_ids.len().saturating_sub(1) {
            let block_num = sub_bucket.start_block + index as u64;
            let first_trace_id = sub_bucket.first_trace_ids[index];
            match boundaries.entry(block_num) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(first_trace_id);
                }
                std::collections::btree_map::Entry::Occupied(entry) => {
                    if *entry.get() != first_trace_id {
                        return Err(Error::Decode(
                            "inconsistent trace directory bucket boundary across sub-buckets",
                        ));
                    }
                }
            }
        }
        let last = *sub_bucket
            .first_trace_ids
            .last()
            .ok_or(Error::Decode("trace directory bucket missing sentinel"))?;
        sentinel = Some(match sentinel {
            Some(current) => current.max(last),
            None => last,
        });
        sub_bucket_start = sub_bucket_start.saturating_add(TRACE_DIRECTORY_SUB_BUCKET_SIZE);
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
        .ok_or(Error::Decode("missing trace directory bucket start block"))?;
    let mut first_trace_ids = boundaries.into_values().collect::<Vec<_>>();
    first_trace_ids.push(sentinel);

    tables
        .trace_dir_buckets()
        .put(
            bucket_start,
            &DirBucket {
                start_block,
                first_trace_ids,
            },
        )
        .await?;
    Ok(())
}
