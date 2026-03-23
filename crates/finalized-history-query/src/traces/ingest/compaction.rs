use roaring::RoaringBitmap;

use crate::error::Result;
use crate::kernel::compaction::{
    compact_directory_bucket_from_sub_buckets, compact_directory_sub_bucket_from_fragments,
    sealed_ranges,
};
use crate::kernel::sharded_streams::compacted_bitmap_blob;
use crate::store::traits::{BlobStore, MetaStore};
use crate::streams::{decode_bitmap_blob, encode_bitmap_blob};
use crate::tables::Tables;
use crate::traces::keys::{TRACE_DIRECTORY_BUCKET_SIZE, TRACE_DIRECTORY_SUB_BUCKET_SIZE};
use crate::traces::types::{DirBucket, StreamBitmapMeta};

pub async fn compact_newly_sealed_trace_directory<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    from_next_trace_id: u64,
    next_trace_id: u64,
) -> Result<()> {
    if next_trace_id <= from_next_trace_id {
        return Ok(());
    }

    for sub_bucket_start in newly_sealed_sub_bucket_starts(from_next_trace_id, next_trace_id) {
        compact_trace_directory_sub_bucket(tables, sub_bucket_start).await?;
    }

    for bucket_start in newly_sealed_bucket_starts(from_next_trace_id, next_trace_id) {
        compact_trace_directory_bucket(tables, bucket_start).await?;
    }

    Ok(())
}

fn newly_sealed_sub_bucket_starts(from_next_trace_id: u64, to_next_trace_id: u64) -> Vec<u64> {
    sealed_ranges(
        from_next_trace_id,
        to_next_trace_id,
        TRACE_DIRECTORY_SUB_BUCKET_SIZE,
        crate::traces::keys::trace_sub_bucket_start,
    )
}

fn newly_sealed_bucket_starts(from_next_trace_id: u64, to_next_trace_id: u64) -> Vec<u64> {
    sealed_ranges(
        from_next_trace_id,
        to_next_trace_id,
        TRACE_DIRECTORY_BUCKET_SIZE,
        crate::traces::keys::trace_bucket_start,
    )
}

pub async fn compact_sealed_trace_stream_pages<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    touched_pages: &[(String, u32)],
    from_next_trace_id: u64,
    next_trace_id: u64,
) -> Result<()> {
    for (stream_id, page_start) in touched_pages {
        if is_page_sealed(*page_start, from_next_trace_id, next_trace_id) {
            compact_trace_stream_page(tables, stream_id, *page_start).await?;
        }
    }
    Ok(())
}

fn is_page_sealed(page_start_local: u32, from_next_trace_id: u64, next_trace_id: u64) -> bool {
    use crate::traces::keys::{TRACE_LOCAL_ID_MASK, TRACE_STREAM_PAGE_LOCAL_ID_SPAN};
    let page_end_local = page_start_local.saturating_add(TRACE_STREAM_PAGE_LOCAL_ID_SPAN);
    let from_local = (from_next_trace_id & TRACE_LOCAL_ID_MASK) as u32;
    let to_local = (next_trace_id & TRACE_LOCAL_ID_MASK) as u32;
    // Page is sealed if next_trace_id moved past the page boundary.
    // This can happen either within the same shard (to_local >= page_end)
    // or by crossing a shard boundary (to_local < from_local).
    to_local >= page_end_local || to_local < from_local
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

    let Some((count, bitmap_blob)) = compacted_bitmap_blob(merged, page_start) else {
        return Ok(false);
    };
    let meta = StreamBitmapMeta {
        block_num: 0,
        count,
        min_local: bitmap_blob.min_local,
        max_local: bitmap_blob.max_local,
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

    let Some((start_block, first_trace_ids)) = compact_directory_sub_bucket_from_fragments(
        &fragments,
        |fragment| fragment.block_num,
        |fragment| fragment.first_trace_id,
        |fragment| fragment.end_trace_id_exclusive,
    ) else {
        return Ok(());
    };

    let bucket = DirBucket {
        start_block,
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
    let mut sub_buckets = Vec::new();

    while sub_bucket_start < bucket_end {
        let Some(sub_bucket) = tables.trace_dir_sub_buckets().get(sub_bucket_start).await? else {
            sub_bucket_start = sub_bucket_start.saturating_add(TRACE_DIRECTORY_SUB_BUCKET_SIZE);
            continue;
        };
        sub_buckets.push(sub_bucket);
        sub_bucket_start = sub_bucket_start.saturating_add(TRACE_DIRECTORY_SUB_BUCKET_SIZE);
    }

    let Some((start_block, first_trace_ids)) = compact_directory_bucket_from_sub_buckets(
        &sub_buckets,
        |sub_bucket| sub_bucket.start_block,
        |sub_bucket| sub_bucket.first_trace_ids.len(),
        |sub_bucket, index| sub_bucket.first_trace_ids[index],
        "trace directory bucket missing sentinel",
        "inconsistent trace directory bucket boundary across sub-buckets",
        "missing trace directory bucket start block",
    )?
    else {
        return Ok(());
    };

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
