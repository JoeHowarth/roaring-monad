use std::collections::BTreeMap;

use roaring::RoaringBitmap;

use crate::error::{Error, Result};
use crate::store::traits::{BlobStore, MetaStore};
use crate::streams::{BitmapBlob, decode_bitmap_blob, encode_bitmap_blob};
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

fn sealed_ranges(from_next: u64, to_next: u64, span: u64, align: impl Fn(u64) -> u64) -> Vec<u64> {
    if to_next <= from_next {
        return Vec::new();
    }
    let mut current = align(from_next);
    let mut out = Vec::new();
    loop {
        let end = current.saturating_add(span);
        if end > to_next {
            break;
        }
        if end > from_next {
            out.push(current);
        }
        current = current.saturating_add(span);
    }
    out
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
