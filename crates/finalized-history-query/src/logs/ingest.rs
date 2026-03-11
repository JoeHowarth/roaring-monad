use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use roaring::RoaringBitmap;

use crate::codec::finalized_state::{encode_block_meta, encode_u64};
use crate::codec::log::{
    decode_log_dir_fragment, encode_block_log_header, encode_log, encode_log_dir_fragment,
    encode_log_directory_bucket, encode_stream_bitmap_meta,
};
use crate::config::Config;
use crate::core::ids::LogId;
use crate::domain::keys::{
    LOG_DIRECTORY_SUB_BUCKET_SIZE, block_hash_to_num_key, block_log_header_key,
    block_logs_blob_key, block_meta_key, log_directory_bucket_key, log_directory_bucket_start,
    log_directory_fragment_key, log_directory_fragment_prefix, log_directory_sub_bucket_key,
    log_directory_sub_bucket_start, log_local, log_shard, stream_fragment_blob_key,
    stream_fragment_meta_key, stream_id, stream_page_blob_key, stream_page_meta_key,
    stream_page_start_local,
};
use crate::domain::types::{
    BlockLogHeader, BlockMeta, Log, LogDirFragment, LogDirectoryBucket, StreamBitmapMeta,
};
use crate::error::{Error, Result};
use crate::logs::types::Block;
use crate::store::traits::{BlobStore, CreateOutcome, FenceToken, MetaStore, PutCond};
use crate::streams::chunk::{ChunkBlob, decode_chunk, encode_chunk};

#[derive(Clone, Copy)]
enum ImmutableClass {
    Artifact,
    Summary,
}

async fn put_immutable_meta<M: MetaStore>(
    meta_store: &M,
    key: &[u8],
    value: Bytes,
    epoch: u64,
    class: ImmutableClass,
) -> Result<()> {
    let result = meta_store
        .put(key, value.clone(), PutCond::IfAbsent, FenceToken(epoch))
        .await?;
    if result.applied {
        return Ok(());
    }

    let Some(existing) = meta_store.get(key).await? else {
        return Err(Error::Backend(
            "immutable metadata create reported existing row but row is missing".to_string(),
        ));
    };
    if existing.value == value {
        return Ok(());
    }

    match class {
        ImmutableClass::Artifact => Err(Error::ArtifactConflict),
        ImmutableClass::Summary => Err(Error::SummaryConflict),
    }
}

async fn put_immutable_blob<B: BlobStore>(
    blob_store: &B,
    key: &[u8],
    value: Bytes,
    class: ImmutableClass,
) -> Result<()> {
    match blob_store.put_blob_if_absent(key, value.clone()).await? {
        CreateOutcome::Created => Ok(()),
        CreateOutcome::AlreadyExists => {
            let Some(existing) = blob_store.get_blob(key).await? else {
                return Err(Error::Backend(
                    "immutable blob create reported existing object but object is missing"
                        .to_string(),
                ));
            };
            if existing == value {
                Ok(())
            } else {
                match class {
                    ImmutableClass::Artifact => Err(Error::ArtifactConflict),
                    ImmutableClass::Summary => Err(Error::SummaryConflict),
                }
            }
        }
    }
}

pub async fn persist_log_artifacts<M: MetaStore, B: BlobStore>(
    _config: &Config,
    meta_store: &M,
    blob_store: &B,
    block_num: u64,
    logs: &[Log],
    _first_log_id: u64,
    epoch: u64,
) -> Result<()> {
    if logs.is_empty() {
        return Ok(());
    }

    let (block_blob, header) = encode_block_logs(logs)?;
    put_immutable_blob(
        blob_store,
        &block_logs_blob_key(block_num),
        block_blob,
        ImmutableClass::Artifact,
    )
    .await?;
    put_immutable_meta(
        meta_store,
        &block_log_header_key(block_num),
        encode_block_log_header(&header),
        epoch,
        ImmutableClass::Artifact,
    )
    .await?;
    Ok(())
}

pub async fn persist_log_block_metadata<M: MetaStore>(
    meta_store: &M,
    block: &Block,
    first_log_id: u64,
    epoch: u64,
) -> Result<()> {
    let block_meta = BlockMeta {
        block_hash: block.block_hash,
        parent_hash: block.parent_hash,
        first_log_id,
        count: block.logs.len() as u32,
    };

    put_immutable_meta(
        meta_store,
        &block_meta_key(block.block_num),
        encode_block_meta(&block_meta),
        epoch,
        ImmutableClass::Artifact,
    )
    .await?;

    put_immutable_meta(
        meta_store,
        &block_hash_to_num_key(&block.block_hash),
        encode_u64(block.block_num),
        epoch,
        ImmutableClass::Artifact,
    )
    .await?;

    Ok(())
}

pub async fn persist_log_directory_fragments<M: MetaStore>(
    meta_store: &M,
    block_num: u64,
    first_log_id: u64,
    count: u32,
    epoch: u64,
) -> Result<()> {
    let fragment = LogDirFragment {
        block_num,
        first_log_id,
        end_log_id_exclusive: first_log_id.saturating_add(u64::from(count)),
    };

    let mut sub_bucket_start = log_directory_sub_bucket_start(LogId::new(first_log_id));
    let last_sub_bucket_start = if count == 0 {
        sub_bucket_start
    } else {
        log_directory_sub_bucket_start(LogId::new(fragment.end_log_id_exclusive.saturating_sub(1)))
    };
    let encoded_fragment = encode_log_dir_fragment(&fragment);

    loop {
        put_immutable_meta(
            meta_store,
            &log_directory_fragment_key(sub_bucket_start, block_num),
            encoded_fragment.clone(),
            epoch,
            ImmutableClass::Artifact,
        )
        .await?;
        if sub_bucket_start == last_sub_bucket_start {
            break;
        }
        sub_bucket_start = sub_bucket_start.saturating_add(LOG_DIRECTORY_SUB_BUCKET_SIZE);
    }

    Ok(())
}

pub async fn compact_sealed_directory<M: MetaStore>(
    meta_store: &M,
    first_log_id: u64,
    count: u32,
    next_log_id: u64,
    epoch: u64,
) -> Result<()> {
    if count == 0 {
        return Ok(());
    }

    let mut sub_bucket_start = log_directory_sub_bucket_start(LogId::new(first_log_id));
    let last_sub_bucket_start =
        log_directory_sub_bucket_start(LogId::new(next_log_id.saturating_sub(1)));
    while sub_bucket_start <= last_sub_bucket_start {
        let sub_bucket_end = sub_bucket_start.saturating_add(LOG_DIRECTORY_SUB_BUCKET_SIZE);
        if sub_bucket_end <= next_log_id {
            compact_directory_sub_bucket(meta_store, sub_bucket_start, epoch).await?;
        }
        sub_bucket_start = sub_bucket_start.saturating_add(LOG_DIRECTORY_SUB_BUCKET_SIZE);
    }

    let bucket_start = log_directory_bucket_start(LogId::new(first_log_id));
    let bucket_end = bucket_start.saturating_add(crate::domain::keys::LOG_DIRECTORY_BUCKET_SIZE);
    if next_log_id >= bucket_end {
        compact_directory_bucket(meta_store, bucket_start, epoch).await?;
    }

    Ok(())
}

pub fn collect_stream_appends(block: &Block, first_log_id: u64) -> BTreeMap<String, Vec<u32>> {
    let mut out: BTreeMap<String, BTreeSet<u32>> = BTreeMap::new();

    for (index, log) in block.logs.iter().enumerate() {
        let global_log_id = LogId::new(first_log_id + index as u64);
        let shard = log_shard(global_log_id);
        let local = log_local(global_log_id).get();

        out.entry(stream_id("addr", &log.address, shard))
            .or_default()
            .insert(local);

        if let Some(topic0) = log.topics.first() {
            out.entry(stream_id("topic0", topic0, shard))
                .or_default()
                .insert(local);
        }

        for (topic_index, topic) in log.topics.iter().enumerate().skip(1).take(3) {
            let kind = match topic_index {
                1 => "topic1",
                2 => "topic2",
                3 => "topic3",
                _ => continue,
            };
            out.entry(stream_id(kind, topic, shard))
                .or_default()
                .insert(local);
        }
    }

    out.into_iter()
        .map(|(stream, values)| (stream, values.into_iter().collect()))
        .collect()
}

pub async fn persist_stream_fragments<M: MetaStore, B: BlobStore>(
    meta_store: &M,
    blob_store: &B,
    block: &Block,
    first_log_id: u64,
    epoch: u64,
) -> Result<Vec<(String, u32)>> {
    let mut touched_pages = BTreeSet::<(String, u32)>::new();

    for (stream, values) in collect_stream_appends(block, first_log_id) {
        let mut pages = BTreeMap::<u32, RoaringBitmap>::new();
        for value in values {
            let page_start = stream_page_start_local(value);
            pages.entry(page_start).or_default().insert(value);
        }

        for (page_start, bitmap) in pages {
            let count = bitmap.len() as u32;
            let min_local = bitmap.min().unwrap_or(page_start);
            let max_local = bitmap.max().unwrap_or(page_start);
            let meta = StreamBitmapMeta {
                block_num: block.block_num,
                count,
                min_local,
                max_local,
            };
            let chunk = ChunkBlob {
                min_local,
                max_local,
                count,
                crc32: 0,
                bitmap,
            };

            put_immutable_meta(
                meta_store,
                &stream_fragment_meta_key(&stream, page_start, block.block_num),
                encode_stream_bitmap_meta(&meta),
                epoch,
                ImmutableClass::Artifact,
            )
            .await?;
            put_immutable_blob(
                blob_store,
                &stream_fragment_blob_key(&stream, page_start, block.block_num),
                encode_chunk(&chunk)?,
                ImmutableClass::Artifact,
            )
            .await?;
            touched_pages.insert((stream.clone(), page_start));
        }
    }

    Ok(touched_pages.into_iter().collect())
}

pub async fn compact_sealed_stream_pages<M: MetaStore, B: BlobStore>(
    meta_store: &M,
    blob_store: &B,
    sealed_pages: &[(String, u32)],
    epoch: u64,
) -> Result<()> {
    if sealed_pages.is_empty() {
        return Ok(());
    }

    for (stream_id, page_start) in sealed_pages {
        compact_stream_page(meta_store, blob_store, stream_id, *page_start, epoch).await?;
    }

    Ok(())
}

fn encode_block_logs(logs: &[Log]) -> Result<(Bytes, BlockLogHeader)> {
    let mut out = Vec::<u8>::new();
    let mut offsets = Vec::with_capacity(logs.len() + 1);
    for log in logs {
        offsets.push(
            u32::try_from(out.len()).map_err(|_| Error::Decode("block log offset overflow"))?,
        );
        out.extend_from_slice(&encode_log(log));
    }
    offsets.push(u32::try_from(out.len()).map_err(|_| Error::Decode("block log size overflow"))?);
    Ok((Bytes::from(out), BlockLogHeader { offsets }))
}

async fn compact_directory_sub_bucket<M: MetaStore>(
    meta_store: &M,
    sub_bucket_start: u64,
    epoch: u64,
) -> Result<()> {
    let page = meta_store
        .list_prefix(
            &log_directory_fragment_prefix(sub_bucket_start),
            None,
            usize::MAX,
        )
        .await?;
    let mut fragments = Vec::with_capacity(page.keys.len());
    for key in page.keys {
        let Some(record) = meta_store.get(&key).await? else {
            continue;
        };
        fragments.push(decode_log_dir_fragment(&record.value)?);
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

    let bucket = LogDirectoryBucket {
        start_block: fragments[0].block_num,
        first_log_ids,
    };
    put_immutable_meta(
        meta_store,
        &log_directory_sub_bucket_key(sub_bucket_start),
        encode_log_directory_bucket(&bucket),
        epoch,
        ImmutableClass::Summary,
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
            .get(&log_directory_sub_bucket_key(sub_bucket_start))
            .await?
        else {
            sub_bucket_start = sub_bucket_start.saturating_add(LOG_DIRECTORY_SUB_BUCKET_SIZE);
            continue;
        };
        let sub_bucket = crate::codec::log::decode_log_directory_bucket(&record.value)?;
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

    put_immutable_meta(
        meta_store,
        &log_directory_bucket_key(bucket_start),
        encode_log_directory_bucket(&LogDirectoryBucket {
            start_block,
            first_log_ids,
        }),
        epoch,
        ImmutableClass::Summary,
    )
    .await?;
    Ok(())
}

pub async fn compact_stream_page<M: MetaStore, B: BlobStore>(
    meta_store: &M,
    blob_store: &B,
    stream_id: &str,
    page_start: u32,
    epoch: u64,
) -> Result<bool> {
    let page = meta_store
        .list_prefix(
            &crate::domain::keys::stream_fragment_meta_prefix(stream_id, page_start),
            None,
            usize::MAX,
        )
        .await?;
    let mut merged = RoaringBitmap::new();
    for key in page.keys {
        let block_num = read_u64_suffix(&key)?;
        let Some(blob) = blob_store
            .get_blob(&stream_fragment_blob_key(stream_id, page_start, block_num))
            .await?
        else {
            continue;
        };
        merged |= &decode_chunk(&blob)?.bitmap;
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
    let chunk = ChunkBlob {
        min_local,
        max_local,
        count,
        crc32: 0,
        bitmap: merged,
    };

    put_immutable_blob(
        blob_store,
        &stream_page_blob_key(stream_id, page_start),
        encode_chunk(&chunk)?,
        ImmutableClass::Summary,
    )
    .await?;
    put_immutable_meta(
        meta_store,
        &stream_page_meta_key(stream_id, page_start),
        encode_stream_bitmap_meta(&meta),
        epoch,
        ImmutableClass::Summary,
    )
    .await?;
    Ok(true)
}

pub fn parse_stream_shard(stream_id: &str) -> Option<crate::core::ids::LogShard> {
    let (_, shard_hex) = stream_id.rsplit_once('/')?;
    let raw = u64::from_str_radix(shard_hex, 16).ok()?;
    crate::core::ids::LogShard::new(raw).ok()
}

fn read_u64_suffix(key: &[u8]) -> Result<u64> {
    if key.len() < 8 {
        return Err(Error::Decode("short key suffix"));
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&key[key.len() - 8..]);
    Ok(u64::from_be_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use crate::codec::log::{
        decode_block_log_header, decode_log, decode_log_dir_fragment, decode_log_directory_bucket,
        decode_stream_bitmap_meta, encode_log,
    };
    use crate::config::Config;
    use crate::core::ids::LogId;
    use crate::domain::keys::{
        LOG_DIRECTORY_BUCKET_SIZE, LOG_DIRECTORY_SUB_BUCKET_SIZE, STREAM_PAGE_LOCAL_ID_SPAN,
        block_hash_to_num_key, block_log_header_key, block_logs_blob_key, block_meta_key,
        log_directory_bucket_key, log_directory_fragment_key, log_directory_sub_bucket_key,
        log_local, stream_fragment_blob_key, stream_fragment_meta_key, stream_page_blob_key,
        stream_page_meta_key, stream_page_start_local,
    };
    use crate::logs::ingest::{
        compact_sealed_directory, compact_sealed_stream_pages, persist_log_artifacts,
        persist_log_block_metadata, persist_log_directory_fragments, persist_stream_fragments,
    };
    use crate::logs::types::Block;
    use crate::store::blob::InMemoryBlobStore;
    use crate::store::meta::InMemoryMetaStore;
    use crate::store::traits::{BlobStore, MetaStore};
    use crate::streams::chunk::decode_chunk;
    use futures::executor::block_on;

    use super::collect_stream_appends;
    use crate::domain::types::Log;

    fn sample_log(block_num: u64, tx_idx: u32, log_idx: u32, seed: u8) -> Log {
        Log {
            address: [seed; 20],
            topics: vec![[seed.wrapping_add(1); 32]],
            data: vec![seed, seed.wrapping_add(2)],
            block_num,
            tx_idx,
            log_idx,
            block_hash: [seed.wrapping_add(3); 32],
        }
    }

    fn sample_block(block_num: u64, seed: u8, logs: Vec<Log>) -> Block {
        Block {
            block_num,
            block_hash: [seed; 32],
            parent_hash: [seed.wrapping_add(1); 32],
            logs,
        }
    }

    #[test]
    fn persist_log_artifacts_writes_block_keyed_storage() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let blob = InMemoryBlobStore::default();
            let config = Config::default();
            let logs = vec![sample_log(7, 0, 0, 1), sample_log(7, 0, 1, 2)];

            persist_log_artifacts(&config, &meta, &blob, 7, &logs, 11, 5)
                .await
                .expect("persist artifacts");

            let block_blob = blob
                .get_blob(&block_logs_blob_key(7))
                .await
                .expect("read block blob")
                .expect("block blob present");
            let header = meta
                .get(&block_log_header_key(7))
                .await
                .expect("read block header")
                .expect("block header present");
            let header = decode_block_log_header(&header.value).expect("decode header");

            assert_eq!(
                header.offsets,
                vec![
                    0,
                    encode_log(&logs[0]).len() as u32,
                    block_blob.len() as u32
                ]
            );
            assert_eq!(
                decode_log(&block_blob[header.offsets[0] as usize..header.offsets[1] as usize])
                    .expect("decode first"),
                logs[0]
            );
        });
    }

    #[test]
    fn persist_log_directory_fragments_and_compaction_cover_spanning_block() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let first_log_id = crate::domain::keys::LOG_DIRECTORY_SUB_BUCKET_SIZE - 3;
            let count = 8u32;

            persist_log_directory_fragments(&meta, 700, first_log_id, count, 3)
                .await
                .expect("persist fragments");
            compact_sealed_directory(&meta, first_log_id, count, first_log_id + count as u64, 3)
                .await
                .expect("compact directory");

            let fragment0 = meta
                .get(&log_directory_fragment_key(0, 700))
                .await
                .expect("read fragment0")
                .expect("fragment0");
            let fragment1 = meta
                .get(&log_directory_fragment_key(
                    crate::domain::keys::LOG_DIRECTORY_SUB_BUCKET_SIZE,
                    700,
                ))
                .await
                .expect("read fragment1")
                .expect("fragment1");
            let sub_bucket = meta
                .get(&log_directory_sub_bucket_key(0))
                .await
                .expect("read sub bucket")
                .expect("sub bucket");

            assert_eq!(
                decode_log_dir_fragment(&fragment0.value)
                    .expect("decode fragment0")
                    .block_num,
                700
            );
            assert_eq!(
                decode_log_dir_fragment(&fragment1.value)
                    .expect("decode fragment1")
                    .end_log_id_exclusive,
                first_log_id + count as u64
            );
            assert_eq!(
                decode_log_directory_bucket(&sub_bucket.value)
                    .expect("decode sub bucket")
                    .first_log_ids,
                vec![first_log_id, first_log_id + count as u64]
            );
        });
    }

    #[test]
    fn collect_stream_appends_groups_locals_by_index_stream() {
        let block = sample_block(1, 9, vec![sample_log(1, 0, 0, 1), sample_log(1, 0, 1, 2)]);
        let appends = collect_stream_appends(&block, 17);
        assert!(!appends.is_empty());
        assert!(
            appends
                .values()
                .all(|values| values.windows(2).all(|w| w[0] <= w[1]))
        );
    }

    #[test]
    fn persist_stream_fragments_and_page_compaction_write_immutable_page_artifacts() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let blob = InMemoryBlobStore::default();
            let block = sample_block(
                7,
                11,
                vec![
                    sample_log(7, 0, 0, 1),
                    sample_log(7, 0, 1, 1),
                    sample_log(7, 0, 2, 1),
                ],
            );
            let first_log_id = u64::from(STREAM_PAGE_LOCAL_ID_SPAN - 2);
            let touched_pages = persist_stream_fragments(&meta, &blob, &block, first_log_id, 5)
                .await
                .expect("persist stream fragments");
            compact_sealed_stream_pages(&meta, &blob, &touched_pages, 5)
                .await
                .expect("compact stream pages");

            let sid = collect_stream_appends(&block, first_log_id)
                .into_keys()
                .next()
                .expect("stream");
            let first_page = stream_page_start_local(log_local(LogId::new(first_log_id)).get());
            let fragment = meta
                .get(&stream_fragment_meta_key(&sid, first_page, block.block_num))
                .await
                .expect("read stream fragment meta")
                .expect("stream fragment meta");
            let fragment_blob = blob
                .get_blob(&stream_fragment_blob_key(&sid, first_page, block.block_num))
                .await
                .expect("read stream fragment blob")
                .expect("stream fragment blob");
            let page_meta = meta
                .get(&stream_page_meta_key(&sid, first_page))
                .await
                .expect("read stream page meta")
                .expect("stream page meta");
            let page_blob = blob
                .get_blob(&stream_page_blob_key(&sid, first_page))
                .await
                .expect("read stream page blob")
                .expect("stream page blob");

            assert_eq!(
                decode_stream_bitmap_meta(&fragment.value)
                    .expect("decode stream fragment meta")
                    .block_num,
                block.block_num
            );
            assert!(
                decode_chunk(&fragment_blob)
                    .expect("decode fragment blob")
                    .count
                    > 0
            );
            assert!(
                decode_stream_bitmap_meta(&page_meta.value)
                    .expect("decode stream page meta")
                    .count
                    > 0
            );
            assert!(
                decode_chunk(&page_blob)
                    .expect("decode stream page blob")
                    .count
                    > 0
            );
        });
    }

    #[test]
    fn persist_log_block_metadata_writes_block_meta_and_hash_index() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let block = sample_block(9, 5, vec![sample_log(9, 0, 0, 4)]);

            persist_log_block_metadata(&meta, &block, 33, 7)
                .await
                .expect("persist block metadata");

            assert!(
                meta.get(&block_meta_key(9))
                    .await
                    .expect("block meta")
                    .is_some()
            );
            assert!(
                meta.get(&block_hash_to_num_key(&block.block_hash))
                    .await
                    .expect("hash index")
                    .is_some()
            );
        });
    }

    #[test]
    fn directory_bucket_compaction_writes_canonical_1m_summary_when_boundary_seals() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let first_log_id = LOG_DIRECTORY_BUCKET_SIZE - LOG_DIRECTORY_SUB_BUCKET_SIZE - 2;
            let count = (LOG_DIRECTORY_SUB_BUCKET_SIZE + 5) as u32;

            persist_log_directory_fragments(&meta, 700, first_log_id, count, 3)
                .await
                .expect("persist fragments");
            compact_sealed_directory(&meta, first_log_id, count, first_log_id + count as u64, 3)
                .await
                .expect("compact directory");

            let bucket = meta
                .get(&log_directory_bucket_key(0))
                .await
                .expect("directory bucket")
                .expect("directory bucket present");
            let bucket =
                decode_log_directory_bucket(&bucket.value).expect("decode directory bucket");
            assert_eq!(bucket.start_block, 700);
            assert_eq!(
                bucket.first_log_ids,
                vec![first_log_id, first_log_id + count as u64]
            );
        });
    }
}
