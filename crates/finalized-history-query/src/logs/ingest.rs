use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;

use crate::Log;
use crate::config::Config;
use crate::core::ids::LogId;
use crate::error::{Error, Result};
use crate::family::FinalizedBlock;
use crate::ingest::bitmap_pages;
use crate::kernel::codec::StorageCodec;
use crate::kernel::sharded_streams::sharded_stream_id;
use crate::logs::keys::STREAM_PAGE_LOCAL_ID_SPAN;
use crate::logs::types::BlockLogHeader;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;

pub fn collect_stream_appends(
    block: &FinalizedBlock,
    first_log_id: u64,
) -> BTreeMap<String, Vec<u32>> {
    let mut out: BTreeMap<String, BTreeSet<u32>> = BTreeMap::new();

    for (index, log) in block.logs.iter().enumerate() {
        let global_log_id = LogId::new(first_log_id + index as u64);
        let shard = global_log_id.shard();
        let local = global_log_id.local().get();

        out.entry(sharded_stream_id("addr", &log.address, shard.get()))
            .or_default()
            .insert(local);

        if let Some(topic0) = log.topics.first() {
            out.entry(sharded_stream_id("topic0", topic0, shard.get()))
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
            out.entry(sharded_stream_id(kind, topic, shard.get()))
                .or_default()
                .insert(local);
        }
    }

    out.into_iter()
        .map(|(stream, values)| (stream, values.into_iter().collect()))
        .collect()
}

pub async fn persist_stream_fragments<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    block: &FinalizedBlock,
    first_log_id: u64,
) -> Result<Vec<(String, u32)>> {
    let grouped_values = collect_stream_appends(block, first_log_id)
        .into_iter()
        .flat_map(|(stream, values)| values.into_iter().map(move |value| (stream.clone(), value)));
    bitmap_pages::persist_stream_fragments(
        &tables.log_streams,
        block.block_num,
        grouped_values,
        STREAM_PAGE_LOCAL_ID_SPAN,
    )
    .await
}

pub async fn persist_log_artifacts<M: MetaStore, B: BlobStore>(
    _config: &Config,
    tables: &Tables<M, B>,
    block_num: u64,
    logs: &[Log],
    _first_log_id: u64,
) -> Result<()> {
    let mut out = Vec::<u8>::new();
    let mut offsets = Vec::with_capacity(logs.len() + 1);
    for log in logs {
        offsets.push(
            u32::try_from(out.len()).map_err(|_| Error::Decode("block log offset overflow"))?,
        );
        out.extend_from_slice(&log.encode());
    }
    offsets.push(u32::try_from(out.len()).map_err(|_| Error::Decode("block log size overflow"))?);
    let block_blob = Bytes::from(out);
    let header = BlockLogHeader { offsets };
    tables
        .point_log_payloads
        .put_block(block_num, block_blob, &header)
        .await
}

pub async fn persist_log_dir_by_block<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    block_num: u64,
    first_log_id: u64,
    count: u32,
) -> Result<()> {
    tables
        .log_dir
        .persist_block_fragment(block_num, first_log_id, count)
        .await
}

#[cfg(test)]
mod tests {
    use crate::config::Config;
    use crate::core::ids::LogId;
    use crate::family::FinalizedBlock;
    use crate::kernel::codec::StorageCodec;
    use crate::kernel::table_specs::{PointTableSpec, ScannableTableSpec};
    use crate::logs::codec::validate_log;
    use crate::logs::keys::{
        LOG_DIRECTORY_BUCKET_SIZE, LOG_DIRECTORY_SUB_BUCKET_SIZE, STREAM_PAGE_LOCAL_ID_SPAN,
    };
    use crate::logs::table_specs::{
        BitmapByBlockSpec, BitmapPageBlobSpec, BitmapPageMetaSpec, BlobTableSpec, BlockLogBlobSpec,
        BlockLogHeaderSpec, LogDirBucketSpec, LogDirByBlockSpec, LogDirSubBucketSpec,
    };
    use crate::store::blob::InMemoryBlobStore;
    use crate::store::meta::InMemoryMetaStore;
    use crate::store::traits::{BlobStore, MetaStore};
    use crate::streams::decode_bitmap_blob;
    use crate::tables::Tables;
    use futures::executor::block_on;

    use super::{
        collect_stream_appends, persist_log_artifacts, persist_log_dir_by_block,
        persist_stream_fragments,
    };
    use crate::ingest::bitmap_pages;
    use crate::ingest::primary_dir::compact_sealed_primary_directory;
    use crate::kernel::sharded_streams::page_start_local;
    use crate::logs::types::{BlockLogHeader, DirBucket, DirByBlock, Log, StreamBitmapMeta};

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

    fn sample_block(block_num: u64, seed: u8, logs: Vec<Log>) -> FinalizedBlock {
        FinalizedBlock {
            block_num,
            block_hash: [seed; 32],
            parent_hash: [seed.wrapping_add(1); 32],
            logs,
            txs: Vec::new(),
            trace_rlp: Vec::new(),
        }
    }

    #[test]
    fn persist_log_artifacts_writes_block_keyed_storage() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let blob = InMemoryBlobStore::default();
            let tables = Tables::without_cache(meta.clone(), blob.clone());
            let config = Config::default();
            let logs = vec![sample_log(7, 0, 0, 1), sample_log(7, 0, 1, 2)];

            persist_log_artifacts(&config, &tables, 7, &logs, 11)
                .await
                .expect("persist artifacts");

            let block_blob = blob
                .get_blob(BlockLogBlobSpec::TABLE, &BlockLogBlobSpec::key(7))
                .await
                .expect("read block blob")
                .expect("block blob present");
            let header = meta
                .get(BlockLogHeaderSpec::TABLE, &BlockLogHeaderSpec::key(7))
                .await
                .expect("read block header")
                .expect("block header present");
            let header = BlockLogHeader::decode(&header.value).expect("decode header");

            assert_eq!(
                header.offsets,
                vec![0, logs[0].encode().len() as u32, block_blob.len() as u32]
            );
            assert_eq!(
                Log::decode(&block_blob[header.offsets[0] as usize..header.offsets[1] as usize])
                    .expect("decode first"),
                logs[0]
            );
            assert!(validate_log(&logs[0]));
        });
    }

    #[test]
    fn persist_log_dir_by_block_and_compaction_cover_spanning_block() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let tables = Tables::without_cache(meta.clone(), InMemoryBlobStore::default());
            let first_log_id = crate::logs::keys::LOG_DIRECTORY_SUB_BUCKET_SIZE - 3;
            let count = 8u32;

            persist_log_dir_by_block(&tables, 700, first_log_id, count)
                .await
                .expect("persist fragments");
            compact_sealed_primary_directory(
                &tables.log_dir,
                first_log_id,
                count,
                first_log_id + count as u64,
            )
            .await
            .expect("compact directory");

            let fragment0 = meta
                .scan_get(
                    LogDirByBlockSpec::TABLE,
                    &LogDirByBlockSpec::partition(0),
                    &LogDirByBlockSpec::clustering(700),
                )
                .await
                .expect("read fragment0")
                .expect("fragment0");
            let fragment1 = meta
                .scan_get(
                    LogDirByBlockSpec::TABLE,
                    &LogDirByBlockSpec::partition(crate::logs::keys::LOG_DIRECTORY_SUB_BUCKET_SIZE),
                    &LogDirByBlockSpec::clustering(700),
                )
                .await
                .expect("read fragment1")
                .expect("fragment1");
            let sub_bucket = meta
                .get(LogDirSubBucketSpec::TABLE, &LogDirSubBucketSpec::key(0))
                .await
                .expect("read sub bucket")
                .expect("sub bucket");

            assert_eq!(
                DirByBlock::decode(&fragment0.value)
                    .expect("decode fragment0")
                    .block_num,
                700
            );
            assert_eq!(
                DirByBlock::decode(&fragment1.value)
                    .expect("decode fragment1")
                    .end_primary_id_exclusive,
                first_log_id + count as u64
            );
            assert_eq!(
                DirBucket::decode(&sub_bucket.value)
                    .expect("decode sub bucket")
                    .first_primary_ids,
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
            let tables = Tables::without_cache(meta.clone(), blob.clone());
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
            let touched_pages = persist_stream_fragments(&tables, &block, first_log_id)
                .await
                .expect("persist stream fragments");
            for (stream_id, page_start) in &touched_pages {
                let _ = bitmap_pages::compact_stream_page(
                    &tables.log_streams,
                    stream_id,
                    *page_start,
                    |count, min_local, max_local| StreamBitmapMeta {
                        count,
                        min_local,
                        max_local,
                    },
                )
                .await
                .expect("compact stream page");
            }

            let sid = collect_stream_appends(&block, first_log_id)
                .into_keys()
                .next()
                .expect("stream");
            let first_page = page_start_local(
                LogId::new(first_log_id).local().get(),
                STREAM_PAGE_LOCAL_ID_SPAN,
            );
            let fragment = meta
                .scan_get(
                    BitmapByBlockSpec::TABLE,
                    &BitmapByBlockSpec::partition(&sid, first_page),
                    &BitmapByBlockSpec::clustering(block.block_num),
                )
                .await
                .expect("read stream fragment")
                .expect("stream fragment");
            let page_meta = meta
                .get(
                    BitmapPageMetaSpec::TABLE,
                    &BitmapPageMetaSpec::key(&sid, first_page),
                )
                .await
                .expect("read stream page meta")
                .expect("stream page meta");
            let page_blob = blob
                .get_blob(
                    BitmapPageBlobSpec::TABLE,
                    &BitmapPageBlobSpec::key(&sid, first_page),
                )
                .await
                .expect("read stream page blob")
                .expect("stream page blob");

            assert!(
                decode_bitmap_blob(&fragment.value)
                    .expect("decode fragment")
                    .count
                    > 0
            );
            assert!(
                StreamBitmapMeta::decode(&page_meta.value)
                    .expect("decode stream page meta")
                    .count
                    > 0
            );
            assert!(
                decode_bitmap_blob(&page_blob)
                    .expect("decode stream page blob")
                    .count
                    > 0
            );
        });
    }

    #[test]
    fn directory_bucket_compaction_writes_canonical_1m_summary_when_boundary_seals() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let tables = Tables::without_cache(meta.clone(), InMemoryBlobStore::default());
            let first_log_id = LOG_DIRECTORY_BUCKET_SIZE - LOG_DIRECTORY_SUB_BUCKET_SIZE - 2;
            let count = (LOG_DIRECTORY_SUB_BUCKET_SIZE + 5) as u32;

            persist_log_dir_by_block(&tables, 700, first_log_id, count)
                .await
                .expect("persist fragments");
            compact_sealed_primary_directory(
                &tables.log_dir,
                first_log_id,
                count,
                first_log_id + count as u64,
            )
            .await
            .expect("compact directory");

            let bucket = meta
                .get(LogDirBucketSpec::TABLE, &LogDirBucketSpec::key(0))
                .await
                .expect("directory bucket")
                .expect("directory bucket present");
            let bucket = DirBucket::decode(&bucket.value).expect("decode directory bucket");
            assert_eq!(bucket.start_block, 700);
            assert_eq!(
                bucket.first_primary_ids,
                vec![first_log_id, first_log_id + count as u64]
            );
        });
    }
}
