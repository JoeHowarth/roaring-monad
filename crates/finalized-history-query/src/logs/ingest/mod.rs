mod artifact;
mod compaction;
mod stream;

pub use artifact::{
    parse_stream_shard, persist_log_artifacts, persist_log_block_record, persist_log_dir_by_block,
};
pub use compaction::{
    compact_newly_sealed_directory, compact_sealed_directory, newly_sealed_directory_bucket_starts,
    newly_sealed_directory_sub_bucket_starts,
};
pub use stream::{
    collect_stream_appends, compact_sealed_stream_pages, compact_stream_page,
    persist_stream_fragments,
};

#[cfg(test)]
mod tests {
    use crate::config::Config;
    use crate::core::ids::LogId;
    use crate::logs::codec::validate_log;
    use crate::logs::ingest::{
        compact_sealed_directory, compact_sealed_stream_pages, persist_log_artifacts,
        persist_log_block_record, persist_log_dir_by_block, persist_stream_fragments,
    };
    use crate::logs::keys::{
        BITMAP_BY_BLOCK_TABLE, BITMAP_PAGE_META_TABLE, BLOCK_HASH_INDEX_TABLE,
        BLOCK_LOG_HEADER_TABLE, BLOCK_RECORD_TABLE, LOG_DIR_BUCKET_TABLE, LOG_DIR_BY_BLOCK_TABLE,
        LOG_DIR_SUB_BUCKET_TABLE, LOG_DIRECTORY_BUCKET_SIZE, LOG_DIRECTORY_SUB_BUCKET_SIZE,
        STREAM_PAGE_LOCAL_ID_SPAN, block_hash_index_suffix,
    };
    use crate::logs::table_specs::{
        self, BitmapByBlockSpec, BitmapPageBlobSpec, BitmapPageMetaSpec, BlobTableSpec,
        BlockLogBlobSpec, BlockLogHeaderSpec, BlockRecordSpec, LogDirBucketSpec, LogDirByBlockSpec,
        LogDirSubBucketSpec,
    };
    use crate::logs::types::Block;
    use crate::store::blob::InMemoryBlobStore;
    use crate::store::meta::InMemoryMetaStore;
    use crate::store::traits::{BlobStore, MetaStore};
    use crate::streams::bitmap_blob::decode_bitmap_blob;
    use futures::executor::block_on;

    use super::collect_stream_appends;
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

            persist_log_artifacts(&config, &meta, &blob, 7, &logs, 11)
                .await
                .expect("persist artifacts");

            let block_blob = blob
                .get_blob(BlockLogBlobSpec::TABLE, &BlockLogBlobSpec::key(7))
                .await
                .expect("read block blob")
                .expect("block blob present");
            let header = meta
                .get(BLOCK_LOG_HEADER_TABLE, &BlockLogHeaderSpec::key(7))
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
            let first_log_id = crate::logs::keys::LOG_DIRECTORY_SUB_BUCKET_SIZE - 3;
            let count = 8u32;

            persist_log_dir_by_block(&meta, 700, first_log_id, count)
                .await
                .expect("persist fragments");
            compact_sealed_directory(&meta, first_log_id, count, first_log_id + count as u64)
                .await
                .expect("compact directory");

            let fragment0 = meta
                .scan_get(
                    LOG_DIR_BY_BLOCK_TABLE,
                    &LogDirByBlockSpec::partition(0),
                    &LogDirByBlockSpec::clustering(700),
                )
                .await
                .expect("read fragment0")
                .expect("fragment0");
            let fragment1 = meta
                .scan_get(
                    LOG_DIR_BY_BLOCK_TABLE,
                    &LogDirByBlockSpec::partition(crate::logs::keys::LOG_DIRECTORY_SUB_BUCKET_SIZE),
                    &LogDirByBlockSpec::clustering(700),
                )
                .await
                .expect("read fragment1")
                .expect("fragment1");
            let sub_bucket = meta
                .get(LOG_DIR_SUB_BUCKET_TABLE, &LogDirSubBucketSpec::key(0))
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
                    .end_log_id_exclusive,
                first_log_id + count as u64
            );
            assert_eq!(
                DirBucket::decode(&sub_bucket.value)
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
            let touched_pages = persist_stream_fragments(&meta, &blob, &block, first_log_id)
                .await
                .expect("persist stream fragments");
            compact_sealed_stream_pages(&meta, &blob, &touched_pages)
                .await
                .expect("compact stream pages");

            let sid = collect_stream_appends(&block, first_log_id)
                .into_keys()
                .next()
                .expect("stream");
            let first_page = table_specs::stream_page_start_local(
                table_specs::log_local(LogId::new(first_log_id)).get(),
            );
            let fragment = meta
                .scan_get(
                    BITMAP_BY_BLOCK_TABLE,
                    &BitmapByBlockSpec::partition(&sid, first_page),
                    &BitmapByBlockSpec::clustering(block.block_num),
                )
                .await
                .expect("read stream fragment")
                .expect("stream fragment");
            let page_meta = meta
                .get(
                    BITMAP_PAGE_META_TABLE,
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
    fn persist_log_block_record_writes_block_record_and_hash_index() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let block = sample_block(9, 5, vec![sample_log(9, 0, 0, 4)]);

            persist_log_block_record(&meta, &block, 33)
                .await
                .expect("persist block metadata");

            assert!(
                meta.get(BLOCK_RECORD_TABLE, &BlockRecordSpec::key(9))
                    .await
                    .expect("block meta")
                    .is_some()
            );
            assert!(
                meta.get(
                    BLOCK_HASH_INDEX_TABLE,
                    &block_hash_index_suffix(&block.block_hash)
                )
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

            persist_log_dir_by_block(&meta, 700, first_log_id, count)
                .await
                .expect("persist fragments");
            compact_sealed_directory(&meta, first_log_id, count, first_log_id + count as u64)
                .await
                .expect("compact directory");

            let bucket = meta
                .get(LOG_DIR_BUCKET_TABLE, &LogDirBucketSpec::key(0))
                .await
                .expect("directory bucket")
                .expect("directory bucket present");
            let bucket = DirBucket::decode(&bucket.value).expect("decode directory bucket");
            assert_eq!(bucket.start_block, 700);
            assert_eq!(
                bucket.first_log_ids,
                vec![first_log_id, first_log_id + count as u64]
            );
        });
    }
}
