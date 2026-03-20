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
    use crate::codec::log::{
        decode_block_log_header, decode_dir_by_block, decode_log, decode_log_dir_bucket,
        decode_stream_bitmap_meta, encode_log,
    };
    use crate::config::Config;
    use crate::core::ids::LogId;
    use crate::domain::keys::{
        LOG_DIRECTORY_BUCKET_SIZE, LOG_DIRECTORY_SUB_BUCKET_SIZE, STREAM_PAGE_LOCAL_ID_SPAN,
        bitmap_by_block_blob_key, bitmap_by_block_meta_key, bitmap_page_blob_key,
        bitmap_page_meta_key, block_hash_index_key, block_log_blob_key, block_log_header_key,
        block_record_key, log_dir_bucket_key, log_dir_by_block_key, log_dir_sub_bucket_key,
        log_local, stream_page_start_local,
    };
    use crate::logs::ingest::{
        compact_sealed_directory, compact_sealed_stream_pages, persist_log_artifacts,
        persist_log_block_record, persist_log_dir_by_block, persist_stream_fragments,
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
                .get_blob(&block_log_blob_key(7))
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
    fn persist_log_dir_by_block_and_compaction_cover_spanning_block() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let first_log_id = crate::domain::keys::LOG_DIRECTORY_SUB_BUCKET_SIZE - 3;
            let count = 8u32;

            persist_log_dir_by_block(&meta, 700, first_log_id, count, 3)
                .await
                .expect("persist fragments");
            compact_sealed_directory(&meta, first_log_id, count, first_log_id + count as u64, 3)
                .await
                .expect("compact directory");

            let fragment0 = meta
                .get(&log_dir_by_block_key(0, 700))
                .await
                .expect("read fragment0")
                .expect("fragment0");
            let fragment1 = meta
                .get(&log_dir_by_block_key(
                    crate::domain::keys::LOG_DIRECTORY_SUB_BUCKET_SIZE,
                    700,
                ))
                .await
                .expect("read fragment1")
                .expect("fragment1");
            let sub_bucket = meta
                .get(&log_dir_sub_bucket_key(0))
                .await
                .expect("read sub bucket")
                .expect("sub bucket");

            assert_eq!(
                decode_dir_by_block(&fragment0.value)
                    .expect("decode fragment0")
                    .block_num,
                700
            );
            assert_eq!(
                decode_dir_by_block(&fragment1.value)
                    .expect("decode fragment1")
                    .end_log_id_exclusive,
                first_log_id + count as u64
            );
            assert_eq!(
                decode_log_dir_bucket(&sub_bucket.value)
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
                .get(&bitmap_by_block_meta_key(&sid, first_page, block.block_num))
                .await
                .expect("read stream fragment meta")
                .expect("stream fragment meta");
            let fragment_blob = blob
                .get_blob(&bitmap_by_block_blob_key(&sid, first_page, block.block_num))
                .await
                .expect("read stream fragment blob")
                .expect("stream fragment blob");
            let page_meta = meta
                .get(&bitmap_page_meta_key(&sid, first_page))
                .await
                .expect("read stream page meta")
                .expect("stream page meta");
            let page_blob = blob
                .get_blob(&bitmap_page_blob_key(&sid, first_page))
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
    fn persist_log_block_record_writes_block_record_and_hash_index() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let block = sample_block(9, 5, vec![sample_log(9, 0, 0, 4)]);

            persist_log_block_record(&meta, &block, 33, 7)
                .await
                .expect("persist block metadata");

            assert!(
                meta.get(&block_record_key(9))
                    .await
                    .expect("block meta")
                    .is_some()
            );
            assert!(
                meta.get(&block_hash_index_key(&block.block_hash))
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

            persist_log_dir_by_block(&meta, 700, first_log_id, count, 3)
                .await
                .expect("persist fragments");
            compact_sealed_directory(&meta, first_log_id, count, first_log_id + count as u64, 3)
                .await
                .expect("compact directory");

            let bucket = meta
                .get(&log_dir_bucket_key(0))
                .await
                .expect("directory bucket")
                .expect("directory bucket present");
            let bucket = decode_log_dir_bucket(&bucket.value).expect("decode directory bucket");
            assert_eq!(bucket.start_block, 700);
            assert_eq!(
                bucket.first_log_ids,
                vec![first_log_id, first_log_id + count as u64]
            );
        });
    }
}
