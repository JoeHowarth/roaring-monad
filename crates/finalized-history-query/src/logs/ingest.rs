use std::collections::BTreeMap;

use bytes::Bytes;

use crate::core::ids::LogId;
use crate::core::offsets::BucketedOffsets;
use crate::error::{Error, Result};
use crate::family::FinalizedBlock;
use crate::ingest::bitmap_pages;
use crate::ingest::indexed_family::{collect_grouped_stream_appends, iter_grouped_stream_appends};
use crate::kernel::codec::StorageCodec;
use crate::kernel::sharded_streams::sharded_stream_id;
use crate::logs::STREAM_PAGE_LOCAL_ID_SPAN;
use crate::logs::codec::validate_log;
use crate::logs::types::{BlockLogHeader, Log};
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;

#[derive(Debug, Clone)]
pub struct LogIngestPlan {
    pub header: BlockLogHeader,
    pub block_blob: Bytes,
    pub stream_appends_by_stream: BTreeMap<String, Vec<u32>>,
}

pub fn plan_log_ingest(block: &FinalizedBlock, first_log_id: u64) -> Result<LogIngestPlan> {
    validate_logs(block)?;
    let (header, block_blob) = encode_log_block(&block.logs)?;
    let stream_appends_by_stream = collect_log_stream_appends(block, first_log_id)?;

    Ok(LogIngestPlan {
        header,
        block_blob,
        stream_appends_by_stream,
    })
}

pub async fn persist_log_stream_fragments<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    block_num: u64,
    grouped_values: &BTreeMap<String, Vec<u32>>,
) -> Result<Vec<(String, u32)>> {
    bitmap_pages::persist_stream_fragments(
        &tables.log_streams,
        block_num,
        iter_grouped_stream_appends(grouped_values),
        STREAM_PAGE_LOCAL_ID_SPAN,
    )
    .await
}

pub async fn persist_log_artifacts<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    block_num: u64,
    plan: &LogIngestPlan,
) -> Result<usize> {
    tables
        .log_block_blobs
        .put_block(block_num, plan.block_blob.clone(), &plan.header)
        .await?;
    Ok(plan.header.log_count())
}

fn validate_logs(block: &FinalizedBlock) -> Result<()> {
    let mut previous_tx_idx = None;

    for (index, log) in block.logs.iter().enumerate() {
        if !validate_log(log) {
            return Err(Error::InvalidParams("log topics exceed 4"));
        }
        if log.block_num != block.block_num {
            return Err(Error::InvalidParams(
                "log block_num must match enclosing block",
            ));
        }
        if log.block_hash != block.block_hash {
            return Err(Error::InvalidParams(
                "log block_hash must match enclosing block",
            ));
        }

        let expected_log_idx =
            u32::try_from(index).map_err(|_| Error::Decode("log_idx overflow"))?;
        if log.log_idx != expected_log_idx {
            return Err(Error::InvalidParams(
                "log_idx must match log position within block",
            ));
        }
        if let Some(previous_tx_idx) = previous_tx_idx
            && log.tx_idx < previous_tx_idx
        {
            return Err(Error::InvalidParams(
                "log tx_idx must be non-decreasing within block",
            ));
        }
        previous_tx_idx = Some(log.tx_idx);
    }

    Ok(())
}

fn encode_log_block(logs: &[Log]) -> Result<(BlockLogHeader, Bytes)> {
    let mut offsets = BucketedOffsets::new();
    let mut out = Vec::<u8>::new();

    for log in logs {
        offsets.push(
            u64::try_from(out.len()).map_err(|_| Error::Decode("block log offset overflow"))?,
        )?;
        out.extend_from_slice(&log.encode());
    }

    offsets
        .push(u64::try_from(out.len()).map_err(|_| Error::Decode("block log size overflow"))?)?;

    Ok((BlockLogHeader { offsets }, Bytes::from(out)))
}

fn collect_log_stream_appends(
    block: &FinalizedBlock,
    first_log_id: u64,
) -> Result<BTreeMap<String, Vec<u32>>> {
    collect_grouped_stream_appends(first_log_id, block.logs.iter(), |log, primary_id| {
        Ok(stream_entries_for_log(log, LogId::new(primary_id)))
    })
}

fn stream_entries_for_log(log: &Log, global_log_id: LogId) -> Vec<(String, u32)> {
    let shard = global_log_id.shard().get();
    let local = global_log_id.local().get();

    let mut entries = Vec::with_capacity(5);
    entries.push((sharded_stream_id("addr", &log.address, shard), local));

    if let Some(topic0) = log.topics.first() {
        entries.push((sharded_stream_id("topic0", topic0, shard), local));
    }

    for (topic_index, topic) in log.topics.iter().enumerate().skip(1).take(3) {
        let kind = match topic_index {
            1 => "topic1",
            2 => "topic2",
            3 => "topic3",
            _ => continue,
        };
        entries.push((sharded_stream_id(kind, topic, shard), local));
    }

    entries
}

#[cfg(test)]
pub fn collect_stream_appends(
    block: &FinalizedBlock,
    first_log_id: u64,
) -> BTreeMap<String, Vec<u32>> {
    plan_log_ingest(block, first_log_id)
        .expect("valid log ingest plan")
        .stream_appends_by_stream
}

#[cfg(test)]
mod tests {
    use crate::core::ids::LogId;
    use crate::core::layout::{DIRECTORY_BUCKET_SIZE, DIRECTORY_SUB_BUCKET_SIZE};
    use crate::error::Error;
    use crate::family::FinalizedBlock;
    use crate::kernel::codec::StorageCodec;
    use crate::kernel::table_specs::{PointTableSpec, ScannableTableSpec};
    use crate::logs::codec::validate_log;
    use crate::logs::table_specs::{
        BlobTableSpec, BlockLogBlobSpec, BlockLogHeaderSpec, LogBitmapByBlockSpec,
        LogBitmapPageBlobSpec, LogBitmapPageMetaSpec, LogDirBucketSpec, LogDirByBlockSpec,
        LogDirSubBucketSpec,
    };
    use crate::store::blob::InMemoryBlobStore;
    use crate::store::meta::InMemoryMetaStore;
    use crate::store::traits::{BlobStore, MetaStore};
    use crate::streams::decode_bitmap_blob;
    use crate::tables::Tables;
    use futures::executor::block_on;

    use super::{
        collect_stream_appends, persist_log_artifacts, persist_log_stream_fragments,
        plan_log_ingest,
    };
    use crate::ingest::bitmap_pages;
    use crate::ingest::primary_dir::compact_sealed_primary_directory;
    use crate::kernel::sharded_streams::page_start_local;
    use crate::logs::STREAM_PAGE_LOCAL_ID_SPAN;
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

    fn sample_block(block_num: u64, seed: u8, mut logs: Vec<Log>) -> FinalizedBlock {
        let block_hash = [seed; 32];
        let parent_hash = [seed.wrapping_add(1); 32];
        for log in &mut logs {
            log.block_num = block_num;
            log.block_hash = block_hash;
        }
        FinalizedBlock {
            block_num,
            block_hash,
            parent_hash,
            header: crate::core::header::EvmBlockHeader::minimal(
                block_num,
                block_hash,
                parent_hash,
            ),
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
            let logs = vec![sample_log(7, 0, 0, 1), sample_log(7, 0, 1, 2)];
            let block = sample_block(7, 9, logs.clone());
            let plan = plan_log_ingest(&block, 11).expect("plan log ingest");

            persist_log_artifacts(&tables, block.block_num, &plan)
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

            assert_eq!(header.offsets.get(0), Some(0));
            assert_eq!(header.offsets.get(1), Some(logs[0].encode().len() as u64));
            assert_eq!(header.offsets.get(2), Some(block_blob.len() as u64));
            let first_start = header.offsets.get(0).expect("first start") as usize;
            let first_end = header.offsets.get(1).expect("first end") as usize;
            assert_eq!(
                Log::decode(&block_blob[first_start..first_end]).expect("decode first"),
                block.logs[0]
            );
            assert!(validate_log(&block.logs[0]));
        });
    }

    #[test]
    fn persist_log_artifacts_rejects_more_than_four_topics() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let blob = InMemoryBlobStore::default();
            let _tables = Tables::without_cache(meta.clone(), blob.clone());
            let mut block = sample_block(7, 9, vec![sample_log(7, 0, 0, 1)]);
            block.logs[0].topics = vec![[1; 32], [2; 32], [3; 32], [4; 32], [5; 32]];

            let err = plan_log_ingest(&block, 11).expect_err("invalid log should fail");

            assert!(matches!(err, Error::InvalidParams("log topics exceed 4")));
            assert!(
                blob.get_blob(BlockLogBlobSpec::TABLE, &BlockLogBlobSpec::key(7))
                    .await
                    .expect("read block blob")
                    .is_none()
            );
            assert!(
                meta.get(BlockLogHeaderSpec::TABLE, &BlockLogHeaderSpec::key(7))
                    .await
                    .expect("read block header")
                    .is_none()
            );
        });
    }

    #[test]
    fn persist_log_artifacts_rejects_block_num_mismatch() {
        block_on(async {
            let _tables =
                Tables::without_cache(InMemoryMetaStore::default(), InMemoryBlobStore::default());
            let mut block = sample_block(7, 9, vec![sample_log(7, 0, 0, 1)]);
            block.logs[0].block_num = 8;

            let err = plan_log_ingest(&block, 11).expect_err("invalid log should fail");

            assert!(matches!(
                err,
                Error::InvalidParams("log block_num must match enclosing block")
            ));
        });
    }

    #[test]
    fn persist_log_artifacts_rejects_non_canonical_log_order() {
        block_on(async {
            let _tables =
                Tables::without_cache(InMemoryMetaStore::default(), InMemoryBlobStore::default());
            let block = sample_block(7, 9, vec![sample_log(7, 1, 0, 1), sample_log(7, 0, 1, 2)]);

            let err = plan_log_ingest(&block, 11).expect_err("invalid log order should fail");

            assert!(matches!(
                err,
                Error::InvalidParams("log tx_idx must be non-decreasing within block")
            ));
        });
    }

    #[test]
    fn persist_log_artifacts_rejects_non_canonical_log_idx() {
        block_on(async {
            let _tables =
                Tables::without_cache(InMemoryMetaStore::default(), InMemoryBlobStore::default());
            let block = sample_block(7, 9, vec![sample_log(7, 0, 0, 1), sample_log(7, 0, 2, 2)]);

            let err = plan_log_ingest(&block, 11).expect_err("invalid log index should fail");

            assert!(matches!(
                err,
                Error::InvalidParams("log_idx must match log position within block")
            ));
        });
    }

    #[test]
    fn persist_log_artifacts_rejects_block_hash_mismatch() {
        block_on(async {
            let _tables =
                Tables::without_cache(InMemoryMetaStore::default(), InMemoryBlobStore::default());
            let mut invalid = sample_log(7, 0, 0, 1);
            invalid.block_hash = [0xaa; 32];
            let block = FinalizedBlock {
                block_num: 7,
                block_hash: [9; 32],
                parent_hash: [10; 32],
                header: crate::core::header::EvmBlockHeader::minimal(7, [9; 32], [10; 32]),
                logs: vec![invalid],
                txs: Vec::new(),
                trace_rlp: Vec::new(),
            };

            let err = plan_log_ingest(&block, 11).expect_err("invalid block hash should fail");

            assert!(matches!(
                err,
                Error::InvalidParams("log block_hash must match enclosing block")
            ));
        });
    }

    #[test]
    fn persist_log_artifacts_persists_empty_block_blob_and_header() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let blob = InMemoryBlobStore::default();
            let tables = Tables::without_cache(meta.clone(), blob.clone());
            let block = sample_block(7, 9, Vec::new());
            let plan = plan_log_ingest(&block, 11).expect("plan log ingest");

            persist_log_artifacts(&tables, block.block_num, &plan)
                .await
                .expect("persist empty artifacts");

            let header = meta
                .get(BlockLogHeaderSpec::TABLE, &BlockLogHeaderSpec::key(7))
                .await
                .expect("read block header")
                .expect("block header present");
            let header = BlockLogHeader::decode(&header.value).expect("decode header");
            let block_blob = blob
                .get_blob(BlockLogBlobSpec::TABLE, &BlockLogBlobSpec::key(7))
                .await
                .expect("read block blob")
                .expect("block blob present");

            assert_eq!(header.log_count(), 0);
            assert_eq!(header.offsets.get(0), Some(0));
            assert!(block_blob.is_empty());
        });
    }

    #[test]
    fn persist_log_dir_fragments_and_compaction_cover_spanning_block() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let tables = Tables::without_cache(meta.clone(), InMemoryBlobStore::default());
            let first_log_id = DIRECTORY_SUB_BUCKET_SIZE - 3;
            let count = 8u32;

            tables
                .log_dir
                .persist_block_fragment(700, first_log_id, count)
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
                    &LogDirByBlockSpec::partition(DIRECTORY_SUB_BUCKET_SIZE),
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
            let plan = plan_log_ingest(&block, first_log_id).expect("plan log ingest");
            let touched_pages = persist_log_stream_fragments(
                &tables,
                block.block_num,
                &plan.stream_appends_by_stream,
            )
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
                    LogBitmapByBlockSpec::TABLE,
                    &LogBitmapByBlockSpec::partition(&sid, first_page),
                    &LogBitmapByBlockSpec::clustering(block.block_num),
                )
                .await
                .expect("read stream fragment")
                .expect("stream fragment");
            let page_meta = meta
                .get(
                    LogBitmapPageMetaSpec::TABLE,
                    &LogBitmapPageMetaSpec::key(&sid, first_page),
                )
                .await
                .expect("read stream page meta")
                .expect("stream page meta");
            let page_blob = blob
                .get_blob(
                    LogBitmapPageBlobSpec::TABLE,
                    &LogBitmapPageBlobSpec::key(&sid, first_page),
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
            let first_log_id = DIRECTORY_BUCKET_SIZE - DIRECTORY_SUB_BUCKET_SIZE - 2;
            let count = (DIRECTORY_SUB_BUCKET_SIZE + 5) as u32;

            tables
                .log_dir
                .persist_block_fragment(700, first_log_id, count)
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
