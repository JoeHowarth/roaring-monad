use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;

use crate::codec::finalized_state::{encode_block_meta, encode_u64};
use crate::codec::log::{
    decode_log_directory_bucket, encode_block_log_header, encode_log, encode_log_directory_bucket,
};
use crate::config::Config;
use crate::core::ids::LogId;
use crate::domain::keys::{
    LOG_DIRECTORY_BUCKET_SIZE, block_hash_to_num_key, block_log_header_key, block_logs_blob_key,
    block_meta_key, log_directory_bucket_key, log_directory_bucket_start, log_local, log_shard,
    stream_id,
};
use crate::domain::types::{BlockLogHeader, BlockMeta, Log, LogDirectoryBucket};
use crate::error::{Error, Result};
use crate::logs::types::Block;
use crate::store::traits::{BlobStore, FenceToken, MetaStore, PutCond};

pub async fn persist_log_artifacts<M: MetaStore, B: BlobStore>(
    _config: &Config,
    meta_store: &M,
    blob_store: &B,
    block_num: u64,
    logs: &[Log],
    first_log_id: u64,
    epoch: u64,
) -> Result<()> {
    if logs.is_empty() {
        persist_log_directory_bucket(meta_store, block_num, first_log_id, 0, epoch).await?;
        return Ok(());
    }

    let (block_blob, header) = encode_block_logs(logs)?;
    blob_store
        .put_blob(&block_logs_blob_key(block_num), block_blob)
        .await?;
    meta_store
        .put(
            &block_log_header_key(block_num),
            encode_block_log_header(&header),
            PutCond::Any,
            FenceToken(epoch),
        )
        .await?;
    persist_log_directory_bucket(
        meta_store,
        block_num,
        first_log_id,
        logs.len() as u32,
        epoch,
    )
    .await
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

    meta_store
        .put(
            &block_meta_key(block.block_num),
            encode_block_meta(&block_meta),
            PutCond::Any,
            FenceToken(epoch),
        )
        .await?;

    meta_store
        .put(
            &block_hash_to_num_key(&block.block_hash),
            encode_u64(block.block_num),
            PutCond::Any,
            FenceToken(epoch),
        )
        .await?;

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

async fn persist_log_directory_bucket<M: MetaStore>(
    meta_store: &M,
    block_num: u64,
    first_log_id: u64,
    count: u32,
    epoch: u64,
) -> Result<()> {
    let sentinel = first_log_id.saturating_add(u64::from(count));
    let mut bucket_start = log_directory_bucket_start(LogId::new(first_log_id));
    let last_bucket_start =
        log_directory_bucket_start(LogId::new(sentinel.saturating_sub(1).max(first_log_id)));
    loop {
        upsert_log_directory_bucket(
            meta_store,
            bucket_start,
            block_num,
            first_log_id,
            sentinel,
            epoch,
        )
        .await?;
        if bucket_start == last_bucket_start {
            break;
        }
        bucket_start = bucket_start.saturating_add(LOG_DIRECTORY_BUCKET_SIZE);
    }

    Ok(())
}

async fn upsert_log_directory_bucket<M: MetaStore>(
    meta_store: &M,
    bucket_start: u64,
    block_num: u64,
    first_log_id: u64,
    sentinel: u64,
    epoch: u64,
) -> Result<()> {
    let bucket_key = log_directory_bucket_key(bucket_start);
    let mut bucket = match meta_store.get(&bucket_key).await? {
        Some(existing) => decode_log_directory_bucket(&existing.value)?,
        None => LogDirectoryBucket {
            start_block: block_num,
            first_log_ids: Vec::new(),
        },
    };

    if block_num < bucket.start_block {
        return Err(Error::InvalidSequence {
            expected: bucket.start_block,
            got: block_num,
        });
    }

    let entry_index = usize::try_from(block_num - bucket.start_block)
        .map_err(|_| Error::Decode("directory bucket block index overflow"))?;
    if bucket.first_log_ids.is_empty() {
        bucket.first_log_ids.push(first_log_id);
        bucket.first_log_ids.push(sentinel);
    } else if bucket.first_log_ids.len() < entry_index + 1 {
        return Err(Error::Decode("directory bucket gap before block"));
    } else if bucket.first_log_ids.len() == entry_index + 1 {
        let last = bucket
            .first_log_ids
            .last_mut()
            .ok_or(Error::Decode("log directory bucket missing sentinel"))?;
        *last = first_log_id;
        bucket.first_log_ids.push(sentinel);
    } else {
        bucket.first_log_ids[entry_index] = first_log_id;
        bucket.first_log_ids[entry_index + 1] = sentinel;
        bucket.first_log_ids.truncate(entry_index + 2);
    }

    meta_store
        .put(
            &bucket_key,
            encode_log_directory_bucket(&bucket),
            PutCond::Any,
            FenceToken(epoch),
        )
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::codec::finalized_state::{decode_block_meta, decode_u64};
    use crate::codec::log::{
        decode_block_log_header, decode_log, decode_log_directory_bucket, encode_log,
    };
    use crate::config::Config;
    use crate::domain::keys::{
        LOG_DIRECTORY_BUCKET_SIZE, block_hash_to_num_key, block_log_header_key,
        block_logs_blob_key, block_meta_key, log_directory_bucket_key,
    };
    use crate::domain::types::{Log, LogDirectoryBucket};
    use crate::logs::ingest::{
        persist_log_artifacts, persist_log_block_metadata, persist_log_directory_bucket,
    };
    use crate::logs::types::Block;
    use crate::store::blob::InMemoryBlobStore;
    use crate::store::meta::InMemoryMetaStore;
    use crate::store::traits::{BlobStore, MetaStore};
    use futures::executor::block_on;

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
            let bucket = meta
                .get(&log_directory_bucket_key(0))
                .await
                .expect("read directory bucket")
                .expect("directory bucket present");
            let header = decode_block_log_header(&header.value).expect("decode header");
            let bucket = decode_log_directory_bucket(&bucket.value).expect("decode bucket");

            assert_eq!(
                header.offsets,
                vec![
                    0,
                    encode_log(&logs[0]).len() as u32,
                    block_blob.len() as u32
                ]
            );
            assert_eq!(bucket.first_log_ids, vec![11, 13]);
            assert_eq!(
                decode_log(&block_blob[header.offsets[0] as usize..header.offsets[1] as usize])
                    .expect("decode first"),
                logs[0]
            );
            assert_eq!(
                decode_log(&block_blob[header.offsets[1] as usize..header.offsets[2] as usize])
                    .expect("decode second"),
                logs[1]
            );
        });
    }

    #[test]
    fn persist_log_artifacts_appends_directory_bucket_entries() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let blob = InMemoryBlobStore::default();
            let config = Config::default();

            persist_log_artifacts(&config, &meta, &blob, 5, &[sample_log(5, 0, 0, 9)], 1, 3)
                .await
                .expect("persist first block");
            persist_log_artifacts(&config, &meta, &blob, 6, &[sample_log(6, 0, 1, 10)], 2, 3)
                .await
                .expect("persist second block");

            let bucket = meta
                .get(&log_directory_bucket_key(0))
                .await
                .expect("read directory bucket")
                .expect("directory bucket present");
            let bucket =
                decode_log_directory_bucket(&bucket.value).expect("decode directory bucket");

            assert_eq!(bucket.first_log_ids, vec![1, 2, 3]);
        });
    }

    #[test]
    fn persist_log_artifacts_writes_spanning_block_into_each_covered_bucket() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let first_log_id = LOG_DIRECTORY_BUCKET_SIZE - 3;
            let count = LOG_DIRECTORY_BUCKET_SIZE + 10;

            persist_log_directory_bucket(&meta, 700, first_log_id, count as u32, 3)
                .await
                .expect("persist spanning directory buckets");

            let bucket0 = meta
                .get(&log_directory_bucket_key(0))
                .await
                .expect("read bucket0")
                .expect("bucket0 present");
            let bucket1 = meta
                .get(&log_directory_bucket_key(LOG_DIRECTORY_BUCKET_SIZE))
                .await
                .expect("read bucket1")
                .expect("bucket1 present");
            let bucket2 = meta
                .get(&log_directory_bucket_key(LOG_DIRECTORY_BUCKET_SIZE * 2))
                .await
                .expect("read bucket2")
                .expect("bucket2 present");
            let bucket0 = decode_log_directory_bucket(&bucket0.value).expect("decode bucket0");
            let bucket1 = decode_log_directory_bucket(&bucket1.value).expect("decode bucket1");
            let bucket2 = decode_log_directory_bucket(&bucket2.value).expect("decode bucket2");

            assert_eq!(
                bucket0,
                LogDirectoryBucket {
                    start_block: 700,
                    first_log_ids: vec![first_log_id, first_log_id + count],
                }
            );
            assert_eq!(bucket1, bucket0);
            assert_eq!(bucket2, bucket0);
        });
    }

    #[test]
    fn persist_log_block_metadata_writes_block_meta_and_hash_lookup() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let block = sample_block(9, 7, vec![sample_log(9, 0, 0, 21), sample_log(9, 0, 1, 22)]);

            persist_log_block_metadata(&meta, &block, 33, 4)
                .await
                .expect("persist block metadata");

            let stored_meta = meta
                .get(&block_meta_key(9))
                .await
                .expect("read block meta")
                .expect("block meta present");
            let stored_hash_lookup = meta
                .get(&block_hash_to_num_key(&block.block_hash))
                .await
                .expect("read block hash lookup")
                .expect("block hash lookup present");

            let decoded_meta = decode_block_meta(&stored_meta.value).expect("decode block meta");
            let decoded_block_num =
                decode_u64(&stored_hash_lookup.value).expect("decode block hash lookup");

            assert_eq!(decoded_meta.block_hash, block.block_hash);
            assert_eq!(decoded_meta.parent_hash, block.parent_hash);
            assert_eq!(decoded_meta.first_log_id, 33);
            assert_eq!(decoded_meta.count, 2);
            assert_eq!(decoded_block_num, 9);
        });
    }
}
