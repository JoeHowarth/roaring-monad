use std::collections::{BTreeMap, BTreeSet, HashMap};

use bytes::Bytes;
use futures::stream::{FuturesUnordered, StreamExt};

use crate::codec::finalized_state::{encode_block_meta, encode_u64};
use crate::codec::log::{decode_log_locator_page, encode_log, encode_log_locator_page};
use crate::config::Config;
use crate::domain::keys::{
    block_hash_to_num_key, block_meta_key, log_local, log_locator_page_key, log_locator_page_start,
    log_pack_blob_key, log_shard, stream_id,
};
use crate::domain::types::{BlockMeta, Log, LogLocator};
use crate::error::{Error, Result};
use crate::logs::types::Block;
use crate::store::traits::{BlobStore, FenceToken, MetaStore, PutCond};

pub async fn persist_log_artifacts<M: MetaStore, B: BlobStore>(
    config: &Config,
    meta_store: &M,
    blob_store: &B,
    logs: &[Log],
    first_log_id: u64,
    epoch: u64,
) -> Result<()> {
    if logs.is_empty() {
        return Ok(());
    }

    let (log_pack, spans) = encode_log_pack(logs)?;
    let pack_key = log_pack_blob_key(first_log_id);
    blob_store.put_blob(&pack_key, log_pack).await?;

    let mut page_updates: HashMap<u64, HashMap<u16, LogLocator>> = HashMap::new();
    for (index, (byte_offset, byte_len)) in spans.iter().enumerate() {
        let global_log_id = first_log_id + index as u64;
        let page_start = log_locator_page_start(global_log_id);
        let slot = u16::try_from(global_log_id - page_start)
            .map_err(|_| Error::Decode("log locator page slot overflow"))?;
        page_updates.entry(page_start).or_default().insert(
            slot,
            LogLocator {
                blob_key: pack_key.clone(),
                byte_offset: *byte_offset,
                byte_len: *byte_len,
            },
        );
    }

    let mut in_flight = FuturesUnordered::new();
    for (page_start, page_entries) in page_updates {
        let page_key = log_locator_page_key(page_start);
        in_flight.push(async move {
            let mut merged_entries = match meta_store.get(&page_key).await? {
                Some(existing) => {
                    let (stored_page_start, stored_entries) =
                        decode_log_locator_page(&existing.value)?;
                    if stored_page_start != page_start {
                        return Err(Error::Decode("log locator page key mismatch"));
                    }
                    stored_entries
                }
                None => HashMap::new(),
            };
            merged_entries.extend(page_entries);
            meta_store
                .put(
                    &page_key,
                    encode_log_locator_page(page_start, &merged_entries),
                    PutCond::Any,
                    FenceToken(epoch),
                )
                .await?;
            Ok::<(), Error>(())
        });

        if in_flight.len() >= config.log_locator_write_concurrency.max(1) {
            match in_flight.next().await {
                Some(Ok(())) => {}
                Some(Err(e)) => return Err(e),
                None => break,
            }
        }
    }
    while let Some(res) = in_flight.next().await {
        res?;
    }

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
        let global_log_id = first_log_id + index as u64;
        let shard = log_shard(global_log_id);
        let local = log_local(global_log_id);

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

fn encode_log_pack(logs: &[Log]) -> Result<(Bytes, Vec<(u32, u32)>)> {
    let mut out = Vec::<u8>::new();
    let mut spans = Vec::with_capacity(logs.len());
    for log in logs {
        let encoded = encode_log(log);
        let offset =
            u32::try_from(out.len()).map_err(|_| Error::Decode("log pack offset overflow"))?;
        let len = u32::try_from(encoded.len())
            .map_err(|_| Error::Decode("log pack entry length overflow"))?;
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(&encoded);
        spans.push((offset + 4, len));
    }
    Ok((Bytes::from(out), spans))
}

#[cfg(test)]
mod tests {
    use crate::codec::finalized_state::{decode_block_meta, decode_u64};
    use crate::codec::log::{decode_log, decode_log_locator_page};
    use crate::config::Config;
    use crate::domain::keys::{
        block_hash_to_num_key, block_meta_key, log_locator_page_key, log_pack_blob_key,
    };
    use crate::domain::types::Log;
    use crate::logs::ingest::{persist_log_artifacts, persist_log_block_metadata};
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
    fn persist_log_artifacts_writes_pack_and_locator_entries() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let blob = InMemoryBlobStore::default();
            let config = Config::default();
            let logs = vec![sample_log(7, 0, 0, 1), sample_log(7, 0, 1, 2)];

            persist_log_artifacts(&config, &meta, &blob, &logs, 11, 5)
                .await
                .expect("persist artifacts");

            let pack = blob
                .get_blob(&log_pack_blob_key(11))
                .await
                .expect("read pack")
                .expect("pack present");
            let page = meta
                .get(&log_locator_page_key(0))
                .await
                .expect("read locator page")
                .expect("locator page present");
            let (page_start, entries) =
                decode_log_locator_page(&page.value).expect("decode locator page");

            assert_eq!(page_start, 0);
            assert_eq!(entries.len(), 2);

            let first = entries.get(&11).expect("slot 11");
            let second = entries.get(&12).expect("slot 12");

            assert_eq!(first.blob_key, log_pack_blob_key(11));
            assert_eq!(second.blob_key, log_pack_blob_key(11));
            assert!(first.byte_offset < second.byte_offset);

            let first_end = (first.byte_offset + first.byte_len) as usize;
            let second_end = (second.byte_offset + second.byte_len) as usize;
            assert_eq!(
                decode_log(&pack[first.byte_offset as usize..first_end]).expect("decode first"),
                logs[0]
            );
            assert_eq!(
                decode_log(&pack[second.byte_offset as usize..second_end]).expect("decode second"),
                logs[1]
            );
        });
    }

    #[test]
    fn persist_log_artifacts_merges_existing_locator_page_entries() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let blob = InMemoryBlobStore::default();
            let config = Config::default();

            persist_log_artifacts(&config, &meta, &blob, &[sample_log(5, 0, 0, 9)], 1, 3)
                .await
                .expect("persist first page");
            persist_log_artifacts(&config, &meta, &blob, &[sample_log(5, 0, 1, 10)], 2, 3)
                .await
                .expect("persist second page");

            let page = meta
                .get(&log_locator_page_key(0))
                .await
                .expect("read locator page")
                .expect("locator page present");
            let (_, entries) = decode_log_locator_page(&page.value).expect("decode locator page");

            assert_eq!(entries.len(), 2);
            assert!(entries.contains_key(&1));
            assert!(entries.contains_key(&2));
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
