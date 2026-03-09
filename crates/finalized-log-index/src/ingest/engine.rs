use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::{SystemTime, UNIX_EPOCH};

use futures::stream::{FuturesUnordered, StreamExt};
use roaring::RoaringBitmap;

use crate::codec::chunk::{ChunkBlob, decode_chunk, encode_chunk};
use crate::codec::log::{
    decode_block_meta, decode_log_locator_page, decode_meta_state, encode_block_meta, encode_log,
    encode_log_locator_page, encode_meta_state, encode_u64,
};
use crate::codec::manifest::{
    ChunkRef, Manifest, decode_manifest, decode_tail, encode_manifest, encode_tail,
};
use crate::config::{Config, IngestMode};
use crate::domain::keys::{
    META_STATE_KEY, block_hash_to_num_key, block_meta_key, chunk_blob_key, log_local,
    log_locator_page_key, log_locator_page_start, log_pack_blob_key, log_shard, manifest_key,
    stream_id, tail_key,
};
use crate::domain::types::{Block, BlockMeta, IngestOutcome, LogLocator, MetaState};
use crate::error::{Error, Result};
use crate::store::traits::{BlobStore, FenceToken, MetaStore, PutCond};

pub struct IngestEngine<M: MetaStore, B: BlobStore> {
    pub config: Config,
    pub meta_store: M,
    pub blob_store: B,
    stream_state_cache: std::sync::RwLock<HashMap<String, CachedStreamState>>,
}

#[derive(Debug, Clone)]
struct CachedStreamState {
    manifest: Manifest,
    manifest_version: Option<u64>,
    tail: RoaringBitmap,
}

impl<M: MetaStore, B: BlobStore> IngestEngine<M, B> {
    pub fn new(config: Config, meta_store: M, blob_store: B) -> Self {
        Self {
            config,
            meta_store,
            blob_store,
            stream_state_cache: std::sync::RwLock::new(HashMap::new()),
        }
    }

    pub async fn ingest_finalized_block(&self, block: &Block, epoch: u64) -> Result<IngestOutcome> {
        let (state, state_version) = self.load_state().await?;
        let expected = state.indexed_finalized_head + 1;
        if block.block_num != expected {
            return Err(Error::InvalidSequence {
                expected,
                got: block.block_num,
            });
        }

        if block.block_num > 0 {
            let expected_parent = if state.indexed_finalized_head == 0 {
                [0u8; 32]
            } else {
                self.load_block_meta(state.indexed_finalized_head)
                    .await?
                    .block_hash
            };
            if block.parent_hash != expected_parent {
                return Err(Error::InvalidParent);
            }
        }

        let first_log_id = state.next_log_id;
        if !block.logs.is_empty() {
            let (log_pack, spans) = encode_log_pack(&block.logs);
            let pack_key = log_pack_blob_key(first_log_id);
            self.blob_store.put_blob(&pack_key, log_pack).await?;

            let mut page_updates: HashMap<u64, HashMap<u16, LogLocator>> = HashMap::new();
            for (i, (byte_offset, byte_len)) in spans.iter().enumerate() {
                let global_log_id = first_log_id + i as u64;
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
                    let mut merged_entries = match self.meta_store.get(&page_key).await? {
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
                    self.meta_store
                        .put(
                            &page_key,
                            encode_log_locator_page(page_start, &merged_entries),
                            PutCond::Any,
                            FenceToken(epoch),
                        )
                        .await
                });

                if in_flight.len() >= self.config.log_locator_write_concurrency.max(1) {
                    match in_flight.next().await {
                        Some(Ok(_)) => {}
                        Some(Err(e)) => return Err(e),
                        None => break,
                    }
                }
            }
            while let Some(res) = in_flight.next().await {
                let _ = res?;
            }
        }

        let block_meta = BlockMeta {
            block_hash: block.block_hash,
            parent_hash: block.parent_hash,
            first_log_id,
            count: block.logs.len() as u32,
        };
        let _ = self
            .meta_store
            .put(
                &block_meta_key(block.block_num),
                encode_block_meta(&block_meta),
                PutCond::Any,
                FenceToken(epoch),
            )
            .await?;

        let _ = self
            .meta_store
            .put(
                &block_hash_to_num_key(&block.block_hash),
                encode_u64(block.block_num),
                PutCond::Any,
                FenceToken(epoch),
            )
            .await?;

        let appends = self
            .collect_stream_appends(block, first_log_id, epoch)
            .await?;
        let mut appends_in_flight = FuturesUnordered::new();
        for (stream_id, values) in appends {
            appends_in_flight
                .push(async move { self.apply_stream_appends(&stream_id, &values, epoch).await });
            if appends_in_flight.len() >= self.config.stream_append_concurrency.max(1) {
                match appends_in_flight.next().await {
                    Some(Ok(_)) => {}
                    Some(Err(e)) => return Err(e),
                    None => break,
                }
            }
        }
        while let Some(res) = appends_in_flight.next().await {
            let _ = res?;
        }

        let next = MetaState {
            indexed_finalized_head: block.block_num,
            next_log_id: first_log_id + block.logs.len() as u64,
            writer_epoch: epoch,
        };

        self.store_state(&next, state_version, epoch).await?;

        Ok(IngestOutcome {
            indexed_finalized_head: block.block_num,
            written_logs: block.logs.len(),
        })
    }

    pub async fn run_periodic_maintenance(&self, epoch: u64) -> Result<MaintenanceStats> {
        if let Ok(mut cache) = self.stream_state_cache.write() {
            cache.clear();
        }
        let mut stats = MaintenanceStats::default();
        let page = self
            .meta_store
            .list_prefix(b"tails/", None, usize::MAX)
            .await?;
        for key in page.keys {
            if let Some(stream) = parse_stream_from_tail_key(&key) {
                let sealed = self.apply_stream_appends(&stream, &[], epoch).await?;
                if sealed {
                    stats.sealed_streams = stats.sealed_streams.saturating_add(1);
                }
                stats.flushed_streams = stats.flushed_streams.saturating_add(1);
            }
        }
        Ok(stats)
    }

    async fn collect_stream_appends(
        &self,
        block: &Block,
        first_log_id: u64,
        _epoch: u64,
    ) -> Result<BTreeMap<String, Vec<u32>>> {
        let mut out: BTreeMap<String, BTreeSet<u32>> = BTreeMap::new();

        for (i, log) in block.logs.iter().enumerate() {
            let global = first_log_id + i as u64;
            let shard = log_shard(global);
            let local = log_local(global);

            out.entry(stream_id("addr", &log.address, shard))
                .or_default()
                .insert(local);

            if let Some(topic0) = log.topics.first() {
                out.entry(stream_id("topic0", topic0, shard))
                    .or_default()
                    .insert(local);
            }

            for (idx, topic) in log.topics.iter().enumerate().skip(1).take(3) {
                let kind = match idx {
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

        let mut flattened = BTreeMap::new();
        for (k, set) in out {
            flattened.insert(k, set.into_iter().collect());
        }
        Ok(flattened)
    }

    async fn apply_stream_appends(&self, stream: &str, values: &[u32], epoch: u64) -> Result<bool> {
        let cached = self
            .stream_state_cache
            .write()
            .ok()
            .and_then(|mut cache| cache.remove(stream));
        let (mut manifest, mut manifest_version, mut tail) = if let Some(cached) = cached {
            (cached.manifest, cached.manifest_version, cached.tail)
        } else if self.config.assume_empty_streams
            && matches!(self.config.ingest_mode, IngestMode::SingleWriterFast)
        {
            (Manifest::default(), None, RoaringBitmap::new())
        } else {
            let (manifest, manifest_version) = self.load_manifest(stream).await?;
            let tail = self.load_tail(stream).await?;
            (manifest, manifest_version, tail)
        };
        let mut manifest_changed = false;

        for v in values {
            tail.insert(*v);
        }

        let now = now_unix_sec();
        let mut sealed = false;
        if self.should_seal(&tail, manifest.last_seal_unix_sec, now)? {
            let min_local = tail.min().unwrap_or(0);
            let max_local = tail.max().unwrap_or(0);
            let count = tail.len() as u32;
            let chunk_seq = manifest.last_chunk_seq + 1;

            let chunk = ChunkBlob {
                min_local,
                max_local,
                count,
                crc32: 0,
                bitmap: tail.clone(),
            };
            let encoded_chunk = encode_chunk(&chunk)?;
            self.blob_store
                .put_blob(&chunk_blob_key(stream, chunk_seq), encoded_chunk)
                .await?;

            manifest.last_chunk_seq = chunk_seq;
            manifest.approx_count += count as u64;
            manifest.last_seal_unix_sec = now;
            manifest.chunk_refs.push(ChunkRef {
                chunk_seq,
                min_local,
                max_local,
                count,
            });

            tail = RoaringBitmap::new();
            manifest_changed = true;
            sealed = true;
        }

        if manifest_changed {
            manifest_version = self
                .store_manifest(stream, &manifest, manifest_version, epoch)
                .await?;
        }
        self.store_tail(stream, &tail, epoch).await?;
        if let Ok(mut cache) = self.stream_state_cache.write() {
            cache.insert(
                stream.to_string(),
                CachedStreamState {
                    manifest,
                    manifest_version,
                    tail,
                },
            );
        }
        Ok(sealed)
    }

    fn should_seal(
        &self,
        tail: &RoaringBitmap,
        last_seal_unix_sec: u64,
        now_unix_sec: u64,
    ) -> Result<bool> {
        if tail.is_empty() {
            return Ok(false);
        }
        if tail.len() >= self.config.target_entries_per_chunk as u64 {
            return Ok(true);
        }
        if encode_tail(tail)?.len() >= self.config.target_chunk_bytes {
            return Ok(true);
        }
        if last_seal_unix_sec > 0
            && now_unix_sec.saturating_sub(last_seal_unix_sec)
                >= self.config.maintenance_seal_seconds
        {
            return Ok(true);
        }
        Ok(false)
    }

    async fn load_state(&self) -> Result<(MetaState, Option<u64>)> {
        match self.meta_store.get(META_STATE_KEY).await? {
            Some(rec) => Ok((decode_meta_state(&rec.value)?, Some(rec.version))),
            None => Ok((MetaState::default(), None)),
        }
    }

    async fn store_state(
        &self,
        next: &MetaState,
        current_version: Option<u64>,
        epoch: u64,
    ) -> Result<()> {
        let (cond, strict_applied_check) = match self.config.ingest_mode {
            IngestMode::StrictCas => (
                match current_version {
                    Some(v) => PutCond::IfVersion(v),
                    None => PutCond::IfAbsent,
                },
                true,
            ),
            IngestMode::SingleWriterFast => (PutCond::Any, false),
        };
        let r = self
            .meta_store
            .put(
                META_STATE_KEY,
                encode_meta_state(next),
                cond,
                FenceToken(epoch),
            )
            .await?;
        if strict_applied_check && !r.applied {
            return Err(Error::CasConflict);
        }
        Ok(())
    }

    async fn load_block_meta(&self, block_num: u64) -> Result<BlockMeta> {
        let rec = self
            .meta_store
            .get(&block_meta_key(block_num))
            .await?
            .ok_or(Error::NotFound)?;
        decode_block_meta(&rec.value)
    }

    async fn load_manifest(&self, stream: &str) -> Result<(Manifest, Option<u64>)> {
        let key = manifest_key(stream);
        match self.meta_store.get(&key).await? {
            Some(rec) => Ok((decode_manifest(&rec.value)?, Some(rec.version))),
            None => Ok((Manifest::default(), None)),
        }
    }

    async fn store_manifest(
        &self,
        stream: &str,
        manifest: &Manifest,
        version: Option<u64>,
        epoch: u64,
    ) -> Result<Option<u64>> {
        let key = manifest_key(stream);
        let (cond, strict_applied_check) = match self.config.ingest_mode {
            IngestMode::StrictCas => (
                match version {
                    Some(v) => PutCond::IfVersion(v),
                    None => PutCond::IfAbsent,
                },
                true,
            ),
            IngestMode::SingleWriterFast => (PutCond::Any, false),
        };
        let r = self
            .meta_store
            .put(&key, encode_manifest(manifest), cond, FenceToken(epoch))
            .await?;
        if strict_applied_check && !r.applied {
            return Err(Error::CasConflict);
        }
        Ok(r.version.or(version))
    }

    async fn load_tail(&self, stream: &str) -> Result<RoaringBitmap> {
        let key = tail_key(stream);
        match self.meta_store.get(&key).await? {
            Some(rec) => decode_tail(&rec.value),
            None => Ok(RoaringBitmap::new()),
        }
    }

    async fn store_tail(&self, stream: &str, tail: &RoaringBitmap, epoch: u64) -> Result<()> {
        let key = tail_key(stream);
        let _ = self
            .meta_store
            .put(&key, encode_tail(tail)?, PutCond::Any, FenceToken(epoch))
            .await?;
        Ok(())
    }
}

#[allow(dead_code)]
async fn load_chunk_if_present<B: BlobStore>(
    blob_store: &B,
    stream: &str,
    seq: u64,
) -> Result<Option<ChunkBlob>> {
    let bytes = blob_store.get_blob(&chunk_blob_key(stream, seq)).await?;
    match bytes {
        Some(v) => Ok(Some(decode_chunk(&v)?)),
        None => Ok(None),
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct MaintenanceStats {
    pub flushed_streams: u64,
    pub sealed_streams: u64,
}

fn now_unix_sec() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn parse_stream_from_tail_key(key: &[u8]) -> Option<String> {
    let prefix = b"tails/";
    if !key.starts_with(prefix) {
        return None;
    }
    Some(String::from_utf8_lossy(&key[prefix.len()..]).to_string())
}

fn encode_log_pack(logs: &[crate::domain::types::Log]) -> (bytes::Bytes, Vec<(u32, u32)>) {
    let mut out = Vec::<u8>::new();
    let mut spans = Vec::with_capacity(logs.len());
    for log in logs {
        let encoded = encode_log(log);
        let len = encoded.len() as u32;
        let offset = out.len() as u32;
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(&encoded);
        spans.push((offset + 4, len));
    }
    (bytes::Bytes::from(out), spans)
}
