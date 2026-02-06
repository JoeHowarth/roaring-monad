use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

use roaring::RoaringBitmap;

use crate::codec::chunk::{ChunkBlob, decode_chunk, encode_chunk};
use crate::codec::log::{
    decode_block_meta, decode_meta_state, decode_topic0_mode, decode_topic0_stats,
    encode_block_meta, encode_log, encode_meta_state, encode_topic0_mode, encode_topic0_stats,
    encode_u64,
};
use crate::codec::manifest::{
    ChunkRef, Manifest, decode_manifest, decode_tail, encode_manifest, encode_tail,
};
use crate::config::Config;
use crate::domain::keys::{
    META_STATE_KEY, block_hash_to_num_key, block_meta_key, chunk_blob_key, log_key, manifest_key,
    stream_id, tail_key, topic0_mode_key, topic0_stats_key,
};
use crate::domain::types::{Block, BlockMeta, IngestOutcome, MetaState, Topic0Mode, Topic0Stats};
use crate::error::{Error, Result};
use crate::store::traits::{BlobStore, FenceToken, MetaStore, PutCond, Record};

pub struct IngestEngine<M: MetaStore, B: BlobStore> {
    pub config: Config,
    pub meta_store: M,
    pub blob_store: B,
}

impl<M: MetaStore, B: BlobStore> IngestEngine<M, B> {
    pub fn new(config: Config, meta_store: M, blob_store: B) -> Self {
        Self {
            config,
            meta_store,
            blob_store,
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
        for (i, log) in block.logs.iter().enumerate() {
            let global_log_id = first_log_id + i as u64;
            let key = log_key(global_log_id);
            let _ = self
                .meta_store
                .put(&key, encode_log(log), PutCond::Any, FenceToken(epoch))
                .await?;
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
        for (stream_id, values) in appends {
            let _ = self
                .apply_stream_appends(&stream_id, &values, epoch)
                .await?;
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
        epoch: u64,
    ) -> Result<BTreeMap<String, Vec<u32>>> {
        let mut out: BTreeMap<String, BTreeSet<u32>> = BTreeMap::new();
        let mut topic0_seen: BTreeSet<[u8; 32]> = BTreeSet::new();

        for (i, log) in block.logs.iter().enumerate() {
            let global = first_log_id + i as u64;
            let shard = (global >> 32) as u32;
            let local = global as u32;

            out.entry(stream_id("addr", &log.address, shard))
                .or_default()
                .insert(local);

            if let Some(topic0) = log.topics.first() {
                let bshard = (block.block_num >> 32) as u32;
                let block_local = block.block_num as u32;
                // topic0_block always on and deduped once per block/signature
                if topic0_seen.insert(*topic0) {
                    out.entry(stream_id("topic0_block", topic0, bshard))
                        .or_default()
                        .insert(block_local);
                }

                if self.topic0_log_enabled(topic0, block.block_num).await? {
                    out.entry(stream_id("topic0_log", topic0, shard))
                        .or_default()
                        .insert(local);
                }
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

        // update basic topic0 rolling stats for signatures seen in this block
        self.update_topic0_modes_for_block(block.block_num, &topic0_seen, epoch)
            .await?;

        let mut flattened = BTreeMap::new();
        for (k, set) in out {
            flattened.insert(k, set.into_iter().collect());
        }
        Ok(flattened)
    }

    async fn apply_stream_appends(&self, stream: &str, values: &[u32], epoch: u64) -> Result<bool> {
        let (mut manifest, manifest_version) = self.load_manifest(stream).await?;
        let mut tail = self.load_tail(stream).await?;

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
            sealed = true;
        }

        self.store_manifest(stream, &manifest, manifest_version, epoch)
            .await?;
        self.store_tail(stream, &tail, epoch).await?;
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
        let cond = match current_version {
            Some(v) => PutCond::IfVersion(v),
            None => PutCond::IfAbsent,
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
        if !r.applied {
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
    ) -> Result<()> {
        let key = manifest_key(stream);
        let cond = match version {
            Some(v) => PutCond::IfVersion(v),
            None => PutCond::IfAbsent,
        };
        let r = self
            .meta_store
            .put(&key, encode_manifest(manifest), cond, FenceToken(epoch))
            .await?;
        if !r.applied {
            return Err(Error::CasConflict);
        }
        Ok(())
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

    async fn topic0_log_enabled(&self, topic0: &[u8; 32], block_num: u64) -> Result<bool> {
        let key = topic0_mode_key(topic0);
        let rec = self.meta_store.get(&key).await?;
        let Some(rec) = rec else {
            return Ok(false);
        };
        let mode = decode_topic0_mode(&rec.value)?;
        Ok(mode.log_enabled && block_num >= mode.enabled_from_block)
    }

    async fn update_topic0_modes_for_block(
        &self,
        block_num: u64,
        seen_signatures: &BTreeSet<[u8; 32]>,
        epoch: u64,
    ) -> Result<()> {
        for sig in seen_signatures {
            self.update_one_topic0_stats(*sig, block_num, true, epoch)
                .await?;
        }
        Ok(())
    }

    async fn update_one_topic0_stats(
        &self,
        topic0: [u8; 32],
        block_num: u64,
        seen_in_block: bool,
        epoch: u64,
    ) -> Result<()> {
        let key = topic0_stats_key(&topic0);
        let (mut stats, version): (Topic0Stats, Option<u64>) =
            match self.meta_store.get(&key).await? {
                Some(Record { value, version }) => (decode_topic0_stats(&value)?, Some(version)),
                None => (
                    Topic0Stats {
                        window_len: 50_000,
                        blocks_seen_in_window: 0,
                        ring_cursor: 0,
                        last_updated_block: block_num.saturating_sub(1),
                        ring_bits: vec![0u8; (50_000usize).div_ceil(8)],
                    },
                    None,
                ),
            };

        if block_num <= stats.last_updated_block {
            return Ok(());
        }

        let start = stats.last_updated_block.saturating_add(1);
        for b in start..=block_num {
            let seen = b == block_num && seen_in_block;
            apply_window_step(&mut stats, b, seen);
        }

        let cond = version.map(PutCond::IfVersion).unwrap_or(PutCond::IfAbsent);
        let result = self
            .meta_store
            .put(&key, encode_topic0_stats(&stats), cond, FenceToken(epoch))
            .await?;
        if !result.applied {
            return Ok(());
        }

        let mode_key = topic0_mode_key(&topic0);
        let mut mode = match self.meta_store.get(&mode_key).await? {
            Some(rec) => decode_topic0_mode(&rec.value)?,
            None => Topic0Mode {
                log_enabled: false,
                enabled_from_block: 0,
            },
        };

        let denom = u64::min(stats.window_len as u64, block_num.saturating_add(1)).max(1);
        let ratio = stats.blocks_seen_in_window as f64 / denom as f64;
        let mut changed = false;
        if !mode.log_enabled && ratio < 0.001 {
            mode.log_enabled = true;
            mode.enabled_from_block = block_num;
            changed = true;
        } else if mode.log_enabled && ratio > 0.01 {
            mode.log_enabled = false;
            changed = true;
        }

        if changed || self.meta_store.get(&mode_key).await?.is_none() {
            let _ = self
                .meta_store
                .put(
                    &mode_key,
                    encode_topic0_mode(&mode),
                    PutCond::Any,
                    FenceToken(epoch),
                )
                .await?;
        }
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

fn apply_window_step(stats: &mut Topic0Stats, block_num: u64, seen: bool) {
    let window = stats.window_len.max(1) as usize;
    let index = (block_num as usize) % window;
    let byte_idx = index / 8;
    let mask = 1u8 << (index % 8);
    let currently_set = (stats.ring_bits[byte_idx] & mask) != 0;

    if currently_set {
        stats.blocks_seen_in_window = stats.blocks_seen_in_window.saturating_sub(1);
        stats.ring_bits[byte_idx] &= !mask;
    }
    if seen {
        stats.ring_bits[byte_idx] |= mask;
        stats.blocks_seen_in_window = stats.blocks_seen_in_window.saturating_add(1);
    }
    stats.ring_cursor = ((index + 1) % window) as u32;
    stats.last_updated_block = block_num;
}

fn parse_stream_from_tail_key(key: &[u8]) -> Option<String> {
    let prefix = b"tails/";
    if !key.starts_with(prefix) {
        return None;
    }
    Some(String::from_utf8_lossy(&key[prefix.len()..]).to_string())
}
