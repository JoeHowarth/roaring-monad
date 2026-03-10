use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use roaring::RoaringBitmap;

use crate::config::{Config, IngestMode};
use crate::domain::keys::{chunk_blob_key, manifest_key};
use crate::error::{Error, Result};
use crate::store::traits::{BlobStore, FenceToken, MetaStore, PutCond};
use crate::streams::chunk::{ChunkBlob, encode_chunk};
use crate::streams::manifest::{ChunkRef, Manifest, decode_manifest, encode_manifest};
use crate::streams::tail_manager::TailManager;

#[derive(Debug, Clone)]
pub struct CachedStreamState {
    pub manifest: Manifest,
    pub manifest_version: Option<u64>,
    pub tail: RoaringBitmap,
}

pub struct StreamWriter<'a, M: MetaStore, B: BlobStore> {
    config: &'a Config,
    meta_store: &'a M,
    blob_store: &'a B,
    cache: &'a RwLock<HashMap<String, CachedStreamState>>,
    tail_manager: TailManager,
}

impl<'a, M: MetaStore, B: BlobStore> StreamWriter<'a, M, B> {
    pub fn new(
        config: &'a Config,
        meta_store: &'a M,
        blob_store: &'a B,
        cache: &'a RwLock<HashMap<String, CachedStreamState>>,
    ) -> Self {
        Self {
            config,
            meta_store,
            blob_store,
            cache,
            tail_manager: TailManager,
        }
    }

    pub fn clear_cache(&self) {
        if let Ok(mut cache) = self.cache.write() {
            cache.clear();
        }
    }

    pub async fn apply_appends(&self, stream: &str, values: &[u32], epoch: u64) -> Result<bool> {
        let cached = self
            .cache
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
            let tail = self.tail_manager.load(self.meta_store, stream).await?;
            (manifest, manifest_version, tail)
        };
        let mut manifest_changed = false;

        self.tail_manager.append_all(&mut tail, values);

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
        self.tail_manager
            .store(self.meta_store, stream, &tail, epoch)
            .await?;
        if let Ok(mut cache) = self.cache.write() {
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
        if crate::streams::manifest::encode_tail(tail)?.len() >= self.config.target_chunk_bytes {
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
}

impl<'a, M: MetaStore, B: BlobStore> Clone for StreamWriter<'a, M, B> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, M: MetaStore, B: BlobStore> Copy for StreamWriter<'a, M, B> {}

fn now_unix_sec() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
