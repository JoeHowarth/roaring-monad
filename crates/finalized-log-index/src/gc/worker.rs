use std::collections::BTreeSet;

use crate::codec::log::decode_u64;
use crate::codec::manifest::decode_manifest;
use crate::config::Config;
use crate::domain::keys::{chunk_blob_key, manifest_key};
use crate::error::Result;
use crate::store::traits::{BlobStore, DelCond, FenceToken, MetaStore};

#[derive(Debug, Default, Clone)]
pub struct GcStats {
    pub orphan_chunk_bytes: u64,
    pub orphan_manifest_segments: u64,
    pub stale_tail_keys: u64,
    pub deleted_orphan_chunks: u64,
    pub deleted_stale_tails: u64,
    pub exceeded_guardrail: bool,
}

pub struct GcWorker<'a, M: MetaStore, B: BlobStore> {
    pub meta_store: &'a M,
    pub blob_store: &'a B,
    pub config: &'a Config,
}

impl<'a, M: MetaStore, B: BlobStore> GcWorker<'a, M, B> {
    pub fn new(meta_store: &'a M, blob_store: &'a B, config: &'a Config) -> Self {
        Self {
            meta_store,
            blob_store,
            config,
        }
    }

    pub async fn run_once(&self) -> Result<GcStats> {
        let mut stats = GcStats::default();

        let mut referenced_chunks = BTreeSet::<Vec<u8>>::new();
        let manifest_page = self
            .meta_store
            .list_prefix(b"manifests/", None, usize::MAX)
            .await?;
        let tail_page = self
            .meta_store
            .list_prefix(b"tails/", None, usize::MAX)
            .await?;

        let mut manifest_streams = BTreeSet::<String>::new();
        for mk in &manifest_page.keys {
            let Some(rec) = self.meta_store.get(mk).await? else {
                continue;
            };
            let m = decode_manifest(&rec.value)?;
            let sid = stream_id_from_manifest_key(mk);
            manifest_streams.insert(sid.clone());
            for cref in m.chunk_refs {
                referenced_chunks.insert(chunk_blob_key(&sid, cref.chunk_seq));
            }
        }

        let blob_page = self
            .blob_store
            .list_prefix(b"chunks/", None, usize::MAX)
            .await?;
        for ck in &blob_page.keys {
            if !referenced_chunks.contains(ck) {
                if let Some(blob) = self.blob_store.get_blob(ck).await? {
                    stats.orphan_chunk_bytes =
                        stats.orphan_chunk_bytes.saturating_add(blob.len() as u64);
                }
                self.blob_store.delete_blob(ck).await?;
                stats.deleted_orphan_chunks = stats.deleted_orphan_chunks.saturating_add(1);
            }
        }

        for tk in &tail_page.keys {
            let sid = stream_id_from_tail_key(tk);
            if !manifest_streams.contains(&sid) {
                self.meta_store
                    .delete(tk, DelCond::Any, FenceToken(u64::MAX))
                    .await?;
                stats.deleted_stale_tails = stats.deleted_stale_tails.saturating_add(1);
            }
        }

        stats.stale_tail_keys = stats.deleted_stale_tails;
        stats.orphan_manifest_segments = 0;

        stats.exceeded_guardrail = stats.orphan_chunk_bytes > self.config.max_orphan_chunk_bytes
            || stats.orphan_manifest_segments > self.config.max_orphan_manifest_segments
            || stats.stale_tail_keys > self.config.max_stale_tail_keys;

        Ok(stats)
    }

    pub async fn prune_block_hash_index_below(
        &self,
        min_block_num: u64,
        fence_epoch: u64,
    ) -> Result<u64> {
        let mut removed = 0u64;
        let page = self
            .meta_store
            .list_prefix(b"block_hash_to_num/", None, usize::MAX)
            .await?;
        for key in page.keys {
            let Some(rec) = self.meta_store.get(&key).await? else {
                continue;
            };
            let num = decode_u64(&rec.value)?;
            if num < min_block_num {
                self.meta_store
                    .delete(&key, DelCond::Any, FenceToken(fence_epoch))
                    .await?;
                removed = removed.saturating_add(1);
            }
        }
        Ok(removed)
    }
}

fn stream_id_from_manifest_key(key: &[u8]) -> String {
    let pref = b"manifests/";
    if key.starts_with(pref) {
        String::from_utf8_lossy(&key[pref.len()..]).to_string()
    } else {
        String::new()
    }
}

fn stream_id_from_tail_key(key: &[u8]) -> String {
    let pref = b"tails/";
    if key.starts_with(pref) {
        String::from_utf8_lossy(&key[pref.len()..]).to_string()
    } else {
        String::new()
    }
}

#[allow(dead_code)]
fn _manifest_key_for_stream(stream: &str) -> Vec<u8> {
    manifest_key(stream)
}
