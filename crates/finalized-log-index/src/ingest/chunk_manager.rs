use roaring::RoaringBitmap;

use crate::codec::chunk::{ChunkBlob, encode_chunk};
use crate::codec::manifest::{ChunkRef, Manifest, encode_manifest};
use crate::config::Config;
use crate::domain::keys::{chunk_blob_key, manifest_key};
use crate::error::{Error, Result};
use crate::store::traits::{BlobStore, FenceToken, MetaStore, PutCond};

#[derive(Debug, Clone)]
pub struct ChunkManager {
    pub config: Config,
}

impl ChunkManager {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    pub fn should_seal(&self, tail: &RoaringBitmap) -> bool {
        tail.len() >= self.config.target_entries_per_chunk as u64
    }

    pub async fn seal_if_needed<M: MetaStore, B: BlobStore>(
        &self,
        meta_store: &M,
        blob_store: &B,
        stream_id: &str,
        state: (Manifest, Option<u64>),
        tail: &mut RoaringBitmap,
        epoch: u64,
    ) -> Result<(Manifest, Option<u64>)> {
        let (mut manifest, manifest_version) = state;
        if !self.should_seal(tail) {
            return Ok((manifest, manifest_version));
        }

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
        blob_store
            .put_blob(&chunk_blob_key(stream_id, chunk_seq), encode_chunk(&chunk)?)
            .await?;

        manifest.last_chunk_seq = chunk_seq;
        manifest.approx_count = manifest.approx_count.saturating_add(count as u64);
        manifest.chunk_refs.push(ChunkRef {
            chunk_seq,
            min_local,
            max_local,
            count,
        });

        let cond = manifest_version
            .map(PutCond::IfVersion)
            .unwrap_or(PutCond::IfAbsent);
        let key = manifest_key(stream_id);
        let put = meta_store
            .put(&key, encode_manifest(&manifest), cond, FenceToken(epoch))
            .await?;
        if !put.applied {
            return Err(Error::CasConflict);
        }

        *tail = RoaringBitmap::new();
        Ok((manifest, put.version))
    }
}
