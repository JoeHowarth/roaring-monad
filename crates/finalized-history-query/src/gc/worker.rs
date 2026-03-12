use crate::codec::finalized_state::decode_u64;
use crate::config::Config;
use crate::error::Result;
use crate::store::traits::{DelCond, FenceToken, MetaStore};

#[derive(Debug, Default, Clone)]
pub struct GcStats {
    pub orphan_chunk_bytes: u64,
    pub orphan_manifest_segments: u64,
    pub stale_tail_keys: u64,
    pub deleted_orphan_chunks: u64,
    pub deleted_stale_tails: u64,
    pub exceeded_guardrail: bool,
}

pub struct GcWorker<'a, M: MetaStore> {
    pub meta_store: &'a M,
    pub config: &'a Config,
}

impl<'a, M: MetaStore> GcWorker<'a, M> {
    pub fn new(meta_store: &'a M, config: &'a Config) -> Self {
        Self { meta_store, config }
    }

    pub async fn run_once_with_fence(&self, _fence: FenceToken) -> Result<GcStats> {
        let _ = self.config;
        Ok(GcStats::default())
    }

    pub async fn prune_block_hash_index_below(
        &self,
        min_block_num: u64,
        fence: FenceToken,
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
                self.meta_store.delete(&key, DelCond::Any, fence).await?;
                removed = removed.saturating_add(1);
            }
        }
        Ok(removed)
    }
}
