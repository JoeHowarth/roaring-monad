use crate::codec::finalized_state::decode_u64;
use crate::error::Result;
use crate::store::traits::{DelCond, FenceToken, MetaStore};

pub struct GcWorker<'a, M: MetaStore> {
    pub meta_store: &'a M,
}

impl<'a, M: MetaStore> GcWorker<'a, M> {
    pub fn new(meta_store: &'a M) -> Self {
        Self { meta_store }
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
