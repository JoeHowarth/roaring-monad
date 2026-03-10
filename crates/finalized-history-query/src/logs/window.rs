use crate::core::ids::PrimaryIdRange;
use crate::core::range::ResolvedBlockRange;
use crate::error::Result;
use crate::logs::state::load_log_block_window;
use crate::logs::types::LogBlockWindow;
use crate::store::traits::MetaStore;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogWindowResolver;

impl LogWindowResolver {
    pub async fn resolve<M: MetaStore>(
        &self,
        meta_store: &M,
        block_range: &ResolvedBlockRange,
    ) -> Result<Option<PrimaryIdRange>> {
        if block_range.is_empty() {
            return Ok(None);
        }

        let Some(from_block_window) = self
            .load_block_window(meta_store, block_range.from_block)
            .await?
        else {
            return Ok(None);
        };
        let Some(to_block_window) = self
            .load_block_window(meta_store, block_range.to_block)
            .await?
        else {
            return Ok(None);
        };

        let start = from_block_window.first_log_id;
        let end_exclusive = to_block_window
            .first_log_id
            .saturating_add(to_block_window.count as u64);
        if start >= end_exclusive {
            return Ok(None);
        }

        Ok(PrimaryIdRange::new(start, end_exclusive - 1))
    }

    async fn load_block_window<M: MetaStore>(
        &self,
        meta_store: &M,
        block_num: u64,
    ) -> Result<Option<LogBlockWindow>> {
        load_log_block_window(meta_store, block_num).await
    }
}
