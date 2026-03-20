use crate::codec::log_ref::LogRef;
use crate::core::execution::PrimaryMaterializer;
use crate::core::ids::LogId;
use crate::core::refs::BlockRef;
use crate::error::{Error, Result};
use crate::logs::filter::{LogFilter, exact_match};
use crate::logs::state::load_log_block_record;
use crate::store::traits::{BlobStore, MetaStore};

use super::LogMaterializer;

impl<'a, M: MetaStore, B: BlobStore> LogMaterializer<'a, M, B> {
    pub(crate) async fn load_contiguous_run(
        &mut self,
        block_num: u64,
        start_local_ordinal: usize,
        end_local_ordinal_inclusive: usize,
    ) -> Result<Vec<LogRef>> {
        self.tables
            .point_log_payloads()
            .load_contiguous_run(block_num, start_local_ordinal, end_local_ordinal_inclusive)
            .await
    }
}

impl<M: MetaStore, B: BlobStore> PrimaryMaterializer for LogMaterializer<'_, M, B> {
    type Primary = LogRef;
    type Filter = LogFilter;

    async fn load_by_id(&mut self, id: LogId) -> Result<Option<Self::Primary>> {
        let Some(location) = self.resolve_log_id(id).await? else {
            return Ok(None);
        };
        Ok(self
            .load_contiguous_run(
                location.block_num,
                location.local_ordinal,
                location.local_ordinal,
            )
            .await?
            .into_iter()
            .next())
    }

    async fn block_ref_for(&mut self, item: &Self::Primary) -> Result<BlockRef> {
        let block_num = item.block_num();
        if let Some(block_ref) = self.block_ref_cache.get(&block_num).copied() {
            return Ok(block_ref);
        }

        let block_ref = if let Some(block_ref) = self
            .range_resolver
            .load_block_ref(self.tables.meta_store(), block_num)
            .await?
        {
            block_ref
        } else {
            let Some(block_record) =
                load_log_block_record(self.tables.meta_store(), block_num).await?
            else {
                return Err(Error::NotFound);
            };
            BlockRef {
                number: block_num,
                hash: *item.block_hash(),
                parent_hash: block_record.parent_hash,
            }
        };
        self.block_ref_cache.insert(block_num, block_ref);
        Ok(block_ref)
    }

    fn exact_match(&self, item: &Self::Primary, filter: &Self::Filter) -> bool {
        exact_match(item, filter)
    }
}
