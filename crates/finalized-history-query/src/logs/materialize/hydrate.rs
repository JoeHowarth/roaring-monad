use crate::core::directory_resolver::{ResolvedPrimaryLocation, resolve_primary_id};
use crate::core::ids::LogId;
use crate::core::refs::BlockRef;
use crate::error::{Error, Result};
use crate::logs::filter::{LogFilter, exact_match};
use crate::logs::log_ref::LogRef;
use crate::query::runner::{QueryMaterializer, cached_parent_block_ref};
use crate::store::traits::{BlobStore, MetaStore};

use super::LogMaterializer;

impl<M: MetaStore, B: BlobStore> QueryMaterializer for LogMaterializer<'_, M, B> {
    type Id = LogId;
    type Item = LogRef;
    type Filter = LogFilter;
    type Output = LogRef;

    async fn resolve_id(&mut self, id: Self::Id) -> Result<Option<ResolvedPrimaryLocation>> {
        Ok(resolve_primary_id::<M, LogId>(
            &self.tables.log_dir,
            &mut self.caches.directory_fragment_cache,
            id,
        )
        .await?
        .map(|location| ResolvedPrimaryLocation {
            block_num: location.block_num,
            local_ordinal: location.local_ordinal,
        }))
    }

    async fn load_by_id(&mut self, id: Self::Id) -> Result<Option<Self::Item>> {
        let Some(location) = self.resolve_id(id).await? else {
            return Ok(None);
        };
        Ok(self
            .tables
            .point_log_payloads
            .load_contiguous_run(
                location.block_num,
                location.local_ordinal,
                location.local_ordinal,
            )
            .await?
            .into_iter()
            .next())
    }

    async fn load_run(
        &mut self,
        run: &[(Self::Id, ResolvedPrimaryLocation)],
    ) -> Result<Vec<(Self::Id, Self::Item)>> {
        let Some((_, first)) = run.first().copied() else {
            return Ok(Vec::new());
        };
        let last = run.last().expect("run must be non-empty").1;
        let run_items = self
            .tables
            .point_log_payloads
            .load_contiguous_run(first.block_num, first.local_ordinal, last.local_ordinal)
            .await?;
        if run_items.len() != run.len() {
            return Err(Error::NotFound);
        }
        Ok(run
            .iter()
            .copied()
            .map(|(id, _)| id)
            .zip(run_items)
            .collect())
    }

    async fn block_ref_for(&mut self, item: &Self::Item) -> Result<BlockRef> {
        cached_parent_block_ref(
            &mut self.caches.block_ref_cache,
            self.tables,
            item.block_num(),
            *item.block_hash(),
        )
        .await
    }

    fn exact_match(&self, item: &Self::Item, filter: &Self::Filter) -> bool {
        exact_match(item, filter)
    }

    fn into_output(item: Self::Item) -> Self::Output {
        item
    }
}
