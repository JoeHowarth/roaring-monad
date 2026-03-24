use crate::core::directory_resolver::{ResolvedPrimaryLocation, resolve_primary_id};
use crate::core::ids::LogId;
use crate::core::refs::BlockRef;
use crate::error::{Error, Result};
use crate::logs::filter::{LogFilter, exact_match};
use crate::logs::log_ref::LogRef;
use crate::logs::table_specs::{LogDirBucketSpec, LogDirSubBucketSpec};
use crate::query::runner::{QueryMaterializer, cached_block_ref_with_fallback};
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

impl<M: MetaStore, B: BlobStore> QueryMaterializer for LogMaterializer<'_, M, B> {
    type Id = LogId;
    type Item = LogRef;
    type Filter = LogFilter;
    type Output = crate::logs::types::Log;

    async fn resolve_id(&mut self, id: Self::Id) -> Result<Option<ResolvedPrimaryLocation>> {
        Ok(resolve_primary_id::<M, LogId>(
            self.tables.log_dir(),
            &mut self.directory_fragment_cache,
            id,
            |value| LogDirBucketSpec::bucket_start(value.get()),
            |value| LogDirSubBucketSpec::sub_bucket_start(value.get()),
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
        let tables = self.tables;
        let block_num = item.block_num();
        cached_block_ref_with_fallback(
            &mut self.block_ref_cache,
            tables,
            block_num,
            *item.block_hash(),
            async {
                Ok(tables
                    .block_records()
                    .get(block_num)
                    .await?
                    .map(|record| record.parent_hash))
            },
        )
        .await
    }

    fn exact_match(&self, item: &Self::Item, filter: &Self::Filter) -> bool {
        exact_match(item, filter)
    }

    fn into_output(item: Self::Item) -> Self::Output {
        item.to_owned_log()
    }
}
