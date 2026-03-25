use alloy_rlp::Header;

use crate::core::directory_resolver::{ResolvedPrimaryLocation, resolve_primary_id};
use crate::core::ids::TraceId;
use crate::core::refs::BlockRef;
use crate::error::{Error, Result};
use crate::query::runner::{MaterializerCaches, QueryMaterializer, cached_parent_block_ref};
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;
use crate::traces::filter::{TraceFilter, exact_match};
use crate::traces::view::TraceRef;

pub struct TraceMaterializer<'a, M: MetaStore, B: BlobStore> {
    tables: &'a Tables<M, B>,
    caches: MaterializerCaches<crate::traces::types::DirByBlock>,
}

impl<'a, M: MetaStore, B: BlobStore> TraceMaterializer<'a, M, B> {
    pub fn new(tables: &'a Tables<M, B>) -> Self {
        Self {
            tables,
            caches: MaterializerCaches::default(),
        }
    }
}

impl<M: MetaStore, B: BlobStore> QueryMaterializer for TraceMaterializer<'_, M, B> {
    type Id = TraceId;
    type Item = TraceRef;
    type Filter = TraceFilter;
    type Output = TraceRef;

    async fn resolve_id(&mut self, id: Self::Id) -> Result<Option<ResolvedPrimaryLocation>> {
        Ok(resolve_primary_id::<M, TraceId>(
            &self.tables.trace_dir,
            &mut self.caches.directory_fragment_cache,
            id,
        )
        .await?
        .map(|location| ResolvedPrimaryLocation {
            block_num: location.block_num,
            local_ordinal: location.local_ordinal,
        }))
    }

    async fn load_run(
        &mut self,
        run: &[(Self::Id, ResolvedPrimaryLocation)],
    ) -> Result<Vec<(Self::Id, Self::Item)>> {
        let mut items = Vec::with_capacity(run.len());
        for (id, location) in run.iter().copied() {
            let Some(item) = self
                .tables
                .trace_payloads
                .load_trace_at(location.block_num, location.local_ordinal)
                .await?
            else {
                continue;
            };
            items.push((id, item));
        }
        Ok(items)
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

/// Compute the total encoded length of the RLP element starting at `buf`.
pub(crate) fn rlp_element_len(buf: &[u8]) -> Result<usize> {
    let mut remaining = buf;
    let original_len = remaining.len();
    let header =
        Header::decode(&mut remaining).map_err(|_| Error::Decode("invalid trace frame header"))?;
    let header_len = original_len - remaining.len();
    Ok(header_len + header.payload_length)
}
