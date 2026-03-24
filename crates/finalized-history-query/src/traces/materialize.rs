use std::collections::HashMap;

use alloy_rlp::Header;

use crate::core::directory_resolver::{ResolvedPrimaryLocation, resolve_primary_id};
use crate::core::ids::TraceId;
use crate::core::refs::BlockRef;
use crate::error::{Error, Result};
use crate::query::runner::{QueryMaterializer, cached_block_ref_with_fallback};
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;
use crate::traces::filter::TraceFilter;
use crate::traces::table_specs::{TraceDirBucketSpec, TraceDirSubBucketSpec};
use crate::traces::types::Trace;

pub struct TraceMaterializer<'a, M: MetaStore, B: BlobStore> {
    tables: &'a Tables<M, B>,
    directory_fragment_cache: HashMap<u64, Vec<crate::traces::types::DirByBlock>>,
    block_ref_cache: HashMap<u64, BlockRef>,
}

impl<'a, M: MetaStore, B: BlobStore> TraceMaterializer<'a, M, B> {
    pub fn new(tables: &'a Tables<M, B>) -> Self {
        Self {
            tables,
            directory_fragment_cache: HashMap::new(),
            block_ref_cache: HashMap::new(),
        }
    }

    pub(crate) async fn load_trace_at(
        &mut self,
        block_num: u64,
        local_ordinal: usize,
    ) -> Result<Option<Trace>> {
        self.tables
            .trace_payloads()
            .load_trace_at(block_num, local_ordinal)
            .await
    }
}

impl<M: MetaStore, B: BlobStore> QueryMaterializer for TraceMaterializer<'_, M, B> {
    type Id = TraceId;
    type Item = Trace;
    type Filter = TraceFilter;
    type Output = Trace;

    async fn resolve_id(&mut self, id: Self::Id) -> Result<Option<ResolvedPrimaryLocation>> {
        Ok(resolve_primary_id::<M, TraceId>(
            self.tables.trace_dir(),
            &mut self.directory_fragment_cache,
            id,
            TraceDirBucketSpec::bucket_start,
            TraceDirSubBucketSpec::sub_bucket_start,
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
        let tables = self.tables;
        let block_num = item.block_num;
        cached_block_ref_with_fallback(
            &mut self.block_ref_cache,
            tables,
            block_num,
            item.block_hash,
            async {
                Ok(tables
                    .trace_block_records()
                    .get(block_num)
                    .await?
                    .map(|record| record.parent_hash))
            },
        )
        .await
    }

    fn exact_match(&self, item: &Self::Item, filter: &Self::Filter) -> bool {
        filter.matches_trace(item)
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
