use std::collections::HashMap;

use alloy_rlp::Header;

use crate::core::ids::TraceId;
use crate::core::refs::BlockRef;
use crate::error::{Error, Result};
use crate::query::runner::{CandidateLocation, QueryMaterializer, cached_block_ref_with_fallback};
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;
use crate::traces::filter::TraceFilter;
use crate::traces::table_specs::{TraceDirBucketSpec, TraceDirSubBucketSpec};
use crate::traces::types::{DirBucket, Trace};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedTraceLocation {
    pub block_num: u64,
    pub local_ordinal: usize,
}

impl CandidateLocation for ResolvedTraceLocation {
    fn block_num(self) -> u64 {
        self.block_num
    }

    fn local_ordinal(self) -> usize {
        self.local_ordinal
    }
}

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

    pub(crate) fn exact_match_trace(&self, item: &Trace, filter: &TraceFilter) -> bool {
        filter.matches_trace(item)
    }

    pub(crate) async fn resolve_trace_id(
        &mut self,
        id: TraceId,
    ) -> Result<Option<ResolvedTraceLocation>> {
        let bucket_start = TraceDirBucketSpec::bucket_start(id);
        if let Some(bucket) = self.tables.trace_dir_buckets().get(bucket_start).await?
            && let Some(entry_index) = containing_bucket_entry(&bucket, id)
        {
            return Ok(Some(ResolvedTraceLocation {
                block_num: bucket.start_block + entry_index as u64,
                local_ordinal: usize::try_from(id.get() - bucket.first_trace_ids[entry_index])
                    .map_err(|_| Error::Decode("trace local ordinal overflow"))?,
            }));
        }

        let sub_bucket_start = TraceDirSubBucketSpec::sub_bucket_start(id);
        if let Some(bucket) = self
            .tables
            .trace_dir_sub_buckets()
            .get(sub_bucket_start)
            .await?
            && let Some(entry_index) = containing_bucket_entry(&bucket, id)
        {
            return Ok(Some(ResolvedTraceLocation {
                block_num: bucket.start_block + entry_index as u64,
                local_ordinal: usize::try_from(id.get() - bucket.first_trace_ids[entry_index])
                    .map_err(|_| Error::Decode("trace local ordinal overflow"))?,
            }));
        }

        let fragments = self.load_directory_fragments(sub_bucket_start).await?;
        let Some(fragment) = fragments.iter().find(|fragment| {
            id.get() >= fragment.first_trace_id && id.get() < fragment.end_trace_id_exclusive
        }) else {
            return Ok(None);
        };

        Ok(Some(ResolvedTraceLocation {
            block_num: fragment.block_num,
            local_ordinal: usize::try_from(id.get() - fragment.first_trace_id)
                .map_err(|_| Error::Decode("trace local ordinal overflow"))?,
        }))
    }

    async fn load_directory_fragments(
        &mut self,
        sub_bucket_start: u64,
    ) -> Result<&[crate::traces::types::DirByBlock]> {
        if let std::collections::hash_map::Entry::Vacant(entry) =
            self.directory_fragment_cache.entry(sub_bucket_start)
        {
            entry.insert(
                self.tables
                    .trace_directory_fragments()
                    .load_sub_bucket_fragments(sub_bucket_start)
                    .await?,
            );
        }
        Ok(self
            .directory_fragment_cache
            .get(&sub_bucket_start)
            .map(Vec::as_slice)
            .unwrap_or(&[]))
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
    type Location = ResolvedTraceLocation;
    type Item = Trace;
    type Filter = TraceFilter;
    type Output = Trace;

    async fn resolve_id(&mut self, id: Self::Id) -> Result<Option<Self::Location>> {
        self.resolve_trace_id(id).await
    }

    async fn load_run(
        &mut self,
        run: &[(Self::Id, Self::Location)],
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
        self.exact_match_trace(item, filter)
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

fn containing_bucket_entry(bucket: &DirBucket, id: TraceId) -> Option<usize> {
    if bucket.first_trace_ids.len() < 2 {
        return None;
    }
    let upper = bucket
        .first_trace_ids
        .partition_point(|first_trace_id| *first_trace_id <= id.get());
    if upper == 0 || upper >= bucket.first_trace_ids.len() {
        return None;
    }
    let entry_index = upper - 1;
    let end = bucket.first_trace_ids[upper];
    (id.get() < end).then_some(entry_index)
}
