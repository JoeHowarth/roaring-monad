use std::collections::HashMap;

use bytes::Bytes;

use crate::core::ids::TraceId;
use crate::core::range::load_block_ref;
use crate::core::refs::BlockRef;
use crate::error::{Error, Result};
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;
use crate::traces::filter::TraceFilter;
use crate::traces::table_specs::{TraceDirBucketSpec, TraceDirSubBucketSpec};
use crate::traces::types::{DirBucket, Trace};
use crate::traces::view::CallFrameView;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ResolvedTraceLocation {
    pub block_num: u64,
    pub local_ordinal: usize,
}

pub struct TraceMaterializer<'a, M: MetaStore, B: BlobStore> {
    tables: &'a Tables<M, B>,
    directory_fragment_cache: HashMap<u64, Vec<crate::traces::types::DirByBlock>>,
    block_ref_cache: HashMap<u64, BlockRef>,
    block_blob_cache: HashMap<u64, Bytes>,
}

impl<'a, M: MetaStore, B: BlobStore> TraceMaterializer<'a, M, B> {
    pub fn new(tables: &'a Tables<M, B>) -> Self {
        Self {
            tables,
            directory_fragment_cache: HashMap::new(),
            block_ref_cache: HashMap::new(),
            block_blob_cache: HashMap::new(),
        }
    }

    pub(crate) async fn block_ref_for_trace(&mut self, item: &Trace) -> Result<BlockRef> {
        if let Some(block_ref) = self.block_ref_cache.get(&item.block_num).copied() {
            return Ok(block_ref);
        }
        let block_ref = if let Some(block_ref) = load_block_ref(self.tables, item.block_num).await?
        {
            block_ref
        } else {
            let Some(block_record) = self
                .tables
                .trace_block_records()
                .get(item.block_num)
                .await?
            else {
                return Err(Error::NotFound);
            };
            BlockRef {
                number: item.block_num,
                hash: item.block_hash,
                parent_hash: block_record.parent_hash,
            }
        };
        self.block_ref_cache.insert(item.block_num, block_ref);
        Ok(block_ref)
    }

    pub(crate) fn exact_match_trace(&self, item: &Trace, filter: &TraceFilter) -> bool {
        // The owned `Trace` already reflects the frame, so rebuild a narrow filter check over owned fields.
        if let Some(expected) = filter.is_top_level
            && (item.depth == 0) != expected
        {
            return false;
        }
        if let Some(expected) = filter.has_value {
            let has_value = item.value.iter().any(|byte| *byte != 0);
            if has_value != expected {
                return false;
            }
        }
        if let Some(clause) = &filter.from {
            match clause {
                crate::Clause::Any => {}
                crate::Clause::One(value) if &item.from == value => {}
                crate::Clause::Or(values) if values.iter().any(|value| value == &item.from) => {}
                _ => return false,
            }
        }
        if let Some(clause) = &filter.to {
            match clause {
                crate::Clause::Any => {}
                crate::Clause::One(value) if item.to.as_ref() == Some(value) => {}
                crate::Clause::Or(values)
                    if item
                        .to
                        .as_ref()
                        .map(|actual| values.iter().any(|value| value == actual))
                        .unwrap_or(false) => {}
                _ => return false,
            }
        }
        if let Some(clause) = &filter.selector {
            let selector = (item.input.len() >= 4)
                .then(|| <[u8; 4]>::try_from(&item.input[..4]).expect("4-byte selector slice"));
            match clause {
                crate::Clause::Any => {}
                crate::Clause::One(value) if selector.as_ref() == Some(value) => {}
                crate::Clause::Or(values)
                    if selector
                        .as_ref()
                        .map(|actual| values.iter().any(|value| value == actual))
                        .unwrap_or(false) => {}
                _ => return false,
            }
        }
        true
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
        let Some(header) = self.tables.block_trace_headers().get(block_num).await? else {
            return Ok(None);
        };
        let blob = self.load_block_blob(block_num).await?;
        let Some(blob) = blob else {
            return Ok(None);
        };
        let (start, end) = header.trace_range(local_ordinal, blob.len())?;
        let start =
            usize::try_from(start).map_err(|_| Error::Decode("trace blob range overflow"))?;
        let end = usize::try_from(end).map_err(|_| Error::Decode("trace blob range overflow"))?;
        if end > blob.len() || start > end {
            return Err(Error::Decode("invalid trace blob range"));
        }
        let frame = blob.slice(start..end);
        let view = CallFrameView::new(frame.as_ref())?;
        let tx_idx = header
            .tx_idx_for_trace(local_ordinal)
            .ok_or(Error::Decode("missing tx_idx for trace"))?;
        let trace_idx = header
            .trace_idx_in_tx(local_ordinal)
            .ok_or(Error::Decode("missing trace_idx for trace"))?;
        let block_record = self.tables.trace_block_records().get(block_num).await?;
        let block_hash = block_record
            .as_ref()
            .map(|record| record.block_hash)
            .unwrap_or([0; 32]);
        Ok(Some(Trace {
            block_num,
            block_hash,
            tx_idx,
            trace_idx,
            typ: view.typ()?,
            flags: view.flags()?,
            from: *view.from_addr()?,
            to: view.to_addr()?.copied(),
            value: view.value_bytes()?.to_vec(),
            gas: view.gas()?,
            gas_used: view.gas_used()?,
            input: view.input()?.to_vec(),
            output: view.output()?.to_vec(),
            status: view.status()?,
            depth: view.depth()?,
        }))
    }

    async fn load_block_blob(&mut self, block_num: u64) -> Result<Option<Bytes>> {
        if let Some(bytes) = self.block_blob_cache.get(&block_num).cloned() {
            return Ok(Some(bytes));
        }
        let Some(bytes) = self.tables.block_trace_blobs().get(block_num).await? else {
            return Ok(None);
        };
        self.block_blob_cache.insert(block_num, bytes.clone());
        Ok(Some(bytes))
    }
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
