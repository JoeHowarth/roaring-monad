use std::collections::HashMap;

use async_trait::async_trait;
use bytes::Bytes;

use crate::codec::log::{decode_block_log_header, decode_log, decode_log_directory_bucket};
use crate::core::execution::PrimaryMaterializer;
use crate::core::range::RangeResolver;
use crate::core::refs::BlockRef;
use crate::domain::keys::{
    block_log_header_key, block_logs_blob_key, log_directory_bucket_key, log_directory_bucket_start,
};
use crate::error::{Error, Result};
use crate::logs::filter::{LogFilter, exact_match};
use crate::logs::state::load_log_block_meta;
use crate::logs::types::{BlockLogHeader, Log, LogDirectoryBucket};
use crate::store::traits::{BlobStore, MetaStore};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ResolvedLogLocation {
    pub block_num: u64,
    pub local_ordinal: usize,
}

pub struct LogMaterializer<'a, M: MetaStore, B: BlobStore> {
    meta_store: &'a M,
    blob_store: &'a B,
    range_resolver: RangeResolver,
    directory_bucket_cache: HashMap<u64, LogDirectoryBucket>,
    block_header_cache: HashMap<u64, BlockLogHeader>,
    block_ref_cache: HashMap<u64, BlockRef>,
}

impl<'a, M: MetaStore, B: BlobStore> LogMaterializer<'a, M, B> {
    pub fn new(meta_store: &'a M, blob_store: &'a B) -> Self {
        Self {
            meta_store,
            blob_store,
            range_resolver: RangeResolver,
            directory_bucket_cache: HashMap::new(),
            block_header_cache: HashMap::new(),
            block_ref_cache: HashMap::new(),
        }
    }

    pub(crate) async fn load_block_header(
        &mut self,
        block_num: u64,
    ) -> Result<Option<BlockLogHeader>> {
        if let std::collections::hash_map::Entry::Vacant(entry) =
            self.block_header_cache.entry(block_num)
        {
            let Some(record) = self
                .meta_store
                .get(&block_log_header_key(block_num))
                .await?
            else {
                return Ok(None);
            };
            entry.insert(decode_block_log_header(&record.value)?);
        }
        Ok(self.block_header_cache.get(&block_num).cloned())
    }

    pub(crate) async fn resolve_log_id(&mut self, id: u64) -> Result<Option<ResolvedLogLocation>> {
        self.lookup_bucket(id, log_directory_bucket_start(id)).await
    }

    pub(crate) async fn read_full_block_blob(&self, block_num: u64) -> Result<Option<Bytes>> {
        self.blob_store
            .get_blob(&block_logs_blob_key(block_num))
            .await
    }

    fn decode_log_from_blob(
        &self,
        header: &BlockLogHeader,
        local_ordinal: usize,
        blob: &[u8],
    ) -> Result<Log> {
        let Some(&start) = header.offsets.get(local_ordinal) else {
            return Err(Error::Decode("block log ordinal out of range"));
        };
        let Some(&end) = header.offsets.get(local_ordinal + 1) else {
            return Err(Error::Decode("block log header missing sentinel"));
        };
        let start = start as usize;
        let end = end as usize;
        if start > end || end > blob.len() {
            return Err(Error::Decode("invalid block log span"));
        }
        decode_log(&blob[start..end])
    }

    async fn lookup_bucket(
        &mut self,
        id: u64,
        bucket_start: u64,
    ) -> Result<Option<ResolvedLogLocation>> {
        let bucket = self.load_directory_bucket(bucket_start).await?;
        let Some(bucket) = bucket else {
            return Ok(None);
        };
        let Some(entry_index) = containing_bucket_entry(&bucket, id) else {
            return Ok(None);
        };
        let block_num = bucket.start_block + entry_index as u64;
        let local_ordinal = usize::try_from(id - bucket.first_log_ids[entry_index])
            .map_err(|_| Error::Decode("local ordinal overflow"))?;
        Ok(Some(ResolvedLogLocation {
            block_num,
            local_ordinal,
        }))
    }

    async fn load_directory_bucket(
        &mut self,
        bucket_start: u64,
    ) -> Result<Option<LogDirectoryBucket>> {
        if let std::collections::hash_map::Entry::Vacant(entry) =
            self.directory_bucket_cache.entry(bucket_start)
        {
            let Some(record) = self
                .meta_store
                .get(&log_directory_bucket_key(bucket_start))
                .await?
            else {
                return Ok(None);
            };
            entry.insert(decode_log_directory_bucket(&record.value)?);
        }
        Ok(self.directory_bucket_cache.get(&bucket_start).cloned())
    }
}

fn containing_bucket_entry(bucket: &LogDirectoryBucket, id: u64) -> Option<usize> {
    if bucket.first_log_ids.len() < 2 {
        return None;
    }
    let upper = bucket
        .first_log_ids
        .partition_point(|first_log_id| *first_log_id <= id);
    if upper == 0 || upper >= bucket.first_log_ids.len() {
        return None;
    }
    let entry_index = upper - 1;
    let end = bucket.first_log_ids[upper];
    if id < end { Some(entry_index) } else { None }
}

#[async_trait]
impl<M: MetaStore, B: BlobStore> PrimaryMaterializer for LogMaterializer<'_, M, B> {
    type Primary = Log;
    type Filter = LogFilter;

    async fn load_by_id(&mut self, id: u64) -> Result<Option<Self::Primary>> {
        let Some(location) = self.resolve_log_id(id).await? else {
            return Ok(None);
        };
        let Some(header) = self.load_block_header(location.block_num).await? else {
            return Ok(None);
        };
        let Some(&start) = header.offsets.get(location.local_ordinal) else {
            return Ok(None);
        };
        let Some(&end) = header.offsets.get(location.local_ordinal + 1) else {
            return Ok(None);
        };
        let Some(bytes) = self
            .blob_store
            .read_range(
                &block_logs_blob_key(location.block_num),
                u64::from(start),
                u64::from(end),
            )
            .await?
        else {
            return Ok(None);
        };
        Ok(Some(decode_log(&bytes)?))
    }

    async fn block_ref_for(&mut self, item: &Self::Primary) -> Result<BlockRef> {
        if let Some(block_ref) = self.block_ref_cache.get(&item.block_num).copied() {
            return Ok(block_ref);
        }

        let block_ref = if let Some(block_ref) = self
            .range_resolver
            .load_block_ref(self.meta_store, item.block_num)
            .await?
        {
            block_ref
        } else {
            let Some(block_meta) = load_log_block_meta(self.meta_store, item.block_num).await?
            else {
                return Err(Error::NotFound);
            };
            BlockRef {
                number: item.block_num,
                hash: item.block_hash,
                parent_hash: block_meta.parent_hash,
            }
        };
        self.block_ref_cache.insert(item.block_num, block_ref);
        Ok(block_ref)
    }

    fn exact_match(&self, item: &Self::Primary, filter: &Self::Filter) -> bool {
        exact_match(item, filter)
    }
}

impl<'a, M: MetaStore, B: BlobStore> LogMaterializer<'a, M, B> {
    pub(crate) fn decode_log_from_cached_block(
        &self,
        header: &BlockLogHeader,
        local_ordinal: usize,
        blob: &[u8],
    ) -> Result<Log> {
        self.decode_log_from_blob(header, local_ordinal, blob)
    }
}

#[cfg(test)]
mod tests {
    use super::LogMaterializer;
    use crate::codec::log::encode_log_directory_bucket;
    use crate::domain::keys::{
        LOG_DIRECTORY_BUCKET_SIZE, log_directory_bucket_key, log_directory_bucket_start,
    };
    use crate::domain::types::LogDirectoryBucket;
    use crate::store::blob::InMemoryBlobStore;
    use crate::store::meta::InMemoryMetaStore;
    use crate::store::traits::{FenceToken, MetaStore, PutCond};
    use futures::executor::block_on;

    #[test]
    fn resolve_log_id_handles_blocks_spanning_more_than_one_bucket() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let blob = InMemoryBlobStore::default();

            let first_log_id = LOG_DIRECTORY_BUCKET_SIZE - 3;
            let log_id = first_log_id + LOG_DIRECTORY_BUCKET_SIZE + 5;
            meta.put(
                &log_directory_bucket_key(log_directory_bucket_start(log_id)),
                encode_log_directory_bucket(&LogDirectoryBucket {
                    start_block: 700,
                    first_log_ids: vec![
                        first_log_id,
                        first_log_id + LOG_DIRECTORY_BUCKET_SIZE + 10,
                    ],
                }),
                PutCond::Any,
                FenceToken(1),
            )
            .await
            .expect("write directory bucket");

            let mut materializer = LogMaterializer::new(&meta, &blob);
            let resolved = materializer
                .resolve_log_id(log_id)
                .await
                .expect("resolve log id");

            assert_eq!(
                resolved,
                Some(super::ResolvedLogLocation {
                    block_num: 700,
                    local_ordinal: (LOG_DIRECTORY_BUCKET_SIZE + 5) as usize,
                })
            );
        });
    }
}
