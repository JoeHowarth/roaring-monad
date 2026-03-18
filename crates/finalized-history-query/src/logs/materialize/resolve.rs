use crate::cache::{BytesCache, TableId};
use crate::codec::log::decode_log_dir_fragment;
use crate::codec::log_ref::LogDirectoryBucketRef;
use crate::core::ids::LogId;
use crate::domain::keys::{
    log_directory_bucket_key, log_directory_bucket_start, log_directory_fragment_prefix,
    log_directory_sub_bucket_key, log_directory_sub_bucket_start,
};
use crate::domain::types::LogDirFragment;
use crate::error::{Error, Result};
use crate::store::traits::{BlobStore, MetaStore};

use super::{LogMaterializer, ResolvedLogLocation};

impl<'a, M: MetaStore, B: BlobStore, C: BytesCache> LogMaterializer<'a, M, B, C> {
    pub(crate) async fn resolve_log_id(
        &mut self,
        id: LogId,
    ) -> Result<Option<ResolvedLogLocation>> {
        let bucket_start = log_directory_bucket_start(id);
        if let Some(bucket) = self
            .load_directory_bucket(bucket_start, TableId::LogDirectoryBuckets)
            .await?
            && let Some(entry_index) = containing_bucket_entry_ref(&bucket, id)
        {
            return resolved_location_from_bucket_ref(&bucket, entry_index, id);
        }

        let sub_bucket_start = log_directory_sub_bucket_start(id);
        if let Some(bucket) = self.load_directory_sub_bucket(sub_bucket_start).await?
            && let Some(entry_index) = containing_bucket_entry_ref(&bucket, id)
        {
            return resolved_location_from_bucket_ref(&bucket, entry_index, id);
        }

        let fragments = self.load_directory_fragments(sub_bucket_start).await?;
        let Some(fragment) = fragments.iter().find(|fragment| {
            id.get() >= fragment.first_log_id && id.get() < fragment.end_log_id_exclusive
        }) else {
            return Ok(None);
        };
        Ok(Some(ResolvedLogLocation {
            block_num: fragment.block_num,
            local_ordinal: usize::try_from(id.get() - fragment.first_log_id)
                .map_err(|_| Error::Decode("local ordinal overflow"))?,
        }))
    }

    async fn load_directory_bucket(
        &self,
        bucket_start: u64,
        table: TableId,
    ) -> Result<Option<LogDirectoryBucketRef>> {
        let key = log_directory_bucket_key(bucket_start);
        if let Some(bytes) = self.cache.get(table, &key) {
            return Ok(Some(LogDirectoryBucketRef::new(bytes)?));
        }
        let Some(record) = self.meta_store.get(&key).await? else {
            return Ok(None);
        };
        self.cache
            .put(table, &key, record.value.clone(), record.value.len());
        Ok(Some(LogDirectoryBucketRef::new(record.value)?))
    }

    async fn load_directory_sub_bucket(
        &self,
        sub_bucket_start: u64,
    ) -> Result<Option<LogDirectoryBucketRef>> {
        let key = log_directory_sub_bucket_key(sub_bucket_start);
        let table = TableId::LogDirectorySubBuckets;
        if let Some(bytes) = self.cache.get(table, &key) {
            return Ok(Some(LogDirectoryBucketRef::new(bytes)?));
        }
        let Some(record) = self.meta_store.get(&key).await? else {
            return Ok(None);
        };
        self.cache
            .put(table, &key, record.value.clone(), record.value.len());
        Ok(Some(LogDirectoryBucketRef::new(record.value)?))
    }

    async fn load_directory_fragments(
        &mut self,
        sub_bucket_start: u64,
    ) -> Result<&[LogDirFragment]> {
        if let std::collections::hash_map::Entry::Vacant(entry) =
            self.directory_fragment_cache.entry(sub_bucket_start)
        {
            let page = self
                .meta_store
                .list_prefix(
                    &log_directory_fragment_prefix(sub_bucket_start),
                    None,
                    usize::MAX,
                )
                .await?;
            let mut fragments = Vec::with_capacity(page.keys.len());
            for key in page.keys {
                let Some(record) = self.meta_store.get(&key).await? else {
                    continue;
                };
                fragments.push(decode_log_dir_fragment(&record.value)?);
            }
            fragments.sort_by_key(|fragment| fragment.block_num);
            entry.insert(fragments);
        }
        Ok(self
            .directory_fragment_cache
            .get(&sub_bucket_start)
            .map(Vec::as_slice)
            .unwrap_or(&[]))
    }
}

fn containing_bucket_entry_ref(bucket: &LogDirectoryBucketRef, id: LogId) -> Option<usize> {
    if bucket.count() < 2 {
        return None;
    }
    let upper = bucket.partition_point(|first_log_id| first_log_id <= id.get());
    if upper == 0 || upper >= bucket.count() {
        return None;
    }
    let entry_index = upper - 1;
    let end = bucket.first_log_id(upper);
    if id.get() < end {
        Some(entry_index)
    } else {
        None
    }
}

fn resolved_location_from_bucket_ref(
    bucket: &LogDirectoryBucketRef,
    entry_index: usize,
    id: LogId,
) -> Result<Option<ResolvedLogLocation>> {
    let block_num = bucket.start_block() + entry_index as u64;
    let local_ordinal = usize::try_from(id.get() - bucket.first_log_id(entry_index))
        .map_err(|_| Error::Decode("local ordinal overflow"))?;
    Ok(Some(ResolvedLogLocation {
        block_num,
        local_ordinal,
    }))
}
