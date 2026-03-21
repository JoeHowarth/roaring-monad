use crate::codec::log_ref::DirBucketRef;
use crate::core::ids::LogId;
use crate::domain::keys::{log_dir_bucket_start, log_dir_sub_bucket_start};
use crate::domain::types::DirByBlock;
use crate::error::{Error, Result};
use crate::store::traits::{BlobStore, MetaStore};

use super::{LogMaterializer, ResolvedLogLocation};

impl<'a, M: MetaStore, B: BlobStore> LogMaterializer<'a, M, B> {
    pub(crate) async fn resolve_log_id(
        &mut self,
        id: LogId,
    ) -> Result<Option<ResolvedLogLocation>> {
        let bucket_start = log_dir_bucket_start(id);
        if let Some(bucket) = self.load_directory_bucket(bucket_start).await?
            && let Some(entry_index) = containing_bucket_entry_ref(&bucket, id)
        {
            return resolved_location_from_bucket_ref(&bucket, entry_index, id);
        }

        let sub_bucket_start = log_dir_sub_bucket_start(id);
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

    async fn load_directory_bucket(&self, bucket_start: u64) -> Result<Option<DirBucketRef>> {
        self.tables.dir_buckets().get(bucket_start).await
    }

    async fn load_directory_sub_bucket(
        &self,
        sub_bucket_start: u64,
    ) -> Result<Option<DirBucketRef>> {
        self.tables
            .log_dir_sub_buckets()
            .get(sub_bucket_start)
            .await
    }

    async fn load_directory_fragments(&mut self, sub_bucket_start: u64) -> Result<&[DirByBlock]> {
        if let std::collections::hash_map::Entry::Vacant(entry) =
            self.directory_fragment_cache.entry(sub_bucket_start)
        {
            entry.insert(
                self.tables
                    .directory_fragments()
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
}

fn containing_bucket_entry_ref(bucket: &DirBucketRef, id: LogId) -> Option<usize> {
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
    bucket: &DirBucketRef,
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
