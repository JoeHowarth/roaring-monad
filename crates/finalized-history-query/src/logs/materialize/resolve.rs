use crate::core::directory_resolver::resolve_primary_id;
use crate::core::ids::LogId;
use crate::error::Result;
use crate::logs::table_specs::LogDirBucketSpec;
use crate::logs::table_specs::LogDirSubBucketSpec;
use crate::store::traits::{BlobStore, MetaStore};

use super::{LogMaterializer, ResolvedLogLocation};

impl<'a, M: MetaStore, B: BlobStore> LogMaterializer<'a, M, B> {
    pub(crate) async fn resolve_log_id(
        &mut self,
        id: LogId,
    ) -> Result<Option<ResolvedLogLocation>> {
        Ok(resolve_primary_id::<M, LogId>(
            self.tables.log_dir(),
            &mut self.directory_fragment_cache,
            id,
            |value| LogDirBucketSpec::bucket_start(value.get()),
            |value| LogDirSubBucketSpec::sub_bucket_start(value.get()),
        )
        .await?
        .map(|location| ResolvedLogLocation {
            block_num: location.block_num,
            local_ordinal: location.local_ordinal,
        }))
    }
}
