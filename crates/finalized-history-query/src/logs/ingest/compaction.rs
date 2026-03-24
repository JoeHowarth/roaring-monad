use crate::error::Result;
use crate::ingest::primary_dir::{
    PrimaryDirCompactionLayout, compact_newly_sealed_primary_directory,
    compact_sealed_primary_directory,
};
use crate::logs::keys::{LOG_DIRECTORY_BUCKET_SIZE, LOG_DIRECTORY_SUB_BUCKET_SIZE};
use crate::logs::table_specs::{LogDirBucketSpec, LogDirSubBucketSpec};
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;

const LOG_PRIMARY_DIR_LAYOUT: PrimaryDirCompactionLayout = PrimaryDirCompactionLayout {
    sub_bucket_span: LOG_DIRECTORY_SUB_BUCKET_SIZE,
    bucket_span: LOG_DIRECTORY_BUCKET_SIZE,
    sub_bucket_start: LogDirSubBucketSpec::sub_bucket_start,
    bucket_start: LogDirBucketSpec::bucket_start,
    missing_sentinel_error: "directory sub-bucket missing sentinel",
    inconsistent_bucket_error: "inconsistent directory bucket boundary across sub-buckets",
    missing_bucket_start_error: "missing directory bucket start block",
};

pub async fn compact_sealed_directory<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    first_log_id: u64,
    count: u32,
    next_log_id: u64,
) -> Result<()> {
    compact_sealed_primary_directory(
        tables.log_dir(),
        first_log_id,
        count,
        next_log_id,
        LOG_PRIMARY_DIR_LAYOUT,
    )
    .await
}

pub async fn compact_newly_sealed_directory<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    from_next_log_id: u64,
    next_log_id: u64,
) -> Result<()> {
    compact_newly_sealed_primary_directory(
        tables.log_dir(),
        from_next_log_id,
        next_log_id,
        LOG_PRIMARY_DIR_LAYOUT,
    )
    .await
}
