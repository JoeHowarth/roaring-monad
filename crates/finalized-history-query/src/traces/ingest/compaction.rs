use crate::error::Result;
use crate::ingest::primary_dir::{
    PrimaryDirCompactionLayout, compact_newly_sealed_primary_directory,
};
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;
use crate::traces::keys::{TRACE_DIRECTORY_BUCKET_SIZE, TRACE_DIRECTORY_SUB_BUCKET_SIZE};

const TRACE_PRIMARY_DIR_LAYOUT: PrimaryDirCompactionLayout = PrimaryDirCompactionLayout {
    sub_bucket_span: TRACE_DIRECTORY_SUB_BUCKET_SIZE,
    bucket_span: TRACE_DIRECTORY_BUCKET_SIZE,
    sub_bucket_start: crate::traces::keys::trace_sub_bucket_start,
    bucket_start: crate::traces::keys::trace_bucket_start,
    missing_sentinel_error: "trace directory bucket missing sentinel",
    inconsistent_bucket_error: "inconsistent trace directory bucket boundary across sub-buckets",
    missing_bucket_start_error: "missing trace directory bucket start block",
};

pub async fn compact_newly_sealed_trace_directory<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    from_next_trace_id: u64,
    next_trace_id: u64,
) -> Result<()> {
    compact_newly_sealed_primary_directory(
        tables.trace_dir(),
        from_next_trace_id,
        next_trace_id,
        TRACE_PRIMARY_DIR_LAYOUT,
    )
    .await
}
