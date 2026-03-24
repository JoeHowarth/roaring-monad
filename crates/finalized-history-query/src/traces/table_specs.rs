use crate::core::ids::TraceId;
use crate::ingest::primary_dir::PrimaryDirCompactionLayout;
pub use crate::kernel::table_specs::{BlobTableSpec, PointTableSpec, ScannableTableSpec};
use crate::store::traits::{BlobTableId, ScannableTableId, TableId};
use crate::traces::keys::{
    BLOCK_TRACE_BLOB_TABLE, BLOCK_TRACE_HEADER_TABLE, TRACE_BITMAP_BY_BLOCK_TABLE,
    TRACE_BITMAP_PAGE_BLOB_TABLE, TRACE_BITMAP_PAGE_META_TABLE, TRACE_DIR_BUCKET_TABLE,
    TRACE_DIR_BY_BLOCK_TABLE, TRACE_DIR_SUB_BUCKET_TABLE, TRACE_DIRECTORY_BUCKET_SIZE,
    TRACE_DIRECTORY_SUB_BUCKET_SIZE,
};

pub(super) const TRACE_PRIMARY_DIR_LAYOUT: PrimaryDirCompactionLayout =
    PrimaryDirCompactionLayout {
        sub_bucket_span: TRACE_DIRECTORY_SUB_BUCKET_SIZE,
        bucket_span: TRACE_DIRECTORY_BUCKET_SIZE,
        sub_bucket_start: crate::traces::table_specs::TraceDirSubBucketSpec::sub_bucket_start,
        bucket_start: crate::traces::table_specs::TraceDirBucketSpec::bucket_start,
        missing_sentinel_error: "trace directory bucket missing sentinel",
        inconsistent_bucket_error: "inconsistent trace directory bucket boundary across sub-buckets",
        missing_bucket_start_error: "missing trace directory bucket start block",
    };

pub struct BlockTraceHeaderSpec;
impl PointTableSpec for BlockTraceHeaderSpec {
    const TABLE: TableId = BLOCK_TRACE_HEADER_TABLE;
}
impl BlockTraceHeaderSpec {
    pub fn key(block_num: u64) -> Vec<u8> {
        block_num.to_be_bytes().to_vec()
    }
}

pub struct TraceDirBucketSpec;
impl PointTableSpec for TraceDirBucketSpec {
    const TABLE: TableId = TRACE_DIR_BUCKET_TABLE;
}
impl TraceDirBucketSpec {
    pub fn bucket_start(global_trace_id: impl Into<TraceId>) -> u64 {
        let global_trace_id = global_trace_id.into().get();
        (global_trace_id / TRACE_DIRECTORY_BUCKET_SIZE) * TRACE_DIRECTORY_BUCKET_SIZE
    }

    pub fn key(bucket_start_trace_id: u64) -> Vec<u8> {
        bucket_start_trace_id.to_be_bytes().to_vec()
    }
}

pub struct TraceDirSubBucketSpec;
impl PointTableSpec for TraceDirSubBucketSpec {
    const TABLE: TableId = TRACE_DIR_SUB_BUCKET_TABLE;
}
impl TraceDirSubBucketSpec {
    pub fn sub_bucket_start(global_trace_id: impl Into<TraceId>) -> u64 {
        let global_trace_id = global_trace_id.into().get();
        (global_trace_id / TRACE_DIRECTORY_SUB_BUCKET_SIZE) * TRACE_DIRECTORY_SUB_BUCKET_SIZE
    }

    pub fn key(sub_bucket_start_trace_id: u64) -> Vec<u8> {
        sub_bucket_start_trace_id.to_be_bytes().to_vec()
    }
}

pub struct TraceDirByBlockSpec;
impl ScannableTableSpec for TraceDirByBlockSpec {
    const TABLE: ScannableTableId = TRACE_DIR_BY_BLOCK_TABLE;
}
impl TraceDirByBlockSpec {
    pub fn partition(sub_bucket_start_trace_id: u64) -> Vec<u8> {
        sub_bucket_start_trace_id.to_be_bytes().to_vec()
    }

    pub fn clustering(block_num: u64) -> Vec<u8> {
        block_num.to_be_bytes().to_vec()
    }
}

pub struct TraceBitmapPageMetaSpec;
impl PointTableSpec for TraceBitmapPageMetaSpec {
    const TABLE: TableId = TRACE_BITMAP_PAGE_META_TABLE;
}
impl TraceBitmapPageMetaSpec {
    pub fn key(stream_id: &str, page_start_local: u32) -> Vec<u8> {
        let mut key = format!("{stream_id}/").into_bytes();
        key.extend_from_slice(&u64::from(page_start_local).to_be_bytes());
        key
    }
}

pub struct TraceBitmapByBlockSpec;
impl ScannableTableSpec for TraceBitmapByBlockSpec {
    const TABLE: ScannableTableId = TRACE_BITMAP_BY_BLOCK_TABLE;
}
impl TraceBitmapByBlockSpec {
    pub fn partition(stream_id: &str, page_start_local: u32) -> Vec<u8> {
        let mut key = format!("{stream_id}/").into_bytes();
        key.extend_from_slice(&u64::from(page_start_local).to_be_bytes());
        key
    }

    pub fn clustering(block_num: u64) -> Vec<u8> {
        block_num.to_be_bytes().to_vec()
    }
}

pub struct BlockTraceBlobSpec;
impl BlobTableSpec for BlockTraceBlobSpec {
    const TABLE: BlobTableId = BLOCK_TRACE_BLOB_TABLE;
}
impl BlockTraceBlobSpec {
    pub fn key(block_num: u64) -> Vec<u8> {
        block_num.to_be_bytes().to_vec()
    }
}

pub struct TraceBitmapPageBlobSpec;
impl BlobTableSpec for TraceBitmapPageBlobSpec {
    const TABLE: BlobTableId = TRACE_BITMAP_PAGE_BLOB_TABLE;
}
impl TraceBitmapPageBlobSpec {
    pub fn key(stream_id: &str, page_start_local: u32) -> Vec<u8> {
        let mut key = format!("{stream_id}/").into_bytes();
        key.extend_from_slice(&u64::from(page_start_local).to_be_bytes());
        key
    }
}
