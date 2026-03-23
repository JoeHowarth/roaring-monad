use crate::core::ids::{compose_trace_id, TraceId, TraceShard};
use crate::store::traits::{BlobTableId, ScannableTableId, TableId};
pub use crate::tables::{BlobTableSpec, PointTableSpec, ScannableTableSpec};
use crate::traces::keys::{
    trace_bucket_start, trace_stream_page_start_local, trace_sub_bucket_start, u64_be_trace,
    BLOCK_TRACE_BLOB_TABLE, BLOCK_TRACE_HEADER_TABLE, TRACE_BITMAP_BY_BLOCK_TABLE,
    TRACE_BITMAP_PAGE_BLOB_TABLE, TRACE_BITMAP_PAGE_META_TABLE, TRACE_BLOCK_RECORD_TABLE,
    TRACE_DIRECTORY_BUCKET_SIZE, TRACE_DIRECTORY_SUB_BUCKET_SIZE, TRACE_DIR_BUCKET_TABLE,
    TRACE_DIR_BY_BLOCK_TABLE, TRACE_DIR_SUB_BUCKET_TABLE, TRACE_OPEN_BITMAP_PAGE_TABLE,
    TRACE_STREAM_PAGE_LOCAL_ID_SPAN,
};

pub struct TraceBlockRecordSpec;
impl PointTableSpec for TraceBlockRecordSpec {
    const TABLE: TableId = TRACE_BLOCK_RECORD_TABLE;
}
impl TraceBlockRecordSpec {
    pub fn key(block_num: u64) -> Vec<u8> {
        u64_be_trace(block_num).to_vec()
    }
}

pub struct BlockTraceHeaderSpec;
impl PointTableSpec for BlockTraceHeaderSpec {
    const TABLE: TableId = BLOCK_TRACE_HEADER_TABLE;
}
impl BlockTraceHeaderSpec {
    pub fn key(block_num: u64) -> Vec<u8> {
        u64_be_trace(block_num).to_vec()
    }
}

pub struct TraceDirBucketSpec;
impl PointTableSpec for TraceDirBucketSpec {
    const TABLE: TableId = TRACE_DIR_BUCKET_TABLE;
}
impl TraceDirBucketSpec {
    pub fn bucket_start(global_trace_id: impl Into<TraceId>) -> u64 {
        trace_bucket_start(global_trace_id)
    }

    pub fn key(bucket_start_trace_id: u64) -> Vec<u8> {
        u64_be_trace(bucket_start_trace_id).to_vec()
    }
}

pub struct TraceDirSubBucketSpec;
impl PointTableSpec for TraceDirSubBucketSpec {
    const TABLE: TableId = TRACE_DIR_SUB_BUCKET_TABLE;
}
impl TraceDirSubBucketSpec {
    pub fn sub_bucket_start(global_trace_id: impl Into<TraceId>) -> u64 {
        trace_sub_bucket_start(global_trace_id)
    }

    pub fn key(sub_bucket_start_trace_id: u64) -> Vec<u8> {
        u64_be_trace(sub_bucket_start_trace_id).to_vec()
    }
}

pub struct TraceDirByBlockSpec;
impl ScannableTableSpec for TraceDirByBlockSpec {
    const TABLE: ScannableTableId = TRACE_DIR_BY_BLOCK_TABLE;
}
impl TraceDirByBlockSpec {
    pub fn partition(sub_bucket_start_trace_id: u64) -> Vec<u8> {
        u64_be_trace(sub_bucket_start_trace_id).to_vec()
    }

    pub fn clustering(block_num: u64) -> Vec<u8> {
        u64_be_trace(block_num).to_vec()
    }
}

pub struct TraceBitmapPageMetaSpec;
impl PointTableSpec for TraceBitmapPageMetaSpec {
    const TABLE: TableId = TRACE_BITMAP_PAGE_META_TABLE;
}
impl TraceBitmapPageMetaSpec {
    pub fn key(stream_id: &str, page_start_local: u32) -> Vec<u8> {
        let mut key = format!("{stream_id}/").into_bytes();
        key.extend_from_slice(&u64_be_trace(u64::from(page_start_local)));
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
        key.extend_from_slice(&u64_be_trace(u64::from(page_start_local)));
        key
    }

    pub fn clustering(block_num: u64) -> Vec<u8> {
        u64_be_trace(block_num).to_vec()
    }
}

pub struct TraceOpenBitmapPageSpec;
impl ScannableTableSpec for TraceOpenBitmapPageSpec {
    const TABLE: ScannableTableId = TRACE_OPEN_BITMAP_PAGE_TABLE;
}
impl TraceOpenBitmapPageSpec {
    pub fn partition(shard: TraceShard) -> Vec<u8> {
        u64_be_trace(shard.get()).to_vec()
    }

    pub fn page_prefix(page_start_local: u32) -> Vec<u8> {
        let mut key = u64_be_trace(u64::from(page_start_local)).to_vec();
        key.push(b'/');
        key
    }

    pub fn clustering(page_start_local: u32, stream_id: &str) -> Vec<u8> {
        let mut key = Self::page_prefix(page_start_local);
        key.extend_from_slice(stream_id.as_bytes());
        key
    }
}

pub struct BlockTraceBlobSpec;
impl BlobTableSpec for BlockTraceBlobSpec {
    const TABLE: BlobTableId = BLOCK_TRACE_BLOB_TABLE;
}
impl BlockTraceBlobSpec {
    pub fn key(block_num: u64) -> Vec<u8> {
        u64_be_trace(block_num).to_vec()
    }
}

pub struct TraceBitmapPageBlobSpec;
impl BlobTableSpec for TraceBitmapPageBlobSpec {
    const TABLE: BlobTableId = TRACE_BITMAP_PAGE_BLOB_TABLE;
}
impl TraceBitmapPageBlobSpec {
    pub fn key(stream_id: &str, page_start_local: u32) -> Vec<u8> {
        let mut key = format!("{stream_id}/").into_bytes();
        key.extend_from_slice(&u64_be_trace(u64::from(page_start_local)));
        key
    }
}

pub fn trace_shard(global_trace_id: impl Into<TraceId>) -> TraceShard {
    global_trace_id.into().shard()
}

pub fn trace_local(global_trace_id: impl Into<TraceId>) -> crate::core::ids::TraceLocalId {
    global_trace_id.into().local()
}

pub fn compose_global_trace_id(
    shard: TraceShard,
    local: crate::core::ids::TraceLocalId,
) -> TraceId {
    compose_trace_id(shard, local)
}

pub fn stream_page_start_local(local_id: u32) -> u32 {
    trace_stream_page_start_local(local_id)
}

#[allow(dead_code)]
pub const fn directory_bucket_size() -> u64 {
    TRACE_DIRECTORY_BUCKET_SIZE
}

#[allow(dead_code)]
pub const fn directory_sub_bucket_size() -> u64 {
    TRACE_DIRECTORY_SUB_BUCKET_SIZE
}

#[allow(dead_code)]
pub const fn stream_page_span() -> u32 {
    TRACE_STREAM_PAGE_LOCAL_ID_SPAN
}
