pub use crate::kernel::table_specs::{BlobTableSpec, PointTableSpec, ScannableTableSpec};
use crate::kernel::table_specs::{stream_page_key, u64_key};
use crate::store::traits::{BlobTableId, ScannableTableId, TableId};
use crate::traces::keys::{
    BLOCK_TRACE_BLOB_TABLE, BLOCK_TRACE_HEADER_TABLE, TRACE_BITMAP_BY_BLOCK_TABLE,
    TRACE_BITMAP_PAGE_BLOB_TABLE, TRACE_BITMAP_PAGE_META_TABLE, TRACE_DIR_BUCKET_TABLE,
    TRACE_DIR_BY_BLOCK_TABLE, TRACE_DIR_SUB_BUCKET_TABLE, TRACE_OPEN_BITMAP_PAGE_TABLE,
};

pub struct BlockTraceHeaderSpec;
impl PointTableSpec for BlockTraceHeaderSpec {
    const TABLE: TableId = BLOCK_TRACE_HEADER_TABLE;
}
impl BlockTraceHeaderSpec {
    pub fn key(block_num: u64) -> Vec<u8> {
        u64_key(block_num)
    }
}

pub struct TraceDirBucketSpec;
impl PointTableSpec for TraceDirBucketSpec {
    const TABLE: TableId = TRACE_DIR_BUCKET_TABLE;
}

pub struct TraceDirSubBucketSpec;
impl PointTableSpec for TraceDirSubBucketSpec {
    const TABLE: TableId = TRACE_DIR_SUB_BUCKET_TABLE;
}

pub struct TraceDirByBlockSpec;
impl ScannableTableSpec for TraceDirByBlockSpec {
    const TABLE: ScannableTableId = TRACE_DIR_BY_BLOCK_TABLE;
}

pub struct TraceBitmapPageMetaSpec;
impl PointTableSpec for TraceBitmapPageMetaSpec {
    const TABLE: TableId = TRACE_BITMAP_PAGE_META_TABLE;
}
impl TraceBitmapPageMetaSpec {
    pub fn key(stream_id: &str, page_start_local: u32) -> Vec<u8> {
        stream_page_key(stream_id, page_start_local)
    }
}

pub struct TraceBitmapByBlockSpec;
impl ScannableTableSpec for TraceBitmapByBlockSpec {
    const TABLE: ScannableTableId = TRACE_BITMAP_BY_BLOCK_TABLE;
}
impl TraceBitmapByBlockSpec {
    pub fn partition(stream_id: &str, page_start_local: u32) -> Vec<u8> {
        stream_page_key(stream_id, page_start_local)
    }

    pub fn clustering(block_num: u64) -> Vec<u8> {
        u64_key(block_num)
    }
}

pub struct TraceOpenBitmapPageSpec;
impl ScannableTableSpec for TraceOpenBitmapPageSpec {
    const TABLE: ScannableTableId = TRACE_OPEN_BITMAP_PAGE_TABLE;
}

pub struct BlockTraceBlobSpec;
impl BlobTableSpec for BlockTraceBlobSpec {
    const TABLE: BlobTableId = BLOCK_TRACE_BLOB_TABLE;
}
impl BlockTraceBlobSpec {
    pub fn key(block_num: u64) -> Vec<u8> {
        u64_key(block_num)
    }
}

pub struct TraceBitmapPageBlobSpec;
impl BlobTableSpec for TraceBitmapPageBlobSpec {
    const TABLE: BlobTableId = TRACE_BITMAP_PAGE_BLOB_TABLE;
}
impl TraceBitmapPageBlobSpec {
    pub fn key(stream_id: &str, page_start_local: u32) -> Vec<u8> {
        stream_page_key(stream_id, page_start_local)
    }
}
