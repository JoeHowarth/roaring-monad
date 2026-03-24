pub use crate::kernel::table_specs::{BlobTableSpec, PointTableSpec, ScannableTableSpec};
use crate::kernel::table_specs::{stream_page_key, u64_key};
use crate::store::traits::{BlobTableId, ScannableTableId, TableId};

pub struct BlockTraceHeaderSpec;
impl PointTableSpec for BlockTraceHeaderSpec {
    const TABLE: TableId = TableId::new("block_trace_header");
}
impl BlockTraceHeaderSpec {
    pub fn key(block_num: u64) -> Vec<u8> {
        u64_key(block_num)
    }
}

pub struct TraceDirBucketSpec;
impl PointTableSpec for TraceDirBucketSpec {
    const TABLE: TableId = TableId::new("trace_dir_bucket");
}

pub struct TraceDirSubBucketSpec;
impl PointTableSpec for TraceDirSubBucketSpec {
    const TABLE: TableId = TableId::new("trace_dir_sub_bucket");
}

pub struct TraceDirByBlockSpec;
impl ScannableTableSpec for TraceDirByBlockSpec {
    const TABLE: ScannableTableId = ScannableTableId::new("trace_dir_by_block");
}

pub struct TraceBitmapPageMetaSpec;
impl PointTableSpec for TraceBitmapPageMetaSpec {
    const TABLE: TableId = TableId::new("trace_bitmap_page_meta");
}
impl TraceBitmapPageMetaSpec {
    pub fn key(stream_id: &str, page_start_local: u32) -> Vec<u8> {
        stream_page_key(stream_id, page_start_local)
    }
}

pub struct TraceBitmapByBlockSpec;
impl ScannableTableSpec for TraceBitmapByBlockSpec {
    const TABLE: ScannableTableId = ScannableTableId::new("trace_bitmap_by_block");
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
    const TABLE: ScannableTableId = ScannableTableId::new("trace_open_bitmap_page");
}

pub struct BlockTraceBlobSpec;
impl BlobTableSpec for BlockTraceBlobSpec {
    const TABLE: BlobTableId = BlobTableId::new("block_trace_blob");
}
impl BlockTraceBlobSpec {
    pub fn key(block_num: u64) -> Vec<u8> {
        u64_key(block_num)
    }
}

pub struct TraceBitmapPageBlobSpec;
impl BlobTableSpec for TraceBitmapPageBlobSpec {
    const TABLE: BlobTableId = BlobTableId::new("trace_bitmap_page_blob");
}
impl TraceBitmapPageBlobSpec {
    pub fn key(stream_id: &str, page_start_local: u32) -> Vec<u8> {
        stream_page_key(stream_id, page_start_local)
    }
}
