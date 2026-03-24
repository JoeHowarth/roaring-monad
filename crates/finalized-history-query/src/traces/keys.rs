use crate::core::layout::LOCAL_ID_BITS;
pub use crate::core::layout::{
    DIRECTORY_BUCKET_SIZE as TRACE_DIRECTORY_BUCKET_SIZE,
    DIRECTORY_SUB_BUCKET_SIZE as TRACE_DIRECTORY_SUB_BUCKET_SIZE,
};
use crate::store::traits::{BlobTableId, ScannableTableId, TableId};

pub const TRACE_LOCAL_ID_MASK: u64 = (1u64 << LOCAL_ID_BITS) - 1;
pub const MAX_TRACE_LOCAL_ID: u32 = TRACE_LOCAL_ID_MASK as u32;

pub const BLOCK_TRACE_HEADER_TABLE: TableId = TableId::new("block_trace_header");
pub const TRACE_DIR_BUCKET_TABLE: TableId = TableId::new("trace_dir_bucket");
pub const TRACE_DIR_SUB_BUCKET_TABLE: TableId = TableId::new("trace_dir_sub_bucket");
pub const TRACE_DIR_BY_BLOCK_TABLE: ScannableTableId = ScannableTableId::new("trace_dir_by_block");
pub const TRACE_BITMAP_BY_BLOCK_TABLE: ScannableTableId =
    ScannableTableId::new("trace_bitmap_by_block");
pub const TRACE_BITMAP_PAGE_META_TABLE: TableId = TableId::new("trace_bitmap_page_meta");
pub const TRACE_OPEN_BITMAP_PAGE_TABLE: ScannableTableId =
    ScannableTableId::new("trace_open_bitmap_page");

pub const BLOCK_TRACE_BLOB_TABLE: BlobTableId = BlobTableId::new("block_trace_blob");
pub const TRACE_BITMAP_PAGE_BLOB_TABLE: BlobTableId = BlobTableId::new("trace_bitmap_page_blob");

pub const TRACE_STREAM_PAGE_LOCAL_ID_SPAN: u32 = 4_096;

pub fn trace_local_range_for_shard(
    from: crate::core::ids::TraceId,
    to_inclusive: crate::core::ids::TraceId,
    shard: crate::core::ids::TraceShard,
) -> (
    crate::core::ids::TraceLocalId,
    crate::core::ids::TraceLocalId,
) {
    let from_shard = from.shard();
    let to_shard = to_inclusive.shard();
    let local_from = if shard == from_shard {
        from.local()
    } else {
        crate::core::ids::TraceLocalId::new(0).expect("0 is a valid trace local id")
    };
    let local_to = if shard == to_shard {
        to_inclusive.local()
    } else {
        crate::core::ids::TraceLocalId::new(MAX_TRACE_LOCAL_ID)
            .expect("MAX_TRACE_LOCAL_ID is valid")
    };
    (local_from, local_to)
}
