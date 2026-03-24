use crate::core::layout::LOCAL_ID_BITS;
use crate::ingest::primary_dir::PrimaryDirCompactionLayout;
use crate::kernel::sharded_streams::{page_start_local, sharded_stream_id};
use crate::store::traits::{BlobTableId, ScannableTableId, TableId};

pub const TRACE_LOCAL_ID_MASK: u64 = (1u64 << LOCAL_ID_BITS) - 1;
pub const MAX_TRACE_LOCAL_ID: u32 = TRACE_LOCAL_ID_MASK as u32;

pub const TRACE_BLOCK_RECORD_TABLE: TableId = TableId::new("trace_block_record");
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

pub const TRACE_DIRECTORY_BUCKET_SIZE: u64 = 1_000_000;
pub const TRACE_DIRECTORY_SUB_BUCKET_SIZE: u64 = 10_000;
pub const TRACE_STREAM_PAGE_LOCAL_ID_SPAN: u32 = 4_096;

pub fn u64_be_trace(v: u64) -> [u8; 8] {
    v.to_be_bytes()
}

pub fn stream_id(index_kind: &str, value: &[u8], shard: crate::core::ids::TraceShard) -> String {
    sharded_stream_id(index_kind, value, shard.get(), LOCAL_ID_BITS)
}

pub fn has_value_stream_id(shard: crate::core::ids::TraceShard) -> String {
    stream_id("has_value", b"\x01", shard)
}

pub fn trace_bucket_start(global_trace_id: impl Into<crate::core::ids::TraceId>) -> u64 {
    let global_trace_id = global_trace_id.into().get();
    (global_trace_id / TRACE_DIRECTORY_BUCKET_SIZE) * TRACE_DIRECTORY_BUCKET_SIZE
}

pub fn trace_sub_bucket_start(global_trace_id: impl Into<crate::core::ids::TraceId>) -> u64 {
    let global_trace_id = global_trace_id.into().get();
    (global_trace_id / TRACE_DIRECTORY_SUB_BUCKET_SIZE) * TRACE_DIRECTORY_SUB_BUCKET_SIZE
}

pub fn trace_stream_page_start_local(local_id: u32) -> u32 {
    page_start_local(local_id, TRACE_STREAM_PAGE_LOCAL_ID_SPAN)
}

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

pub(super) const TRACE_PRIMARY_DIR_LAYOUT: PrimaryDirCompactionLayout =
    PrimaryDirCompactionLayout {
        sub_bucket_span: TRACE_DIRECTORY_SUB_BUCKET_SIZE,
        bucket_span: TRACE_DIRECTORY_BUCKET_SIZE,
        sub_bucket_start: crate::traces::keys::trace_sub_bucket_start,
        bucket_start: crate::traces::keys::trace_bucket_start,
        missing_sentinel_error: "trace directory bucket missing sentinel",
        inconsistent_bucket_error: "inconsistent trace directory bucket boundary across sub-buckets",
        missing_bucket_start_error: "missing trace directory bucket start block",
    };
