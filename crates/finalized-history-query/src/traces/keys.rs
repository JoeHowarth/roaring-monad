use crate::logs::keys::{
    LOCAL_ID_BITS, LOG_DIRECTORY_BUCKET_SIZE, LOG_DIRECTORY_SUB_BUCKET_SIZE, MAX_LOCAL_ID,
    STREAM_PAGE_LOCAL_ID_SPAN, hex_digit, u64_be,
};
use crate::store::traits::{BlobTableId, ScannableTableId, TableId};

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

pub const TRACE_DIRECTORY_BUCKET_SIZE: u64 = LOG_DIRECTORY_BUCKET_SIZE;
pub const TRACE_DIRECTORY_SUB_BUCKET_SIZE: u64 = LOG_DIRECTORY_SUB_BUCKET_SIZE;
pub const TRACE_STREAM_PAGE_LOCAL_ID_SPAN: u32 = STREAM_PAGE_LOCAL_ID_SPAN;

pub fn u64_be_trace(v: u64) -> [u8; 8] {
    u64_be(v)
}

pub fn stream_id(index_kind: &str, value: &[u8], shard: crate::core::ids::TraceShard) -> String {
    let shard_hex_width = ((64 - LOCAL_ID_BITS) as usize).div_ceil(4);
    let mut out =
        String::with_capacity(index_kind.len() + 1 + value.len() * 2 + 1 + shard_hex_width);
    out.push_str(index_kind);
    out.push('/');
    for byte in value {
        out.push(hex_digit((byte >> 4) & 0xf));
        out.push(hex_digit(byte & 0xf));
    }
    out.push('/');
    out.push_str(&format!(
        "{:0width$x}",
        shard.get(),
        width = shard_hex_width
    ));
    out
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
    (local_id / TRACE_STREAM_PAGE_LOCAL_ID_SPAN) * TRACE_STREAM_PAGE_LOCAL_ID_SPAN
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
        crate::core::ids::TraceLocalId::new(MAX_LOCAL_ID).expect("MAX_LOCAL_ID is valid")
    };
    (local_from, local_to)
}
