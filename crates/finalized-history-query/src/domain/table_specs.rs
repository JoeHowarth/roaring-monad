use crate::core::ids::{LogId, LogShard, compose_log_id};
use crate::domain::keys::{
    LOCAL_ID_BITS, LOG_DIRECTORY_BUCKET_SIZE, LOG_DIRECTORY_SUB_BUCKET_SIZE, MAX_LOCAL_ID,
    STREAM_PAGE_LOCAL_ID_SPAN, hex_digit, u64_be,
};
use crate::store::traits::{BlobTableId, ScannableTableId, TableId};

pub trait PointTableSpec {
    const TABLE: TableId;
}

pub trait ScannableTableSpec {
    const TABLE: ScannableTableId;
}

pub trait BlobTableSpec {
    const TABLE: BlobTableId;
}

pub struct PublicationStateSpec;
impl PointTableSpec for PublicationStateSpec {
    const TABLE: TableId = TableId::new("publication_state");
}
impl PublicationStateSpec {
    pub const fn key() -> &'static [u8] {
        b"state"
    }
}

pub struct BlockRecordSpec;
impl PointTableSpec for BlockRecordSpec {
    const TABLE: TableId = TableId::new("block_record");
}
impl BlockRecordSpec {
    pub fn key(block_num: u64) -> Vec<u8> {
        u64_be(block_num).to_vec()
    }
}

pub struct BlockLogHeaderSpec;
impl PointTableSpec for BlockLogHeaderSpec {
    const TABLE: TableId = TableId::new("block_log_header");
}
impl BlockLogHeaderSpec {
    pub fn key(block_num: u64) -> Vec<u8> {
        u64_be(block_num).to_vec()
    }
}

pub struct BlockHashIndexSpec;
impl PointTableSpec for BlockHashIndexSpec {
    const TABLE: TableId = TableId::new("block_hash_index");
}
impl BlockHashIndexSpec {
    pub fn key(hash: &[u8; 32]) -> Vec<u8> {
        hash.to_vec()
    }
}

pub struct LogDirBucketSpec;
impl PointTableSpec for LogDirBucketSpec {
    const TABLE: TableId = TableId::new("log_dir_bucket");
}
impl LogDirBucketSpec {
    pub fn bucket_start(global_log_id: impl Into<LogId>) -> u64 {
        let global_log_id = global_log_id.into().get();
        (global_log_id / LOG_DIRECTORY_BUCKET_SIZE) * LOG_DIRECTORY_BUCKET_SIZE
    }

    pub fn key(bucket_start_log_id: u64) -> Vec<u8> {
        u64_be(bucket_start_log_id).to_vec()
    }
}

pub struct LogDirSubBucketSpec;
impl PointTableSpec for LogDirSubBucketSpec {
    const TABLE: TableId = TableId::new("log_dir_sub_bucket");
}
impl LogDirSubBucketSpec {
    pub fn sub_bucket_start(global_log_id: impl Into<LogId>) -> u64 {
        let global_log_id = global_log_id.into().get();
        (global_log_id / LOG_DIRECTORY_SUB_BUCKET_SIZE) * LOG_DIRECTORY_SUB_BUCKET_SIZE
    }

    pub fn key(sub_bucket_start_log_id: u64) -> Vec<u8> {
        u64_be(sub_bucket_start_log_id).to_vec()
    }
}

pub struct LogDirByBlockSpec;
impl ScannableTableSpec for LogDirByBlockSpec {
    const TABLE: ScannableTableId = ScannableTableId::new("log_dir_by_block");
}
impl LogDirByBlockSpec {
    pub fn partition(sub_bucket_start_log_id: u64) -> Vec<u8> {
        u64_be(sub_bucket_start_log_id).to_vec()
    }

    pub fn clustering(block_num: u64) -> Vec<u8> {
        u64_be(block_num).to_vec()
    }
}

pub struct BitmapPageMetaSpec;
impl PointTableSpec for BitmapPageMetaSpec {
    const TABLE: TableId = TableId::new("bitmap_page_meta");
}
impl BitmapPageMetaSpec {
    pub fn key(stream_id: &str, page_start_local: u32) -> Vec<u8> {
        let mut key = format!("{stream_id}/").into_bytes();
        key.extend_from_slice(&u64_be(u64::from(page_start_local)));
        key
    }
}

pub struct BitmapByBlockSpec;
impl ScannableTableSpec for BitmapByBlockSpec {
    const TABLE: ScannableTableId = ScannableTableId::new("bitmap_by_block");
}
impl BitmapByBlockSpec {
    pub fn partition(stream_id: &str, page_start_local: u32) -> Vec<u8> {
        let mut key = format!("{stream_id}/").into_bytes();
        key.extend_from_slice(&u64_be(u64::from(page_start_local)));
        key
    }

    pub fn clustering(block_num: u64) -> Vec<u8> {
        u64_be(block_num).to_vec()
    }
}

pub struct OpenBitmapPageSpec;
impl ScannableTableSpec for OpenBitmapPageSpec {
    const TABLE: ScannableTableId = ScannableTableId::new("open_bitmap_page");
}
impl OpenBitmapPageSpec {
    pub fn partition(shard: LogShard) -> Vec<u8> {
        u64_be(shard.get()).to_vec()
    }

    pub fn page_prefix(page_start_local: u32) -> Vec<u8> {
        let mut key = u64_be(u64::from(page_start_local)).to_vec();
        key.push(b'/');
        key
    }

    pub fn clustering(page_start_local: u32, stream_id: &str) -> Vec<u8> {
        let mut key = Self::page_prefix(page_start_local);
        key.extend_from_slice(stream_id.as_bytes());
        key
    }
}

pub struct BlockLogBlobSpec;
impl BlobTableSpec for BlockLogBlobSpec {
    const TABLE: BlobTableId = BlobTableId::new("block_log_blob");
}
impl BlockLogBlobSpec {
    pub fn key(block_num: u64) -> Vec<u8> {
        u64_be(block_num).to_vec()
    }
}

pub struct BitmapPageBlobSpec;
impl BlobTableSpec for BitmapPageBlobSpec {
    const TABLE: BlobTableId = BlobTableId::new("bitmap_page_blob");
}
impl BitmapPageBlobSpec {
    pub fn key(stream_id: &str, page_start_local: u32) -> Vec<u8> {
        let mut key = format!("{stream_id}/").into_bytes();
        key.extend_from_slice(&u64_be(u64::from(page_start_local)));
        key
    }
}

pub fn point_log_payload_cache_key(block_num: u64, local_ordinal: u64) -> Vec<u8> {
    let mut key = b"point_log_payload/".to_vec();
    key.extend_from_slice(&u64_be(block_num));
    key.extend_from_slice(&u64_be(local_ordinal));
    key
}

pub fn log_shard(global_log_id: impl Into<crate::core::ids::LogId>) -> LogShard {
    global_log_id.into().shard()
}

pub fn log_local(
    global_log_id: impl Into<crate::core::ids::LogId>,
) -> crate::core::ids::LogLocalId {
    global_log_id.into().local()
}

pub fn compose_global_log_id(
    shard: LogShard,
    local: crate::core::ids::LogLocalId,
) -> crate::core::ids::LogId {
    compose_log_id(shard, local)
}

pub fn stream_page_start_local(local_id: u32) -> u32 {
    (local_id / STREAM_PAGE_LOCAL_ID_SPAN) * STREAM_PAGE_LOCAL_ID_SPAN
}

pub fn local_range_for_shard(
    from: crate::core::ids::LogId,
    to_inclusive: crate::core::ids::LogId,
    shard: LogShard,
) -> (crate::core::ids::LogLocalId, crate::core::ids::LogLocalId) {
    let from_shard = log_shard(from);
    let to_shard = log_shard(to_inclusive);
    let local_from = if shard == from_shard {
        log_local(from)
    } else {
        crate::core::ids::LogLocalId::new(0).expect("0 is a valid local id")
    };
    let local_to = if shard == to_shard {
        log_local(to_inclusive)
    } else {
        crate::core::ids::LogLocalId::new(MAX_LOCAL_ID).expect("MAX_LOCAL_ID is valid")
    };
    (local_from, local_to)
}

pub fn stream_id(index_kind: &str, value: &[u8], shard: LogShard) -> String {
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
