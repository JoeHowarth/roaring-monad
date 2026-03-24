use crate::core::ids::{LogId, LogShard};
use crate::core::layout::{LOCAL_ID_BITS, MAX_LOCAL_ID};
use crate::kernel::sharded_streams::sharded_stream_id;
pub use crate::kernel::table_specs::{BlobTableSpec, PointTableSpec, ScannableTableSpec};
use crate::logs::keys::{LOG_DIRECTORY_BUCKET_SIZE, LOG_DIRECTORY_SUB_BUCKET_SIZE};
use crate::store::traits::{BlobTableId, ScannableTableId, TableId};

pub struct BlockLogHeaderSpec;
impl PointTableSpec for BlockLogHeaderSpec {
    const TABLE: TableId = TableId::new("block_log_header");
}
impl BlockLogHeaderSpec {
    pub fn key(block_num: u64) -> Vec<u8> {
        block_num.to_be_bytes().to_vec()
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
        bucket_start_log_id.to_be_bytes().to_vec()
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
        sub_bucket_start_log_id.to_be_bytes().to_vec()
    }
}

pub struct LogDirByBlockSpec;
impl ScannableTableSpec for LogDirByBlockSpec {
    const TABLE: ScannableTableId = ScannableTableId::new("log_dir_by_block");
}
impl LogDirByBlockSpec {
    pub fn partition(sub_bucket_start_log_id: u64) -> Vec<u8> {
        sub_bucket_start_log_id.to_be_bytes().to_vec()
    }

    pub fn clustering(block_num: u64) -> Vec<u8> {
        block_num.to_be_bytes().to_vec()
    }
}

pub struct BitmapPageMetaSpec;
impl PointTableSpec for BitmapPageMetaSpec {
    const TABLE: TableId = TableId::new("bitmap_page_meta");
}
impl BitmapPageMetaSpec {
    pub fn key(stream_id: &str, page_start_local: u32) -> Vec<u8> {
        let mut key = format!("{stream_id}/").into_bytes();
        key.extend_from_slice(&u64::from(page_start_local).to_be_bytes());
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
        key.extend_from_slice(&u64::from(page_start_local).to_be_bytes());
        key
    }

    pub fn clustering(block_num: u64) -> Vec<u8> {
        block_num.to_be_bytes().to_vec()
    }
}

pub struct OpenBitmapPageSpec;
impl ScannableTableSpec for OpenBitmapPageSpec {
    const TABLE: ScannableTableId = ScannableTableId::new("open_bitmap_page");
}
impl OpenBitmapPageSpec {
    pub fn partition(shard: LogShard) -> Vec<u8> {
        shard.get().to_be_bytes().to_vec()
    }

    pub fn page_prefix(page_start_local: u32) -> Vec<u8> {
        let mut key = u64::from(page_start_local).to_be_bytes().to_vec();
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
        block_num.to_be_bytes().to_vec()
    }
}

pub struct BitmapPageBlobSpec;
impl BlobTableSpec for BitmapPageBlobSpec {
    const TABLE: BlobTableId = BlobTableId::new("bitmap_page_blob");
}
impl BitmapPageBlobSpec {
    pub fn key(stream_id: &str, page_start_local: u32) -> Vec<u8> {
        let mut key = format!("{stream_id}/").into_bytes();
        key.extend_from_slice(&u64::from(page_start_local).to_be_bytes());
        key
    }
}

pub fn local_range_for_shard(
    from: crate::core::ids::LogId,
    to_inclusive: crate::core::ids::LogId,
    shard: LogShard,
) -> (crate::core::ids::LogLocalId, crate::core::ids::LogLocalId) {
    let from_shard = from.shard();
    let to_shard = to_inclusive.shard();
    let local_from = if shard == from_shard {
        from.local()
    } else {
        crate::core::ids::LogLocalId::new(0).expect("0 is a valid local id")
    };
    let local_to = if shard == to_shard {
        to_inclusive.local()
    } else {
        crate::core::ids::LogLocalId::new(MAX_LOCAL_ID).expect("MAX_LOCAL_ID is valid")
    };
    (local_from, local_to)
}

pub fn stream_id(index_kind: &str, value: &[u8], shard: LogShard) -> String {
    sharded_stream_id(index_kind, value, shard.get(), LOCAL_ID_BITS)
}
