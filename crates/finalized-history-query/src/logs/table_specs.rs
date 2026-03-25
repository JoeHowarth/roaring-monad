use crate::core::ids::LogId;
use crate::core::layout::{DIRECTORY_BUCKET_SIZE, DIRECTORY_SUB_BUCKET_SIZE};
pub use crate::kernel::table_specs::{BlobTableSpec, PointTableSpec, ScannableTableSpec};
use crate::kernel::table_specs::{aligned_u64_start, stream_page_key, u64_key};
use crate::store::traits::{BlobTableId, ScannableTableId, TableId};

pub struct BlockLogHeaderSpec;
impl PointTableSpec for BlockLogHeaderSpec {
    const TABLE: TableId = TableId::new("block_log_header");
}
impl BlockLogHeaderSpec {
    pub fn key(block_num: u64) -> Vec<u8> {
        u64_key(block_num)
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
        aligned_u64_start(global_log_id.into().get(), DIRECTORY_BUCKET_SIZE)
    }

    pub fn key(bucket_start_log_id: u64) -> Vec<u8> {
        u64_key(bucket_start_log_id)
    }
}

pub struct LogDirSubBucketSpec;
impl PointTableSpec for LogDirSubBucketSpec {
    const TABLE: TableId = TableId::new("log_dir_sub_bucket");
}
impl LogDirSubBucketSpec {
    pub fn sub_bucket_start(global_log_id: impl Into<LogId>) -> u64 {
        aligned_u64_start(global_log_id.into().get(), DIRECTORY_SUB_BUCKET_SIZE)
    }

    pub fn key(sub_bucket_start_log_id: u64) -> Vec<u8> {
        u64_key(sub_bucket_start_log_id)
    }
}

pub struct LogDirByBlockSpec;
impl ScannableTableSpec for LogDirByBlockSpec {
    const TABLE: ScannableTableId = ScannableTableId::new("log_dir_by_block");
}
impl LogDirByBlockSpec {
    pub fn partition(sub_bucket_start_log_id: u64) -> Vec<u8> {
        u64_key(sub_bucket_start_log_id)
    }

    pub fn clustering(block_num: u64) -> Vec<u8> {
        u64_key(block_num)
    }
}

pub struct LogBitmapPageMetaSpec;
impl PointTableSpec for LogBitmapPageMetaSpec {
    const TABLE: TableId = TableId::new("log_bitmap_page_meta");
}
impl LogBitmapPageMetaSpec {
    pub fn key(stream_id: &str, page_start_local: u32) -> Vec<u8> {
        stream_page_key(stream_id, page_start_local)
    }
}

pub struct LogBitmapByBlockSpec;
impl ScannableTableSpec for LogBitmapByBlockSpec {
    const TABLE: ScannableTableId = ScannableTableId::new("log_bitmap_by_block");
}
impl LogBitmapByBlockSpec {
    pub fn partition(stream_id: &str, page_start_local: u32) -> Vec<u8> {
        stream_page_key(stream_id, page_start_local)
    }

    pub fn clustering(block_num: u64) -> Vec<u8> {
        u64_key(block_num)
    }
}

pub struct LogOpenBitmapPageSpec;
impl ScannableTableSpec for LogOpenBitmapPageSpec {
    const TABLE: ScannableTableId = ScannableTableId::new("log_open_bitmap_page");
}

pub struct BlockLogBlobSpec;
impl BlobTableSpec for BlockLogBlobSpec {
    const TABLE: BlobTableId = BlobTableId::new("block_log_blob");
}
impl BlockLogBlobSpec {
    pub fn key(block_num: u64) -> Vec<u8> {
        u64_key(block_num)
    }
}

pub struct LogBitmapPageBlobSpec;
impl BlobTableSpec for LogBitmapPageBlobSpec {
    const TABLE: BlobTableId = BlobTableId::new("log_bitmap_page_blob");
}
impl LogBitmapPageBlobSpec {
    pub fn key(stream_id: &str, page_start_local: u32) -> Vec<u8> {
        stream_page_key(stream_id, page_start_local)
    }
}
