use crate::core::ids::TxId;
use crate::core::layout::{DIRECTORY_BUCKET_SIZE, DIRECTORY_SUB_BUCKET_SIZE};
pub use crate::kernel::table_specs::{BlobTableSpec, PointTableSpec, ScannableTableSpec};
use crate::kernel::table_specs::{aligned_u64_start, stream_page_key, u64_key};
use crate::store::traits::{BlobTableId, ScannableTableId, TableId};

pub struct BlockTxHeaderSpec;
impl PointTableSpec for BlockTxHeaderSpec {
    const TABLE: TableId = TableId::new("block_tx_header");
}
impl BlockTxHeaderSpec {
    pub fn key(block_num: u64) -> Vec<u8> {
        u64_key(block_num)
    }
}

pub struct TxDirBucketSpec;
impl PointTableSpec for TxDirBucketSpec {
    const TABLE: TableId = TableId::new("tx_dir_bucket");
}
impl TxDirBucketSpec {
    pub fn bucket_start(global_tx_id: impl Into<TxId>) -> u64 {
        aligned_u64_start(global_tx_id.into().get(), DIRECTORY_BUCKET_SIZE)
    }

    pub fn key(bucket_start_tx_id: u64) -> Vec<u8> {
        u64_key(bucket_start_tx_id)
    }
}

pub struct TxDirSubBucketSpec;
impl PointTableSpec for TxDirSubBucketSpec {
    const TABLE: TableId = TableId::new("tx_dir_sub_bucket");
}
impl TxDirSubBucketSpec {
    pub fn sub_bucket_start(global_tx_id: impl Into<TxId>) -> u64 {
        aligned_u64_start(global_tx_id.into().get(), DIRECTORY_SUB_BUCKET_SIZE)
    }

    pub fn key(sub_bucket_start_tx_id: u64) -> Vec<u8> {
        u64_key(sub_bucket_start_tx_id)
    }
}

pub struct TxDirByBlockSpec;
impl ScannableTableSpec for TxDirByBlockSpec {
    const TABLE: ScannableTableId = ScannableTableId::new("tx_dir_by_block");
}
impl TxDirByBlockSpec {
    pub fn partition(sub_bucket_start_tx_id: u64) -> Vec<u8> {
        u64_key(sub_bucket_start_tx_id)
    }

    pub fn clustering(block_num: u64) -> Vec<u8> {
        u64_key(block_num)
    }
}

pub struct TxBitmapPageMetaSpec;
impl PointTableSpec for TxBitmapPageMetaSpec {
    const TABLE: TableId = TableId::new("tx_bitmap_page_meta");
}
impl TxBitmapPageMetaSpec {
    pub fn key(stream_id: &str, page_start_local: u32) -> Vec<u8> {
        stream_page_key(stream_id, page_start_local)
    }
}

pub struct TxBitmapByBlockSpec;
impl ScannableTableSpec for TxBitmapByBlockSpec {
    const TABLE: ScannableTableId = ScannableTableId::new("tx_bitmap_by_block");
}
impl TxBitmapByBlockSpec {
    pub fn partition(stream_id: &str, page_start_local: u32) -> Vec<u8> {
        stream_page_key(stream_id, page_start_local)
    }

    pub fn clustering(block_num: u64) -> Vec<u8> {
        u64_key(block_num)
    }
}

pub struct TxOpenBitmapPageSpec;
impl ScannableTableSpec for TxOpenBitmapPageSpec {
    const TABLE: ScannableTableId = ScannableTableId::new("tx_open_bitmap_page");
}

pub struct BlockTxBlobSpec;
impl BlobTableSpec for BlockTxBlobSpec {
    const TABLE: BlobTableId = BlobTableId::new("block_tx_blob");
}
impl BlockTxBlobSpec {
    pub fn key(block_num: u64) -> Vec<u8> {
        u64_key(block_num)
    }
}

pub struct TxBitmapPageBlobSpec;
impl BlobTableSpec for TxBitmapPageBlobSpec {
    const TABLE: BlobTableId = BlobTableId::new("tx_bitmap_page_blob");
}
impl TxBitmapPageBlobSpec {
    pub fn key(stream_id: &str, page_start_local: u32) -> Vec<u8> {
        stream_page_key(stream_id, page_start_local)
    }
}
