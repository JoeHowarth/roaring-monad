pub use crate::core::layout::{
    DIRECTORY_BUCKET_SIZE, DIRECTORY_SUB_BUCKET_SIZE, LOCAL_ID_BITS, LOCAL_ID_MASK, MAX_LOCAL_ID,
    read_u64_be,
};
use crate::ingest::primary_dir::PrimaryDirCompactionLayout;
use crate::store::traits::{ScannableTableId, TableId};

pub const BLOCK_LOG_HEADER_TABLE: TableId = TableId::new("block_log_header");
pub const BLOCK_HASH_INDEX_TABLE: TableId = TableId::new("block_hash_index");
pub const LOG_DIR_BUCKET_TABLE: TableId = TableId::new("log_dir_bucket");
pub const LOG_DIR_SUB_BUCKET_TABLE: TableId = TableId::new("log_dir_sub_bucket");
pub const LOG_DIR_BY_BLOCK_TABLE: ScannableTableId = ScannableTableId::new("log_dir_by_block");
pub const BITMAP_BY_BLOCK_TABLE: ScannableTableId = ScannableTableId::new("bitmap_by_block");
pub const BITMAP_PAGE_META_TABLE: TableId = TableId::new("bitmap_page_meta");
pub const OPEN_BITMAP_PAGE_TABLE: ScannableTableId = ScannableTableId::new("open_bitmap_page");
pub const STREAM_PAGE_LOCAL_ID_SPAN: u32 = 4_096;
pub(super) const LOG_PRIMARY_DIR_LAYOUT: PrimaryDirCompactionLayout = PrimaryDirCompactionLayout {
    sub_bucket_span: DIRECTORY_SUB_BUCKET_SIZE,
    bucket_span: DIRECTORY_BUCKET_SIZE,
    sub_bucket_start: crate::logs::table_specs::LogDirSubBucketSpec::sub_bucket_start,
    bucket_start: crate::logs::table_specs::LogDirBucketSpec::bucket_start,
    missing_sentinel_error: "directory sub-bucket missing sentinel",
    inconsistent_bucket_error: "inconsistent directory bucket boundary across sub-buckets",
    missing_bucket_start_error: "missing directory bucket start block",
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::ids::{LogId, LogLocalId};
    use crate::logs::table_specs;

    #[test]
    fn shard_local_ranges_respect_24_bit_locals() {
        let from = LogId::new(u64::from(MAX_LOCAL_ID) - 2);
        let to = LogId::new(u64::from(MAX_LOCAL_ID) + 2);
        let first_shard = from.shard();
        let second_shard = to.shard();

        assert_eq!(first_shard.get() + 1, second_shard.get());
        assert_eq!(
            table_specs::local_range_for_shard(from, to, first_shard),
            (
                LogLocalId::new(MAX_LOCAL_ID - 2).unwrap(),
                LogLocalId::new(MAX_LOCAL_ID).unwrap(),
            )
        );
        assert_eq!(
            table_specs::local_range_for_shard(from, to, second_shard),
            (LogLocalId::new(0).unwrap(), LogLocalId::new(1).unwrap())
        );
    }

    #[test]
    fn log_id_split_roundtrip_preserves_shards_above_u32_max() {
        let value = LogId::new(((u64::from(u32::MAX) + 1) << LOCAL_ID_BITS) | 7);
        let split_shard = value.shard();
        let local = value.local();

        assert_eq!(split_shard.get(), u64::from(u32::MAX) + 1);
        assert_eq!(crate::core::ids::compose_log_id(split_shard, local), value);
    }
}
