use crate::store::traits::{ScannableTableId, TableId};

pub const BLOCK_RECORD_TABLE: TableId = TableId::new("block_record");
pub const BLOCK_LOG_HEADER_TABLE: TableId = TableId::new("block_log_header");
pub const BLOCK_HASH_INDEX_TABLE: TableId = TableId::new("block_hash_index");
pub const LOG_DIR_BUCKET_TABLE: TableId = TableId::new("log_dir_bucket");
pub const LOG_DIR_SUB_BUCKET_TABLE: TableId = TableId::new("log_dir_sub_bucket");
pub const LOG_DIR_BY_BLOCK_TABLE: ScannableTableId = ScannableTableId::new("log_dir_by_block");
pub const BITMAP_BY_BLOCK_TABLE: ScannableTableId = ScannableTableId::new("bitmap_by_block");
pub const BITMAP_PAGE_META_TABLE: TableId = TableId::new("bitmap_page_meta");
pub const OPEN_BITMAP_PAGE_TABLE: ScannableTableId = ScannableTableId::new("open_bitmap_page");
pub const LOG_DIRECTORY_BUCKET_SIZE: u64 = 1_000_000;
pub const LOG_DIRECTORY_SUB_BUCKET_SIZE: u64 = 10_000;
pub const STREAM_PAGE_LOCAL_ID_SPAN: u32 = 4_096;
pub const LOCAL_ID_BITS: u32 = 24;
pub const LOCAL_ID_MASK: u64 = (1u64 << LOCAL_ID_BITS) - 1;
pub const MAX_LOCAL_ID: u32 = LOCAL_ID_MASK as u32;

pub fn u64_be(v: u64) -> [u8; 8] {
    v.to_be_bytes()
}

pub fn read_u64_be(bytes: &[u8]) -> Option<u64> {
    if bytes.len() != 8 {
        return None;
    }
    let mut out = [0u8; 8];
    out.copy_from_slice(bytes);
    Some(u64::from_be_bytes(out))
}

pub fn block_hash_index_suffix(hash: &[u8; 32]) -> Vec<u8> {
    hash.to_vec()
}

pub(crate) fn hex_digit(v: u8) -> char {
    match v {
        0..=9 => (b'0' + v) as char,
        10..=15 => (b'a' + (v - 10)) as char,
        _ => '0',
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::ids::{LogId, LogLocalId};
    use crate::logs::table_specs;

    #[test]
    fn shard_local_ranges_respect_24_bit_locals() {
        let from = LogId::new(u64::from(MAX_LOCAL_ID) - 2);
        let to = LogId::new(u64::from(MAX_LOCAL_ID) + 2);
        let first_shard = table_specs::log_shard(from);
        let second_shard = table_specs::log_shard(to);

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
        let split_shard = table_specs::log_shard(value);
        let local = table_specs::log_local(value);

        assert_eq!(split_shard.get(), u64::from(u32::MAX) + 1);
        assert_eq!(
            table_specs::compose_global_log_id(split_shard, local),
            value
        );
    }
}
