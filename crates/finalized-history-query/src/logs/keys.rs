pub use crate::core::layout::{
    DIRECTORY_BUCKET_SIZE as LOG_DIRECTORY_BUCKET_SIZE,
    DIRECTORY_SUB_BUCKET_SIZE as LOG_DIRECTORY_SUB_BUCKET_SIZE,
};
pub use crate::core::layout::{LOCAL_ID_BITS, LOCAL_ID_MASK, MAX_LOCAL_ID, read_u64_be};
pub const STREAM_PAGE_LOCAL_ID_SPAN: u32 = 4_096;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::ids::{LogId, compose_log_id, family_local_range_for_shard};

    #[test]
    fn shard_local_ranges_respect_24_bit_locals() {
        let from = LogId::new(u64::from(MAX_LOCAL_ID) - 2);
        let to = LogId::new(u64::from(MAX_LOCAL_ID) + 2);
        let first_shard = from.shard();
        let second_shard = to.shard();

        assert_eq!(first_shard.get() + 1, second_shard.get());
        assert_eq!(
            family_local_range_for_shard(from, to, first_shard.get()),
            (MAX_LOCAL_ID - 2, MAX_LOCAL_ID)
        );
        assert_eq!(
            family_local_range_for_shard(from, to, second_shard.get()),
            (0, 1)
        );
    }

    #[test]
    fn log_id_split_roundtrip_preserves_shards_above_u32_max() {
        let value = LogId::new(((u64::from(u32::MAX) + 1) << LOCAL_ID_BITS) | 7);
        let split_shard = value.shard();
        let local = value.local();

        assert_eq!(split_shard.get(), u64::from(u32::MAX) + 1);
        assert_eq!(compose_log_id(split_shard, local), value);
    }
}
