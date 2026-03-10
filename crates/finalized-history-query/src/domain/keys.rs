use crate::core::ids::{LogId, LogLocalId, LogShard, compose_log_id};

pub const META_STATE_KEY: &[u8] = b"meta/state";
pub const LOG_DIRECTORY_BUCKET_SIZE: u64 = 1_000_000;
pub const LOCAL_ID_BITS: u32 = 24;
pub const LOCAL_ID_MASK: u64 = (1u64 << LOCAL_ID_BITS) - 1;
pub const MAX_LOCAL_ID: u32 = LOCAL_ID_MASK as u32;
const SHARD_HEX_WIDTH: usize = ((64 - LOCAL_ID_BITS) as usize).div_ceil(4);

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

pub fn log_directory_bucket_start(global_log_id: LogId) -> u64 {
    (global_log_id.get() / LOG_DIRECTORY_BUCKET_SIZE) * LOG_DIRECTORY_BUCKET_SIZE
}

pub fn log_directory_bucket_key(bucket_start_log_id: u64) -> Vec<u8> {
    let mut k = b"log_dir/".to_vec();
    k.extend_from_slice(&u64_be(bucket_start_log_id));
    k
}

pub fn log_directory_prefix() -> &'static [u8] {
    b"log_dir/"
}

pub fn block_log_header_key(block_num: u64) -> Vec<u8> {
    let mut k = b"block_log_headers/".to_vec();
    k.extend_from_slice(&u64_be(block_num));
    k
}

pub fn block_log_headers_prefix() -> &'static [u8] {
    b"block_log_headers/"
}

pub fn block_logs_blob_key(block_num: u64) -> Vec<u8> {
    let mut k = b"block_logs/".to_vec();
    k.extend_from_slice(&u64_be(block_num));
    k
}

pub fn block_meta_key(block_num: u64) -> Vec<u8> {
    let mut k = b"block_meta/".to_vec();
    k.extend_from_slice(&u64_be(block_num));
    k
}

pub fn block_hash_to_num_key(hash: &[u8; 32]) -> Vec<u8> {
    let mut k = b"block_hash_to_num/".to_vec();
    k.extend_from_slice(hash);
    k
}

pub fn log_shard(global_log_id: impl Into<LogId>) -> LogShard {
    global_log_id.into().shard()
}

pub fn log_local(global_log_id: impl Into<LogId>) -> LogLocalId {
    global_log_id.into().local()
}

pub fn compose_global_log_id(shard: LogShard, local: LogLocalId) -> LogId {
    compose_log_id(shard, local)
}

pub fn local_range_for_shard(
    from: LogId,
    to_inclusive: LogId,
    shard: LogShard,
) -> (LogLocalId, LogLocalId) {
    let from_shard = log_shard(from);
    let to_shard = log_shard(to_inclusive);
    let local_from = if shard == from_shard {
        log_local(from)
    } else {
        LogLocalId::new(0).expect("0 is a valid local id")
    };
    let local_to = if shard == to_shard {
        log_local(to_inclusive)
    } else {
        LogLocalId::new(MAX_LOCAL_ID).expect("MAX_LOCAL_ID is valid")
    };
    (local_from, local_to)
}

pub fn manifest_key(stream_id: &str) -> Vec<u8> {
    format!("manifests/{stream_id}").into_bytes()
}

pub fn tail_key(stream_id: &str) -> Vec<u8> {
    format!("tails/{stream_id}").into_bytes()
}

pub fn chunk_blob_key(stream_id: &str, chunk_seq: u64) -> Vec<u8> {
    let mut k = format!("chunks/{stream_id}/").into_bytes();
    k.extend_from_slice(&u64_be(chunk_seq));
    k
}

pub fn stream_id(index_kind: &str, value: &[u8], shard: LogShard) -> String {
    let mut s = String::with_capacity(index_kind.len() + 1 + value.len() * 2 + 1 + SHARD_HEX_WIDTH);
    s.push_str(index_kind);
    s.push('/');
    for b in value {
        s.push(hex_digit((b >> 4) & 0xf));
        s.push(hex_digit(b & 0xf));
    }
    s.push('/');
    s.push_str(&format!(
        "{:0width$x}",
        shard.get(),
        width = SHARD_HEX_WIDTH
    ));
    s
}

fn hex_digit(v: u8) -> char {
    match v {
        0..=9 => (b'0' + v) as char,
        10..=15 => (b'a' + (v - 10)) as char,
        _ => '0',
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_id_split_roundtrips_across_boundary() {
        let values = [
            LogId::new(0),
            LogId::new(1),
            LogId::new(u64::from(MAX_LOCAL_ID)),
            LogId::new(u64::from(MAX_LOCAL_ID) + 1),
        ];
        for value in values {
            let shard = log_shard(value);
            let local = log_local(value);
            assert_eq!(compose_global_log_id(shard, local), value);
        }
    }

    #[test]
    fn shard_local_ranges_respect_24_bit_locals() {
        let from = LogId::new(u64::from(MAX_LOCAL_ID) - 2);
        let to = LogId::new(u64::from(MAX_LOCAL_ID) + 2);
        let first_shard = log_shard(from);
        let second_shard = log_shard(to);

        assert_eq!(first_shard.get() + 1, second_shard.get());
        assert_eq!(
            local_range_for_shard(from, to, first_shard),
            (
                LogLocalId::new(MAX_LOCAL_ID - 2).unwrap(),
                LogLocalId::new(MAX_LOCAL_ID).unwrap(),
            )
        );
        assert_eq!(
            local_range_for_shard(from, to, second_shard),
            (LogLocalId::new(0).unwrap(), LogLocalId::new(1).unwrap())
        );
    }

    #[test]
    fn log_id_split_roundtrip_preserves_shards_above_u32_max() {
        let value = LogId::new(((u64::from(u32::MAX) + 1) << LOCAL_ID_BITS) | 7);
        let split_shard = log_shard(value);
        let local = log_local(value);

        assert_eq!(split_shard.get(), u64::from(u32::MAX) + 1);
        assert_eq!(compose_global_log_id(split_shard, local), value);
    }

    #[test]
    fn log_directory_bucket_start_aligns_to_bucket_size() {
        assert_eq!(log_directory_bucket_start(LogId::new(0)), 0);
        assert_eq!(log_directory_bucket_start(LogId::new(123)), 0);
        assert_eq!(
            log_directory_bucket_start(LogId::new(LOG_DIRECTORY_BUCKET_SIZE + 17)),
            LOG_DIRECTORY_BUCKET_SIZE
        );
    }
}
