use crate::domain::types::Topic32;

pub const META_STATE_KEY: &[u8] = b"meta/state";
pub const LOG_LOCATOR_PAGE_SIZE: u64 = 1024;
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

pub fn log_locator_key(global_log_id: u64) -> Vec<u8> {
    let mut k = b"log_locators/".to_vec();
    k.extend_from_slice(&u64_be(global_log_id));
    k
}

pub fn log_locator_page_start(global_log_id: u64) -> u64 {
    (global_log_id / LOG_LOCATOR_PAGE_SIZE) * LOG_LOCATOR_PAGE_SIZE
}

pub fn log_locator_page_key(page_start_log_id: u64) -> Vec<u8> {
    let mut k = b"log_locator_pages/".to_vec();
    k.extend_from_slice(&u64_be(page_start_log_id));
    k
}

pub fn log_locator_pages_prefix() -> &'static [u8] {
    b"log_locator_pages/"
}

pub fn log_locators_prefix() -> &'static [u8] {
    b"log_locators/"
}

pub fn log_pack_blob_key(first_log_id: u64) -> Vec<u8> {
    let mut k = b"log_packs/".to_vec();
    k.extend_from_slice(&u64_be(first_log_id));
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

pub fn log_shard(global_log_id: u64) -> u32 {
    (global_log_id >> LOCAL_ID_BITS) as u32
}

pub fn log_local(global_log_id: u64) -> u32 {
    (global_log_id & LOCAL_ID_MASK) as u32
}

pub fn block_shard(block_num: u64) -> u32 {
    (block_num >> LOCAL_ID_BITS) as u32
}

pub fn block_local(block_num: u64) -> u32 {
    (block_num & LOCAL_ID_MASK) as u32
}

pub fn compose_global_log_id(shard: u32, local: u32) -> u64 {
    (u64::from(shard) << LOCAL_ID_BITS) | u64::from(local)
}

pub fn local_range_for_shard(from: u64, to_inclusive: u64, shard: u32) -> (u32, u32) {
    let from_shard = log_shard(from);
    let to_shard = log_shard(to_inclusive);
    let local_from = if shard == from_shard {
        log_local(from)
    } else {
        0
    };
    let local_to = if shard == to_shard {
        log_local(to_inclusive)
    } else {
        MAX_LOCAL_ID
    };
    (local_from, local_to)
}

pub fn block_range_for_shard(from: u64, to_inclusive: u64, shard: u32) -> (u32, u32) {
    let from_shard = block_shard(from);
    let to_shard = block_shard(to_inclusive);
    let local_from = if shard == from_shard {
        block_local(from)
    } else {
        0
    };
    let local_to = if shard == to_shard {
        block_local(to_inclusive)
    } else {
        MAX_LOCAL_ID
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

pub fn topic0_mode_key(topic0: &Topic32) -> Vec<u8> {
    let mut k = b"topic0_mode/".to_vec();
    push_hex(&mut k, topic0);
    k
}

pub fn topic0_stats_key(topic0: &Topic32) -> Vec<u8> {
    let mut k = b"topic0_stats/".to_vec();
    push_hex(&mut k, topic0);
    k
}

pub fn topic0_stats_prefix() -> &'static [u8] {
    b"topic0_stats/"
}

pub fn stream_id(index_kind: &str, value: &[u8], shard: u32) -> String {
    let mut s = String::with_capacity(index_kind.len() + 1 + value.len() * 2 + 1 + SHARD_HEX_WIDTH);
    s.push_str(index_kind);
    s.push('/');
    for b in value {
        s.push(hex_digit((b >> 4) & 0xf));
        s.push(hex_digit(b & 0xf));
    }
    s.push('/');
    s.push_str(&format!("{shard:0width$x}", width = SHARD_HEX_WIDTH));
    s
}

fn push_hex(out: &mut Vec<u8>, value: &[u8]) {
    for b in value {
        out.push(hex_digit((b >> 4) & 0xf) as u8);
        out.push(hex_digit(b & 0xf) as u8);
    }
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
            0u64,
            1,
            u64::from(MAX_LOCAL_ID),
            u64::from(MAX_LOCAL_ID) + 1,
        ];
        for value in values {
            let shard = log_shard(value);
            let local = log_local(value);
            assert_eq!(compose_global_log_id(shard, local), value);
        }
    }

    #[test]
    fn shard_local_ranges_respect_24_bit_locals() {
        let from = u64::from(MAX_LOCAL_ID) - 2;
        let to = u64::from(MAX_LOCAL_ID) + 2;
        let first_shard = log_shard(from);
        let second_shard = log_shard(to);

        assert_eq!(first_shard + 1, second_shard);
        assert_eq!(
            local_range_for_shard(from, to, first_shard),
            (MAX_LOCAL_ID - 2, MAX_LOCAL_ID)
        );
        assert_eq!(local_range_for_shard(from, to, second_shard), (0, 1));
    }
}
