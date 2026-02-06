use crate::domain::types::Topic32;

pub const META_STATE_KEY: &[u8] = b"meta/state";

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

pub fn log_key(global_log_id: u64) -> Vec<u8> {
    let mut k = b"logs/".to_vec();
    k.extend_from_slice(&u64_be(global_log_id));
    k
}

pub fn logs_prefix() -> &'static [u8] {
    b"logs/"
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

pub fn stream_id(index_kind: &str, value: &[u8], shard_hi32: u32) -> String {
    let mut s = String::with_capacity(index_kind.len() + 1 + value.len() * 2 + 1 + 8);
    s.push_str(index_kind);
    s.push('/');
    for b in value {
        s.push(hex_digit((b >> 4) & 0xf));
        s.push(hex_digit(b & 0xf));
    }
    s.push('/');
    s.push_str(&format!("{shard_hi32:08x}"));
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
