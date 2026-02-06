pub fn u64_be(v: u64) -> [u8; 8] {
    v.to_be_bytes()
}

pub fn log_key(global_log_id: u64) -> Vec<u8> {
    let mut k = b"logs/".to_vec();
    k.extend_from_slice(&u64_be(global_log_id));
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
