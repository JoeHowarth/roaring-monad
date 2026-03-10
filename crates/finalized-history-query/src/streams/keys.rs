pub use crate::domain::keys::{
    LOCAL_ID_BITS, LOCAL_ID_MASK, LOG_LOCATOR_PAGE_SIZE, MAX_LOCAL_ID, META_STATE_KEY,
    block_hash_to_num_key, block_meta_key, chunk_blob_key, compose_global_log_id,
    local_range_for_shard, log_local, log_locator_key, log_locator_page_key,
    log_locator_page_start, log_locator_pages_prefix, log_locators_prefix, log_pack_blob_key,
    log_shard, manifest_key, read_u64_be, stream_id, tail_key, u64_be,
};

pub fn parse_stream_from_tail_key(key: &[u8]) -> Option<String> {
    let prefix = b"tails/";
    if !key.starts_with(prefix) {
        return None;
    }
    Some(String::from_utf8_lossy(&key[prefix.len()..]).to_string())
}
