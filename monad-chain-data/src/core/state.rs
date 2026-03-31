use crate::family::Hash32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockRecord {
    pub block_number: u64,
    pub block_hash: Hash32,
    pub parent_hash: Hash32,
    pub log_count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublicationState {
    pub indexed_finalized_head: u64,
}
