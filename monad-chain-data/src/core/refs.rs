use crate::family::Hash32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockRef {
    pub number: u64,
    pub hash: Hash32,
    pub parent_hash: Hash32,
}
