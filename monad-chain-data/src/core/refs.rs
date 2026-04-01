use crate::family::Hash32;
use crate::core::state::BlockRecord;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockRef {
    pub number: u64,
    pub hash: Hash32,
    pub parent_hash: Hash32,
}

impl From<&BlockRecord> for BlockRef {
    fn from(value: &BlockRecord) -> Self {
        Self {
            number: value.block_number,
            hash: value.block_hash,
            parent_hash: value.parent_hash,
        }
    }
}
