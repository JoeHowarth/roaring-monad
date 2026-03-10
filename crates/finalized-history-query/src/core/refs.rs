#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockRef {
    pub number: u64,
    pub hash: [u8; 32],
    pub parent_hash: [u8; 32],
}

impl BlockRef {
    pub const fn zero(number: u64) -> Self {
        Self {
            number,
            hash: [0; 32],
            parent_hash: [0; 32],
        }
    }
}
