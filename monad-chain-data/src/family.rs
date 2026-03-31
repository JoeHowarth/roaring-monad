use alloy_primitives::Log;

pub type Hash32 = [u8; 32];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizedBlock {
    pub block_number: u64,
    pub block_hash: Hash32,
    pub parent_hash: Hash32,
    pub logs_by_tx: Vec<Vec<Log>>,
}
