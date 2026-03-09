use crate::core::refs::BlockRef;
use crate::domain::types::{BlockMeta, MetaState};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FinalizedHeadState {
    pub indexed_finalized_head: u64,
    pub writer_epoch: u64,
}

impl From<&MetaState> for FinalizedHeadState {
    fn from(value: &MetaState) -> Self {
        Self {
            indexed_finalized_head: value.indexed_finalized_head,
            writer_epoch: value.writer_epoch,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockIdentity {
    pub number: u64,
    pub hash: [u8; 32],
    pub parent_hash: [u8; 32],
}

impl BlockIdentity {
    pub fn into_block_ref(self) -> BlockRef {
        BlockRef {
            number: self.number,
            hash: self.hash,
            parent_hash: self.parent_hash,
        }
    }
}

impl From<(u64, &BlockMeta)> for BlockIdentity {
    fn from((number, meta): (u64, &BlockMeta)) -> Self {
        Self {
            number,
            hash: meta.block_hash,
            parent_hash: meta.parent_hash,
        }
    }
}
