use crate::core::refs::BlockRef;
use crate::error::{Error, Result};
use crate::logs::types::BlockRecord;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;

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

impl From<(u64, &BlockRecord)> for BlockIdentity {
    fn from((number, meta): (u64, &BlockRecord)) -> Self {
        Self {
            number,
            hash: meta.block_hash,
            parent_hash: meta.parent_hash,
        }
    }
}

pub async fn load_block_identity<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    block_num: u64,
) -> Result<Option<BlockIdentity>> {
    tables
        .block_records()
        .get(block_num)
        .await
        .map(|opt| opt.map(|block_record| BlockIdentity::from((block_num, &block_record))))
}

pub async fn derive_next_log_id<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    indexed_finalized_head: u64,
) -> Result<u64> {
    if indexed_finalized_head == 0 {
        return Ok(0);
    }

    let Some(block_record) = tables.block_records().get(indexed_finalized_head).await? else {
        return Err(Error::NotFound);
    };
    Ok(block_record
        .first_log_id
        .saturating_add(u64::from(block_record.count)))
}
