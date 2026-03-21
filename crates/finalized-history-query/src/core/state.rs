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
    let Some(block_record) = tables.block_records().get(block_num).await? else {
        return Ok(None);
    };
    Ok(Some(BlockIdentity::from((block_num, &block_record))))
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

#[cfg(test)]
mod tests {
    use super::{derive_next_log_id, load_block_identity};
    use crate::domain::types::PublicationState;
    use crate::logs::keys::BLOCK_RECORD_TABLE;
    use crate::logs::table_specs::BlockRecordSpec;
    use crate::logs::types::BlockRecord;
    use crate::store::blob::InMemoryBlobStore;
    use crate::store::meta::InMemoryMetaStore;
    use crate::store::publication::{CasOutcome, MetaPublicationStore, PublicationStore};
    use crate::store::traits::{MetaStore, PutCond};
    use crate::tables::Tables;
    use futures::executor::block_on;

    #[test]
    fn load_block_identity_returns_shared_block_fields() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let tables = Tables::without_cache(meta.clone(), InMemoryBlobStore::default());
            let publication_store = MetaPublicationStore::new(meta.clone());
            assert!(matches!(
                publication_store
                    .create_if_absent(&PublicationState {
                        owner_id: 5,
                        session_id: [5u8; 16],
                        indexed_finalized_head: 7,
                        lease_valid_through_block: u64::MAX,
                    })
                    .await
                    .expect("write publication state"),
                CasOutcome::Applied(_)
            ));
            meta.put(
                BLOCK_RECORD_TABLE,
                &BlockRecordSpec::key(7),
                BlockRecord {
                    block_hash: [3; 32],
                    parent_hash: [4; 32],
                    first_log_id: 90,
                    count: 2,
                }
                .encode(),
                PutCond::Any,
            )
            .await
            .expect("write block meta");

            let state = publication_store
                .load_finalized_head_state()
                .await
                .expect("load finalized head state");
            let identity = load_block_identity(&tables, 7)
                .await
                .expect("load block identity")
                .expect("block identity present");

            assert_eq!(state.indexed_finalized_head, 7);
            assert_eq!(identity.number, 7);
            assert_eq!(identity.hash, [3; 32]);
            assert_eq!(identity.parent_hash, [4; 32]);
            assert_eq!(
                derive_next_log_id(&tables, 7)
                    .await
                    .expect("derive next log id"),
                92
            );
        });
    }
}
