use crate::codec::finalized_state::decode_block_meta;
use crate::core::refs::BlockRef;
use crate::domain::keys::block_meta_key;
use crate::domain::types::{BlockMeta, MetaState, PublicationState};
use crate::error::{Error, Result};
use crate::store::publication::PublicationStore;
use crate::store::traits::MetaStore;

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

impl From<&PublicationState> for FinalizedHeadState {
    fn from(value: &PublicationState) -> Self {
        Self {
            indexed_finalized_head: value.indexed_finalized_head,
            writer_epoch: value.epoch,
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

pub async fn load_finalized_head_state<P: PublicationStore>(
    publication_store: &P,
) -> Result<FinalizedHeadState> {
    Ok(match publication_store.load().await? {
        Some(state) => FinalizedHeadState::from(&state),
        None => FinalizedHeadState {
            indexed_finalized_head: 0,
            writer_epoch: 0,
        },
    })
}

pub async fn load_block_identity<M: MetaStore>(
    meta_store: &M,
    block_num: u64,
) -> Result<Option<BlockIdentity>> {
    let Some(record) = meta_store.get(&block_meta_key(block_num)).await? else {
        return Ok(None);
    };
    let block_meta = decode_block_meta(&record.value)?;
    Ok(Some(BlockIdentity::from((block_num, &block_meta))))
}

pub async fn derive_next_log_id<M: MetaStore>(
    meta_store: &M,
    indexed_finalized_head: u64,
) -> Result<u64> {
    if indexed_finalized_head == 0 {
        return Ok(0);
    }

    let Some(record) = meta_store
        .get(&block_meta_key(indexed_finalized_head))
        .await?
    else {
        return Err(Error::NotFound);
    };
    let block_meta = decode_block_meta(&record.value)?;
    Ok(block_meta
        .first_log_id
        .saturating_add(u64::from(block_meta.count)))
}

#[cfg(test)]
mod tests {
    use super::{derive_next_log_id, load_block_identity, load_finalized_head_state};
    use crate::codec::finalized_state::encode_block_meta;
    use crate::domain::keys::block_meta_key;
    use crate::domain::types::{BlockMeta, PublicationState};
    use crate::store::meta::InMemoryMetaStore;
    use crate::store::publication::{CasOutcome, PublicationStore};
    use crate::store::traits::{FenceToken, MetaStore, PutCond};
    use futures::executor::block_on;

    #[test]
    fn load_finalized_head_state_defaults_when_missing() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let state = load_finalized_head_state(&meta)
                .await
                .expect("load finalized head state");
            assert_eq!(state.indexed_finalized_head, 0);
            assert_eq!(state.writer_epoch, 0);
        });
    }

    #[test]
    fn load_block_identity_returns_shared_block_fields() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            assert!(matches!(
                meta.create_if_absent(&PublicationState {
                    owner_id: 5,
                    epoch: 12,
                    indexed_finalized_head: 7,
                })
                .await
                .expect("write publication state"),
                CasOutcome::Applied(_)
            ));
            meta.put(
                &block_meta_key(7),
                encode_block_meta(&BlockMeta {
                    block_hash: [3; 32],
                    parent_hash: [4; 32],
                    first_log_id: 90,
                    count: 2,
                }),
                PutCond::Any,
                FenceToken(1),
            )
            .await
            .expect("write block meta");

            let state = load_finalized_head_state(&meta)
                .await
                .expect("load finalized head state");
            let identity = load_block_identity(&meta, 7)
                .await
                .expect("load block identity")
                .expect("block identity present");

            assert_eq!(state.indexed_finalized_head, 7);
            assert_eq!(state.writer_epoch, 12);
            assert_eq!(identity.number, 7);
            assert_eq!(identity.hash, [3; 32]);
            assert_eq!(identity.parent_hash, [4; 32]);
            assert_eq!(
                derive_next_log_id(&meta, 7)
                    .await
                    .expect("derive next log id"),
                92
            );
        });
    }
}
