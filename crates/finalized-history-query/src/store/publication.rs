use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::kernel::codec::StorageCodec;
use crate::store::traits::{KvTable, MetaStore, PutCond, TableId};

pub type SessionId = [u8; 16];

pub const PUBLICATION_STATE_TABLE: TableId = TableId::new("publication_state");
pub const PUBLICATION_STATE_SUFFIX: &[u8] = b"state";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct PublicationState {
    pub owner_id: u64,
    pub session_id: SessionId,
    pub indexed_finalized_head: u64,
    pub lease_valid_through_block: u64,
}

impl PublicationState {
    pub fn finalized_head_state(&self) -> FinalizedHeadState {
        FinalizedHeadState {
            indexed_finalized_head: self.indexed_finalized_head,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CasOutcome<T> {
    Applied(T),
    Failed { current: Option<T> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FinalizedHeadState {
    pub indexed_finalized_head: u64,
}

#[derive(Debug, Clone)]
pub struct MetaPublicationStore<M> {
    table: KvTable<M>,
}

impl<M: MetaStore> MetaPublicationStore<M> {
    pub fn new(meta_store: M) -> Self {
        Self {
            table: meta_store.table(PUBLICATION_STATE_TABLE),
        }
    }
}

#[allow(async_fn_in_trait)]
pub trait PublicationStore: Send + Sync {
    async fn load(&self) -> Result<Option<PublicationState>>;

    async fn load_finalized_head_state(&self) -> Result<FinalizedHeadState> {
        Ok(match self.load().await? {
            Some(state) => state.finalized_head_state(),
            None => FinalizedHeadState {
                indexed_finalized_head: 0,
            },
        })
    }

    async fn create_if_absent(
        &self,
        initial: &PublicationState,
    ) -> Result<CasOutcome<PublicationState>>;

    async fn compare_and_set(
        &self,
        expected: &PublicationState,
        next: &PublicationState,
    ) -> Result<CasOutcome<PublicationState>>;
}

impl<M: MetaStore> PublicationStore for MetaPublicationStore<M> {
    async fn load(&self) -> Result<Option<PublicationState>> {
        let Some(record) = self.table.get(PUBLICATION_STATE_SUFFIX).await? else {
            return Ok(None);
        };
        Ok(Some(PublicationState::decode(&record.value)?))
    }

    async fn create_if_absent(
        &self,
        initial: &PublicationState,
    ) -> Result<CasOutcome<PublicationState>> {
        let result = self
            .table
            .put(
                PUBLICATION_STATE_SUFFIX,
                initial.encode(),
                PutCond::IfAbsent,
            )
            .await?;
        if result.applied {
            return Ok(CasOutcome::Applied(initial.clone()));
        }
        Ok(CasOutcome::Failed {
            current: self.load().await?,
        })
    }

    async fn compare_and_set(
        &self,
        expected: &PublicationState,
        next: &PublicationState,
    ) -> Result<CasOutcome<PublicationState>> {
        let Some(current) = self.table.get(PUBLICATION_STATE_SUFFIX).await? else {
            return Ok(CasOutcome::Failed { current: None });
        };
        let current_state = PublicationState::decode(&current.value)?;
        if current_state != *expected {
            return Ok(CasOutcome::Failed {
                current: Some(current_state),
            });
        }

        let result = self
            .table
            .put(
                PUBLICATION_STATE_SUFFIX,
                next.encode(),
                PutCond::IfVersion(current.version),
            )
            .await?;
        if result.applied {
            return Ok(CasOutcome::Applied(next.clone()));
        }

        Ok(CasOutcome::Failed {
            current: self.load().await?,
        })
    }
}

impl<T: PublicationStore> PublicationStore for Arc<T> {
    async fn load(&self) -> Result<Option<PublicationState>> {
        self.as_ref().load().await
    }

    async fn load_finalized_head_state(&self) -> Result<FinalizedHeadState> {
        self.as_ref().load_finalized_head_state().await
    }

    async fn create_if_absent(
        &self,
        initial: &PublicationState,
    ) -> Result<CasOutcome<PublicationState>> {
        self.as_ref().create_if_absent(initial).await
    }

    async fn compare_and_set(
        &self,
        expected: &PublicationState,
        next: &PublicationState,
    ) -> Result<CasOutcome<PublicationState>> {
        self.as_ref().compare_and_set(expected, next).await
    }
}

#[cfg(test)]
mod tests {
    use super::{PublicationState, SessionId};
    use crate::kernel::codec::StorageCodec;

    #[test]
    fn publication_state_storage_codec_roundtrips_through_trait_api() {
        let state = PublicationState {
            owner_id: 17,
            session_id: sample_session_id(),
            indexed_finalized_head: 91,
            lease_valid_through_block: 123,
        };

        let encoded = state.encode();
        let decoded = PublicationState::decode(&encoded).expect("decode publication state");

        assert_eq!(decoded, state);
    }

    fn sample_session_id() -> SessionId {
        *b"session-id-00001"
    }
}
