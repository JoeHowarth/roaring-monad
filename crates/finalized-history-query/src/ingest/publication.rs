use crate::domain::types::PublicationState;
use crate::error::{Error, Result};
use crate::store::publication::{CasOutcome, PublicationStore};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublicationLease {
    pub owner_id: u64,
    pub epoch: u64,
    pub indexed_finalized_head: u64,
}

impl From<PublicationState> for PublicationLease {
    fn from(value: PublicationState) -> Self {
        Self {
            owner_id: value.owner_id,
            epoch: value.epoch,
            indexed_finalized_head: value.indexed_finalized_head,
        }
    }
}

impl PublicationLease {
    pub fn as_state(self) -> PublicationState {
        PublicationState {
            owner_id: self.owner_id,
            epoch: self.epoch,
            indexed_finalized_head: self.indexed_finalized_head,
        }
    }
}

pub async fn bootstrap_publication_state<P: PublicationStore>(
    publication_store: &P,
    owner_id: u64,
) -> Result<PublicationLease> {
    let initial = PublicationState {
        owner_id,
        epoch: 1,
        indexed_finalized_head: 0,
    };
    match publication_store.create_if_absent(&initial).await? {
        CasOutcome::Applied(state) => Ok(state.into()),
        CasOutcome::Failed {
            current: Some(state),
        } => Ok(state.into()),
        CasOutcome::Failed { current: None } => Err(Error::PublicationConflict),
    }
}

pub async fn acquire_publication<P: PublicationStore>(
    publication_store: &P,
    owner_id: u64,
) -> Result<PublicationLease> {
    let mut current = match publication_store.load().await? {
        Some(state) => state,
        None => return bootstrap_publication_state(publication_store, owner_id).await,
    };

    loop {
        if current.owner_id == owner_id {
            return Ok(current.into());
        }

        let next = PublicationState {
            owner_id,
            epoch: current.epoch.saturating_add(1),
            indexed_finalized_head: current.indexed_finalized_head,
        };

        match publication_store.compare_and_set(&current, &next).await? {
            CasOutcome::Applied(state) => return Ok(state.into()),
            CasOutcome::Failed {
                current: Some(state),
            } => current = state,
            CasOutcome::Failed { current: None } => {
                return bootstrap_publication_state(publication_store, owner_id).await;
            }
        }
    }
}
