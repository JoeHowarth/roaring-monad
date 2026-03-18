use crate::domain::types::PublicationState;
use crate::error::{Error, Result};
use crate::store::publication::{CasOutcome, FenceStore, PublicationStore};

use super::{LeaseAuthority, PublicationLease};

impl<P: PublicationStore + FenceStore> LeaseAuthority<P> {
    async fn ensure_fence_for(&self, lease: PublicationLease) -> Result<PublicationLease> {
        self.publication_store.advance_fence(lease.epoch).await?;
        Ok(lease)
    }

    pub(super) fn lease_valid_through_block(&self, observed_upstream_finalized_block: u64) -> u64 {
        observed_upstream_finalized_block.saturating_add(self.lease_blocks - 1)
    }

    pub(super) async fn acquire_publication_with_session(
        &self,
        observed_upstream_finalized_block: Option<u64>,
    ) -> Result<PublicationLease> {
        let observed_upstream_finalized_block =
            super::require_observed_finalized_block(observed_upstream_finalized_block)?;
        let mut current = match self.publication_store.load().await? {
            Some(state) => state,
            None => {
                let initial = PublicationState {
                    owner_id: self.owner_id,
                    session_id: self.session_id,
                    epoch: 1,
                    indexed_finalized_head: 0,
                    lease_valid_through_block: self
                        .lease_valid_through_block(observed_upstream_finalized_block),
                };
                match self.publication_store.create_if_absent(&initial).await? {
                    CasOutcome::Applied(state) => return self.ensure_fence_for(state.into()).await,
                    CasOutcome::Failed {
                        current: Some(state),
                    } => state,
                    CasOutcome::Failed { current: None } => {
                        return Err(Error::PublicationConflict);
                    }
                }
            }
        };

        loop {
            if current.owner_id == self.owner_id
                && current.session_id == self.session_id
                && observed_upstream_finalized_block <= current.lease_valid_through_block
            {
                let next = PublicationState {
                    owner_id: self.owner_id,
                    session_id: self.session_id,
                    epoch: current.epoch,
                    indexed_finalized_head: current.indexed_finalized_head,
                    lease_valid_through_block: self
                        .lease_valid_through_block(observed_upstream_finalized_block),
                };
                if next.lease_valid_through_block <= current.lease_valid_through_block {
                    return self.ensure_fence_for(current.into()).await;
                }

                match self
                    .publication_store
                    .compare_and_set(&current, &next)
                    .await?
                {
                    CasOutcome::Applied(state) => return self.ensure_fence_for(state.into()).await,
                    CasOutcome::Failed {
                        current: Some(state),
                    } => {
                        current = state;
                    }
                    CasOutcome::Failed { current: None } => {
                        return Err(Error::PublicationConflict);
                    }
                }
                continue;
            }

            let same_owner = current.owner_id == self.owner_id;
            if !same_owner && observed_upstream_finalized_block <= current.lease_valid_through_block
            {
                return Err(Error::LeaseStillFresh);
            }

            let next = PublicationState {
                owner_id: self.owner_id,
                session_id: self.session_id,
                epoch: current.epoch.saturating_add(1),
                indexed_finalized_head: current.indexed_finalized_head,
                lease_valid_through_block: self
                    .lease_valid_through_block(observed_upstream_finalized_block),
            };

            match self
                .publication_store
                .compare_and_set(&current, &next)
                .await?
            {
                CasOutcome::Applied(state) => return self.ensure_fence_for(state.into()).await,
                CasOutcome::Failed {
                    current: Some(state),
                } => current = state,
                CasOutcome::Failed { current: None } => {
                    return Err(Error::PublicationConflict);
                }
            }
        }
    }
}
