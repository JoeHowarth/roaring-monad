use crate::error::{Error, Result};
use crate::ingest::authority::{AuthorityState, WriteAuthority};
use crate::store::publication::{CasOutcome, PublicationStore};

use super::{LeaseAuthority, PublicationLease};

impl<P: PublicationStore> LeaseAuthority<P> {
    async fn renew_if_needed(
        &self,
        lease: PublicationLease,
        observed_upstream_finalized_block: Option<u64>,
    ) -> Result<PublicationLease> {
        let observed_upstream_finalized_block =
            super::require_observed_finalized_block(observed_upstream_finalized_block)?;
        if !lease.needs_renewal(
            observed_upstream_finalized_block,
            self.renew_threshold_blocks,
        ) {
            return Ok(lease);
        }

        let mut current = self
            .publication_store
            .load()
            .await?
            .ok_or(Error::LeaseLost)?;
        loop {
            if current.owner_id != lease.owner_id || current.session_id != lease.session_id {
                return Err(Error::LeaseLost);
            }

            if observed_upstream_finalized_block > current.lease_valid_through_block {
                return Err(Error::LeaseLost);
            }

            let next = crate::domain::types::PublicationState {
                owner_id: current.owner_id,
                session_id: current.session_id,
                indexed_finalized_head: current.indexed_finalized_head,
                lease_valid_through_block: self
                    .lease_valid_through_block(observed_upstream_finalized_block),
            };
            if next.lease_valid_through_block <= current.lease_valid_through_block {
                return Ok(current.into());
            }

            match self
                .publication_store
                .compare_and_set(&current, &next)
                .await?
            {
                CasOutcome::Applied(state) => return Ok(state.into()),
                CasOutcome::Failed {
                    current: Some(state),
                } => current = state,
                CasOutcome::Failed { current: None } => return Err(Error::LeaseLost),
            }
        }
    }
}

impl<P: PublicationStore> WriteAuthority for LeaseAuthority<P> {
    async fn ensure_writer(
        &self,
        observed_upstream_finalized_block: Option<u64>,
    ) -> Result<AuthorityState> {
        let mut guard = self.lease.lock().await;
        let next_lease = match *guard {
            Some(lease) => {
                self.renew_if_needed(lease, observed_upstream_finalized_block)
                    .await?
            }
            None => {
                self.acquire_publication_with_session(observed_upstream_finalized_block)
                    .await?
            }
        };
        *guard = Some(next_lease);
        Ok(AuthorityState {
            indexed_finalized_head: next_lease.indexed_finalized_head,
        })
    }

    async fn publish(&self, new_head: u64) -> Result<()> {
        let mut guard = self.lease.lock().await;
        let lease = (*guard).ok_or(Error::PublicationConflict)?;

        let expected_state = lease.as_state();
        let next_lease = PublicationLease {
            owner_id: lease.owner_id,
            session_id: lease.session_id,
            indexed_finalized_head: new_head,
            lease_valid_through_block: lease.lease_valid_through_block,
        };
        let next_state = next_lease.as_state();
        match self
            .publication_store
            .compare_and_set(&expected_state, &next_state)
            .await?
        {
            CasOutcome::Applied(_) => {
                *guard = Some(next_lease);
                Ok(())
            }
            CasOutcome::Failed {
                current: Some(state),
            } => {
                if state.owner_id != lease.owner_id || state.session_id != lease.session_id {
                    return Err(Error::LeaseLost);
                }
                Err(Error::PublicationConflict)
            }
            CasOutcome::Failed { current: None } => Err(Error::PublicationConflict),
        }
    }

    async fn clear(&self) {
        *self.lease.lock().await = None;
    }
}
