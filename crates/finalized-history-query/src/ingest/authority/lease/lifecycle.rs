use std::fmt;

use futures::lock::MutexGuard;

use crate::error::{Error, Result};
use crate::ingest::authority::{AuthorityState, WriteAuthority, WriteSession};
use crate::store::publication::{CasOutcome, PublicationStore};

use super::{LeaseAuthority, PublicationLease};

pub struct LeaseWriteSession<'a, P> {
    authority: &'a LeaseAuthority<P>,
    _operation: MutexGuard<'a, ()>,
    state: AuthorityState,
}

impl<P> fmt::Debug for LeaseWriteSession<'_, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LeaseWriteSession")
            .field("state", &self.state)
            .finish()
    }
}

impl<P: PublicationStore> LeaseAuthority<P> {
    async fn clear_cached_lease(&self) {
        *self.lease.lock().await = None;
    }

    fn should_invalidate_cached_lease(error: &Error) -> bool {
        matches!(
            error,
            Error::LeaseLost | Error::LeaseObservationUnavailable | Error::PublicationConflict
        )
    }

    async fn can_retry_after_invalidation(&self) -> Result<bool> {
        let current = self.publication_store.load().await?;
        Ok(match current {
            Some(state) => state.owner_id == self.owner_id && state.session_id == self.session_id,
            None => true,
        })
    }

    async fn ensure_cached_lease(
        &self,
        observed_upstream_finalized_block: Option<u64>,
    ) -> Result<PublicationLease> {
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
        Ok(next_lease)
    }

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

    async fn publish_current(
        &self,
        new_head: u64,
        observed_upstream_finalized_block: Option<u64>,
    ) -> Result<()> {
        let mut guard = self.lease.lock().await;
        let lease = (*guard).ok_or(Error::PublicationConflict)?;
        let lease = self
            .renew_if_needed(lease, observed_upstream_finalized_block)
            .await?;

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
}

impl<P> WriteSession for LeaseWriteSession<'_, P>
where
    P: PublicationStore,
{
    fn state(&self) -> AuthorityState {
        self.state
    }

    async fn publish(
        self,
        new_head: u64,
        observed_upstream_finalized_block: Option<u64>,
    ) -> Result<()> {
        let result = self
            .authority
            .publish_current(new_head, observed_upstream_finalized_block)
            .await;
        if let Err(error) = &result
            && LeaseAuthority::<P>::should_invalidate_cached_lease(error)
        {
            self.authority.clear_cached_lease().await;
        }
        result
    }
}

impl<P: PublicationStore> WriteAuthority for LeaseAuthority<P> {
    type Session<'a>
        = LeaseWriteSession<'a, P>
    where
        Self: 'a;

    async fn begin_write(
        &self,
        observed_upstream_finalized_block: Option<u64>,
    ) -> Result<Self::Session<'_>> {
        let operation = self.operation.lock().await;
        let lease = match self
            .ensure_cached_lease(observed_upstream_finalized_block)
            .await
        {
            Ok(lease) => lease,
            Err(error) => {
                if Self::should_invalidate_cached_lease(&error) {
                    let retry = self.can_retry_after_invalidation().await?;
                    self.clear_cached_lease().await;
                    if retry {
                        self.ensure_cached_lease(observed_upstream_finalized_block)
                            .await?
                    } else {
                        return Err(error);
                    }
                } else {
                    return Err(error);
                }
            }
        };
        Ok(LeaseWriteSession {
            authority: self,
            _operation: operation,
            state: AuthorityState {
                indexed_finalized_head: lease.indexed_finalized_head,
            },
        })
    }
}
