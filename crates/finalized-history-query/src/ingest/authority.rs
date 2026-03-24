use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::lock::{Mutex, MutexGuard};
use rand::random;

use crate::error::{Error, Result};
use crate::store::publication::{CasOutcome, PublicationState, PublicationStore, SessionId};

// --- traits ---

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteContinuity {
    /// This process created the first publication state for the service.
    Fresh,
    /// This process continued the same live publication session without an
    /// ownership transition.
    Continuous,
    /// This process had to reacquire publication ownership after a handoff,
    /// lease expiry, or any other continuity break.
    Reacquired,
}

impl WriteContinuity {
    pub fn requires_recovery(self) -> bool {
        matches!(self, Self::Fresh | Self::Reacquired)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthorityState {
    pub indexed_finalized_head: u64,
    pub continuity: WriteContinuity,
}

#[allow(async_fn_in_trait)]
pub trait WriteSession: Send {
    fn state(&self) -> AuthorityState;

    async fn publish(
        self,
        new_head: u64,
        observed_upstream_finalized_block: Option<u64>,
    ) -> Result<()>;
}

#[allow(async_fn_in_trait)]
pub trait WriteAuthority: Send + Sync {
    type Session<'a>: WriteSession + 'a
    where
        Self: 'a;

    async fn begin_write(
        &self,
        observed_upstream_finalized_block: Option<u64>,
    ) -> Result<Self::Session<'_>>;
}

// --- read-only authority ---

#[derive(Debug, Default, Clone, Copy)]
pub struct ReadOnlyAuthority;

pub struct ReadOnlyWriteSession<'a> {
    _marker: std::marker::PhantomData<&'a ReadOnlyAuthority>,
}

impl ReadOnlyAuthority {
    fn writer_mode_error() -> Error {
        Error::ReadOnlyMode("reader-only service cannot acquire write authority")
    }
}

impl WriteSession for ReadOnlyWriteSession<'_> {
    fn state(&self) -> AuthorityState {
        unreachable!("reader-only authority never yields a write session")
    }

    async fn publish(
        self,
        _new_head: u64,
        _observed_upstream_finalized_block: Option<u64>,
    ) -> Result<()> {
        unreachable!("reader-only authority never yields a write session")
    }
}

impl WriteAuthority for ReadOnlyAuthority {
    type Session<'a>
        = ReadOnlyWriteSession<'a>
    where
        Self: 'a;

    async fn begin_write(
        &self,
        _observed_upstream_finalized_block: Option<u64>,
    ) -> Result<Self::Session<'_>> {
        Err(Self::writer_mode_error())
    }
}

// --- lease authority ---

pub const DEFAULT_LEASE_BLOCKS: u64 = 10;

static NEXT_SESSION_NONCE: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
pub struct LeaseAuthority<P> {
    publication_store: P,
    owner_id: u64,
    session_id: SessionId,
    lease_blocks: u64,
    renew_threshold_blocks: u64,
    operation: Mutex<()>,
    lease: Mutex<Option<PublicationLease>>,
}

impl<P> LeaseAuthority<P> {
    pub fn new(
        publication_store: P,
        owner_id: u64,
        lease_blocks: u64,
        renew_threshold_blocks: u64,
    ) -> Self {
        Self::with_session(
            publication_store,
            owner_id,
            new_session_id(owner_id),
            lease_blocks,
            renew_threshold_blocks,
        )
    }

    pub(crate) fn with_session(
        publication_store: P,
        owner_id: u64,
        session_id: SessionId,
        lease_blocks: u64,
        renew_threshold_blocks: u64,
    ) -> Self {
        assert!(
            lease_blocks > 0,
            "publication_lease_blocks must be at least 1"
        );
        assert!(
            renew_threshold_blocks < lease_blocks,
            "publication_lease_renew_threshold_blocks must be less than publication_lease_blocks"
        );
        Self {
            publication_store,
            owner_id,
            session_id,
            lease_blocks,
            renew_threshold_blocks,
            operation: Mutex::new(()),
            lease: Mutex::new(None),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PublicationLease {
    owner_id: u64,
    session_id: SessionId,
    indexed_finalized_head: u64,
    lease_valid_through_block: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LeaseBeginOutcome {
    lease: PublicationLease,
    continuity: WriteContinuity,
}

impl From<PublicationState> for PublicationLease {
    fn from(value: PublicationState) -> Self {
        Self {
            owner_id: value.owner_id,
            session_id: value.session_id,
            indexed_finalized_head: value.indexed_finalized_head,
            lease_valid_through_block: value.lease_valid_through_block,
        }
    }
}

impl PublicationLease {
    fn as_state(self) -> PublicationState {
        PublicationState {
            owner_id: self.owner_id,
            session_id: self.session_id,
            indexed_finalized_head: self.indexed_finalized_head,
            lease_valid_through_block: self.lease_valid_through_block,
        }
    }

    fn needs_renewal(
        self,
        observed_upstream_finalized_block: u64,
        renew_threshold_blocks: u64,
    ) -> bool {
        self.lease_valid_through_block
            .saturating_sub(observed_upstream_finalized_block)
            .le(&renew_threshold_blocks)
    }
}

fn require_observed_finalized_block(observed_upstream_finalized_block: Option<u64>) -> Result<u64> {
    observed_upstream_finalized_block.ok_or(Error::LeaseObservationUnavailable)
}

pub fn new_session_id(owner_id: u64) -> SessionId {
    let mut session_id = random::<SessionId>();
    let nonce = NEXT_SESSION_NONCE.fetch_add(1, Ordering::Relaxed);
    let owner_bits = owner_id ^ nonce.rotate_left(17);
    for (slot, byte) in session_id[8..].iter_mut().zip(owner_bits.to_be_bytes()) {
        *slot ^= byte;
    }
    session_id
}

// --- acquisition ---

impl<P: PublicationStore> LeaseAuthority<P> {
    fn lease_valid_through_block(&self, observed_upstream_finalized_block: u64) -> u64 {
        observed_upstream_finalized_block.saturating_add(self.lease_blocks - 1)
    }

    async fn acquire_publication_with_session(
        &self,
        observed_upstream_finalized_block: Option<u64>,
    ) -> Result<LeaseBeginOutcome> {
        let observed_upstream_finalized_block =
            require_observed_finalized_block(observed_upstream_finalized_block)?;
        let mut current = match self.publication_store.load().await? {
            Some(state) => state,
            None => {
                let initial = PublicationState {
                    owner_id: self.owner_id,
                    session_id: self.session_id,
                    indexed_finalized_head: 0,
                    lease_valid_through_block: self
                        .lease_valid_through_block(observed_upstream_finalized_block),
                };
                match self.publication_store.create_if_absent(&initial).await? {
                    CasOutcome::Applied(state) => {
                        return Ok(LeaseBeginOutcome {
                            lease: state.into(),
                            continuity: WriteContinuity::Fresh,
                        });
                    }
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
                    indexed_finalized_head: current.indexed_finalized_head,
                    lease_valid_through_block: self
                        .lease_valid_through_block(observed_upstream_finalized_block),
                };
                if next.lease_valid_through_block <= current.lease_valid_through_block {
                    return Ok(LeaseBeginOutcome {
                        lease: current.into(),
                        continuity: WriteContinuity::Continuous,
                    });
                }

                match self
                    .publication_store
                    .compare_and_set(&current, &next)
                    .await?
                {
                    CasOutcome::Applied(state) => {
                        return Ok(LeaseBeginOutcome {
                            lease: state.into(),
                            continuity: WriteContinuity::Continuous,
                        });
                    }
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
                indexed_finalized_head: current.indexed_finalized_head,
                lease_valid_through_block: self
                    .lease_valid_through_block(observed_upstream_finalized_block),
            };

            match self
                .publication_store
                .compare_and_set(&current, &next)
                .await?
            {
                CasOutcome::Applied(state) => {
                    return Ok(LeaseBeginOutcome {
                        lease: state.into(),
                        continuity: WriteContinuity::Reacquired,
                    });
                }
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

// --- lifecycle ---

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
    ) -> Result<LeaseBeginOutcome> {
        let mut guard = self.lease.lock().await;
        let outcome = match *guard {
            Some(lease) => LeaseBeginOutcome {
                lease: self
                    .renew_if_needed(lease, observed_upstream_finalized_block)
                    .await?,
                continuity: WriteContinuity::Continuous,
            },
            None => {
                self.acquire_publication_with_session(observed_upstream_finalized_block)
                    .await?
            }
        };
        *guard = Some(outcome.lease);
        Ok(outcome)
    }

    async fn renew_if_needed(
        &self,
        lease: PublicationLease,
        observed_upstream_finalized_block: Option<u64>,
    ) -> Result<PublicationLease> {
        let observed_upstream_finalized_block =
            require_observed_finalized_block(observed_upstream_finalized_block)?;
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

            let next = PublicationState {
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
        let begin = match self
            .ensure_cached_lease(observed_upstream_finalized_block)
            .await
        {
            Ok(begin) => begin,
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
                indexed_finalized_head: begin.lease.indexed_finalized_head,
                continuity: begin.continuity,
            },
        })
    }
}

// --- tests ---

#[cfg(test)]
mod tests {
    use futures::executor::block_on;

    use crate::store::meta::InMemoryMetaStore;
    use crate::store::publication::MetaPublicationStore;

    use super::*;

    fn publication_store(store: &InMemoryMetaStore) -> MetaPublicationStore<InMemoryMetaStore> {
        MetaPublicationStore::new(store.clone())
    }

    #[test]
    fn begin_write_does_not_require_recovery_after_cache_invalidation_without_transition() {
        block_on(async {
            let store = InMemoryMetaStore::default();
            let authority =
                LeaseAuthority::with_session(publication_store(&store), 7, [1u8; 16], 50, 10);

            let first = authority.begin_write(Some(100)).await.expect("acquire");
            assert_eq!(first.state().continuity, WriteContinuity::Fresh);
            drop(first);

            authority.clear_cached_lease().await;

            let second = authority
                .begin_write(Some(120))
                .await
                .expect("reuse same publication session after cache invalidation");
            assert!(
                second.state().continuity == WriteContinuity::Continuous,
                "same-session continuity should not trigger takeover recovery just because the local cache was dropped"
            );
        });
    }
}
