use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::lock::{Mutex, MutexGuard};
use rand::random;

use crate::error::{Error, Result};
use crate::store::publication::{CasOutcome, PublicationState, PublicationStore, SessionId};

// --- traits ---

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthorityState {
    pub indexed_finalized_head: u64,
    /// True when this write session follows a fresh ownership acquisition or a
    /// reacquisition after lease loss/expiry. Callers can use this to run
    /// takeover-style recovery only when mutable writer state may be stale.
    pub needs_recovery: bool,
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
    needs_recovery: bool,
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
    ) -> Result<PublicationLease> {
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
                    CasOutcome::Applied(state) => return Ok(state.into()),
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
                CasOutcome::Applied(state) => return Ok(state.into()),
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
                needs_recovery: false,
            },
            None => LeaseBeginOutcome {
                lease: self
                    .acquire_publication_with_session(observed_upstream_finalized_block)
                    .await?,
                needs_recovery: true,
            },
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
                needs_recovery: begin.needs_recovery,
            },
        })
    }
}

// --- tests ---

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use futures::executor::block_on;

    use crate::error::Error;
    use crate::store::meta::InMemoryMetaStore;
    use crate::store::publication::{CasOutcome, MetaPublicationStore, PublicationState};

    use super::*;

    struct BootstrapRaceStore {
        state: Mutex<Option<PublicationState>>,
        losing_owner: PublicationState,
    }

    impl BootstrapRaceStore {
        fn new(losing_owner: PublicationState) -> Self {
            Self {
                state: Mutex::new(None),
                losing_owner,
            }
        }
    }

    impl PublicationStore for BootstrapRaceStore {
        async fn load(&self) -> Result<Option<PublicationState>> {
            Ok(self.state.lock().expect("state lock").clone())
        }

        async fn create_if_absent(
            &self,
            _initial: &PublicationState,
        ) -> Result<CasOutcome<PublicationState>> {
            let mut guard = self.state.lock().expect("state lock");
            if guard.is_none() {
                *guard = Some(self.losing_owner.clone());
            }
            Ok(CasOutcome::Failed {
                current: guard.clone(),
            })
        }

        async fn compare_and_set(
            &self,
            expected: &PublicationState,
            next: &PublicationState,
        ) -> Result<CasOutcome<PublicationState>> {
            let mut guard = self.state.lock().expect("state lock");
            match guard.as_ref() {
                Some(current) if current == expected => {
                    *guard = Some(next.clone());
                    Ok(CasOutcome::Applied(next.clone()))
                }
                Some(current) => Ok(CasOutcome::Failed {
                    current: Some(current.clone()),
                }),
                None => Ok(CasOutcome::Failed { current: None }),
            }
        }
    }

    fn publication_store(store: &InMemoryMetaStore) -> MetaPublicationStore<InMemoryMetaStore> {
        MetaPublicationStore::new(store.clone())
    }

    #[test]
    fn acquire_publication_does_not_accept_foreign_owner_after_bootstrap_race() {
        block_on(async {
            let store = BootstrapRaceStore::new(PublicationState {
                owner_id: 9,
                session_id: [9u8; 16],
                indexed_finalized_head: 0,
                lease_valid_through_block: 500,
            });
            let authority = LeaseAuthority::with_session(store, 7, [7u8; 16], 100, 0);

            let session = authority
                .begin_write(Some(1_000))
                .await
                .expect("acquire publication");

            assert_eq!(session.state().indexed_finalized_head, 0);
            assert!(session.state().needs_recovery);
            assert_eq!(
                authority
                    .publication_store
                    .load()
                    .await
                    .expect("load")
                    .expect("state")
                    .session_id,
                [7u8; 16]
            );
        });
    }

    #[test]
    fn fresh_lease_rejects_takeover_until_expiry() {
        block_on(async {
            let store = InMemoryMetaStore::default();
            let first =
                LeaseAuthority::with_session(publication_store(&store), 7, [1u8; 16], 50, 0);
            let second =
                LeaseAuthority::with_session(publication_store(&store), 8, [2u8; 16], 50, 0);

            let _ = first
                .begin_write(Some(100))
                .await
                .expect("bootstrap publication");
            let err = second
                .begin_write(Some(120))
                .await
                .expect_err("fresh foreign lease should reject takeover");

            assert!(matches!(err, Error::LeaseStillFresh));
        });
    }

    #[test]
    fn same_owner_restart_after_expiry_uses_new_session() {
        block_on(async {
            let store = InMemoryMetaStore::default();
            let first =
                LeaseAuthority::with_session(publication_store(&store), 7, [1u8; 16], 50, 0);
            let second =
                LeaseAuthority::with_session(publication_store(&store), 7, [2u8; 16], 50, 0);

            first
                .begin_write(Some(100))
                .await
                .expect("first acquire publication");
            let first_session = first
                .publication_store
                .load()
                .await
                .expect("load")
                .expect("state")
                .session_id;
            second
                .begin_write(Some(151))
                .await
                .expect("same owner restart after expiry");
            let second_session = second
                .publication_store
                .load()
                .await
                .expect("load")
                .expect("state")
                .session_id;

            assert_ne!(second_session, first_session);
        });
    }

    #[test]
    fn same_owner_restart_before_expiry_uses_new_session() {
        block_on(async {
            let store = InMemoryMetaStore::default();
            let first =
                LeaseAuthority::with_session(publication_store(&store), 7, [1u8; 16], 50, 0);
            let second =
                LeaseAuthority::with_session(publication_store(&store), 7, [2u8; 16], 50, 0);

            first
                .begin_write(Some(100))
                .await
                .expect("first acquire publication");
            let store_view = publication_store(&store);
            let first_session = store_view
                .load()
                .await
                .expect("load")
                .expect("state")
                .session_id;
            second
                .begin_write(Some(120))
                .await
                .expect("same owner restart before expiry");
            let second_session = store_view
                .load()
                .await
                .expect("load")
                .expect("state")
                .session_id;

            assert_ne!(second_session, first_session);

            let state = publication_store(&store)
                .load()
                .await
                .expect("load")
                .expect("publication state");
            assert_eq!(state.owner_id, 7);
            assert_eq!(state.session_id, [2u8; 16]);
        });
    }

    #[test]
    fn begin_write_returns_lease_lost_after_external_takeover() {
        block_on(async {
            let store = InMemoryMetaStore::default();
            let authority =
                LeaseAuthority::with_session(publication_store(&store), 7, [1u8; 16], 50, 10);
            authority.begin_write(Some(100)).await.expect("acquire");
            let takeover =
                LeaseAuthority::with_session(publication_store(&store), 8, [2u8; 16], 50, 10);
            let _ = takeover.begin_write(Some(151)).await.expect("takeover");

            let err = authority
                .begin_write(Some(151))
                .await
                .expect_err("begin_write should observe takeover");

            assert!(matches!(err, Error::LeaseLost));
        });
    }

    #[test]
    fn publish_returns_lease_lost_on_session_mismatch() {
        block_on(async {
            let store = InMemoryMetaStore::default();
            let authority =
                LeaseAuthority::with_session(publication_store(&store), 7, [1u8; 16], 50, 0);
            let session = authority.begin_write(Some(100)).await.expect("acquire");
            let takeover =
                LeaseAuthority::with_session(publication_store(&store), 8, [2u8; 16], 50, 0);
            let _ = takeover.begin_write(Some(151)).await.expect("takeover");

            let err = session
                .publish(1, Some(151))
                .await
                .expect_err("publish should fail after takeover");

            assert!(matches!(err, Error::LeaseLost));
        });
    }

    #[test]
    fn publish_returns_publication_conflict_on_head_mismatch() {
        block_on(async {
            let store = InMemoryMetaStore::default();
            let authority =
                LeaseAuthority::with_session(publication_store(&store), 7, [1u8; 16], 50, 0);
            let session = authority.begin_write(Some(100)).await.expect("acquire");
            let pub_store = publication_store(&store);
            let current = pub_store.load().await.expect("load").expect("state");
            let next = PublicationState {
                indexed_finalized_head: 9,
                ..current.clone()
            };
            let _ = pub_store
                .compare_and_set(&current, &next)
                .await
                .expect("mutate state");

            let err = session
                .publish(1, Some(100))
                .await
                .expect_err("publish should reject head mismatch");

            assert!(matches!(err, Error::PublicationConflict));
        });
    }

    #[test]
    fn same_session_reacquire_after_expiry_keeps_session() {
        block_on(async {
            let store = InMemoryMetaStore::default();
            let authority =
                LeaseAuthority::with_session(publication_store(&store), 7, [1u8; 16], 50, 0);

            let first_needs_recovery = authority
                .begin_write(Some(100))
                .await
                .expect("first acquire")
                .state()
                .needs_recovery;
            assert!(first_needs_recovery);
            let first_session = authority
                .publication_store
                .load()
                .await
                .expect("load")
                .expect("state")
                .session_id;
            // valid_through = 100 + 49 = 149; observed 150 > 149 -> expired
            let second_needs_recovery = authority
                .begin_write(Some(150))
                .await
                .expect("reacquire after expiry")
                .state()
                .needs_recovery;
            assert!(second_needs_recovery);
            let second_session = authority
                .publication_store
                .load()
                .await
                .expect("load")
                .expect("state")
                .session_id;

            assert_eq!(second_session, first_session);
        });
    }

    #[test]
    fn same_session_acquire_before_expiry_keeps_session() {
        block_on(async {
            let store = InMemoryMetaStore::default();
            let authority =
                LeaseAuthority::with_session(publication_store(&store), 7, [1u8; 16], 50, 0);

            let first_needs_recovery = authority
                .begin_write(Some(100))
                .await
                .expect("first acquire")
                .state()
                .needs_recovery;
            assert!(first_needs_recovery);
            let first_session = authority
                .publication_store
                .load()
                .await
                .expect("load")
                .expect("state")
                .session_id;
            // valid_through = 149; observed 140 <= 149 -> still valid
            let second_needs_recovery = authority
                .begin_write(Some(140))
                .await
                .expect("reuse before expiry")
                .state()
                .needs_recovery;
            assert!(!second_needs_recovery);
            let second_session = authority
                .publication_store
                .load()
                .await
                .expect("load")
                .expect("state")
                .session_id;

            assert_eq!(second_session, first_session);
        });
    }

    #[test]
    fn begin_write_reacquires_after_own_expiry() {
        block_on(async {
            let store = InMemoryMetaStore::default();
            let authority =
                LeaseAuthority::with_session(publication_store(&store), 7, [1u8; 16], 50, 10);

            let first_needs_recovery = authority
                .begin_write(Some(100))
                .await
                .expect("acquire")
                .state()
                .needs_recovery;
            assert!(first_needs_recovery);
            let first_session = authority
                .publication_store
                .load()
                .await
                .expect("load")
                .expect("state")
                .session_id;
            // valid_through = 149; observed 150 > 149 -> expired, no external takeover
            let second_needs_recovery = authority
                .begin_write(Some(150))
                .await
                .expect("begin_write should reacquire after own expiry")
                .state()
                .needs_recovery;
            assert!(second_needs_recovery);
            let second_session = authority
                .publication_store
                .load()
                .await
                .expect("load")
                .expect("state")
                .session_id;

            assert_eq!(second_session, first_session);
        });
    }

    #[test]
    fn lease_blocks_grants_exact_n_blocks() {
        block_on(async {
            let store = InMemoryMetaStore::default();
            let first =
                LeaseAuthority::with_session(publication_store(&store), 7, [1u8; 16], 10, 0);
            let second =
                LeaseAuthority::with_session(publication_store(&store), 8, [2u8; 16], 10, 0);

            let _ = first.begin_write(Some(100)).await.expect("bootstrap");
            // valid_through = 100 + 9 = 109; observed 109 <= 109 -> still fresh
            let err = second
                .begin_write(Some(109))
                .await
                .expect_err("should be still fresh at last valid block");
            assert!(matches!(err, Error::LeaseStillFresh));

            // observed 110 > 109 -> expired, takeover allowed
            second
                .begin_write(Some(110))
                .await
                .expect("takeover at first expired block");
            assert_eq!(
                second
                    .publication_store
                    .load()
                    .await
                    .expect("load")
                    .expect("state")
                    .session_id,
                [2u8; 16]
            );
        });
    }

    #[test]
    fn acquire_fails_closed_without_observed_finalized_block() {
        block_on(async {
            let store = InMemoryMetaStore::default();
            let authority =
                LeaseAuthority::with_session(publication_store(&store), 7, [1u8; 16], 50, 0);

            let err = authority
                .begin_write(None)
                .await
                .expect_err("missing observation should fail closed");

            assert!(matches!(err, Error::LeaseObservationUnavailable));
        });
    }
}
