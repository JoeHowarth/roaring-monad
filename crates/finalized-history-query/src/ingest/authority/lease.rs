use rand::random;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::domain::types::{PublicationState, SessionId};
use crate::error::{Error, Result};
use crate::ingest::authority::{WriteAuthority, WriteToken};
use crate::store::publication::{CasOutcome, FenceStore, PublicationStore};
use crate::store::traits::FenceToken;

pub const DEFAULT_LEASE_BLOCKS: u64 = 10;

static NEXT_SESSION_NONCE: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
pub struct LeaseAuthority<P> {
    publication_store: P,
    owner_id: u64,
    session_id: SessionId,
    lease_blocks: u64,
    renew_threshold_blocks: u64,
    lease: futures::lock::Mutex<Option<PublicationLease>>,
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
            lease: futures::lock::Mutex::new(None),
        }
    }
}

impl<P: PublicationStore + FenceStore> LeaseAuthority<P> {
    async fn ensure_fence_for(&self, lease: PublicationLease) -> Result<PublicationLease> {
        self.publication_store.advance_fence(lease.epoch).await?;
        Ok(lease)
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
                    epoch: 1,
                    indexed_finalized_head: 0,
                    lease_valid_through_block: observed_upstream_finalized_block
                        .saturating_add(self.lease_blocks - 1),
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
                    lease_valid_through_block: observed_upstream_finalized_block
                        .saturating_add(self.lease_blocks - 1),
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
                lease_valid_through_block: observed_upstream_finalized_block
                    .saturating_add(self.lease_blocks - 1),
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
            if current.owner_id != lease.owner_id
                || current.session_id != lease.session_id
                || current.epoch != lease.epoch
            {
                return Err(Error::LeaseLost);
            }

            if observed_upstream_finalized_block > current.lease_valid_through_block {
                return Err(Error::LeaseLost);
            }

            let next = PublicationState {
                owner_id: current.owner_id,
                session_id: current.session_id,
                epoch: current.epoch,
                indexed_finalized_head: current.indexed_finalized_head,
                lease_valid_through_block: observed_upstream_finalized_block
                    .saturating_add(self.lease_blocks - 1),
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

impl<P: PublicationStore + FenceStore> WriteAuthority for LeaseAuthority<P> {
    async fn authorize(
        &self,
        current: &WriteToken,
        observed_upstream_finalized_block: Option<u64>,
    ) -> Result<WriteToken> {
        let mut guard = self.lease.lock().await;
        let lease = (*guard).ok_or(Error::PublicationConflict)?;
        if lease.as_token().epoch != current.epoch
            || lease.as_token().indexed_finalized_head != current.indexed_finalized_head
        {
            return Err(Error::PublicationConflict);
        }

        let renewed = self
            .renew_if_needed(lease, observed_upstream_finalized_block)
            .await?;
        *guard = Some(renewed);
        Ok(renewed.as_token())
    }

    async fn publish(&self, current: &WriteToken, new_head: u64) -> Result<WriteToken> {
        let mut guard = self.lease.lock().await;
        let lease = (*guard).ok_or(Error::PublicationConflict)?;
        if lease.epoch != current.epoch
            || lease.indexed_finalized_head != current.indexed_finalized_head
        {
            return Err(Error::PublicationConflict);
        }

        let expected_state = lease.as_state();
        let next_lease = PublicationLease {
            owner_id: lease.owner_id,
            session_id: lease.session_id,
            epoch: lease.epoch,
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
                Ok(next_lease.as_token())
            }
            CasOutcome::Failed {
                current: Some(state),
            } => {
                if state.owner_id != lease.owner_id
                    || state.session_id != lease.session_id
                    || state.epoch != lease.epoch
                {
                    return Err(Error::LeaseLost);
                }
                Err(Error::PublicationConflict)
            }
            CasOutcome::Failed { current: None } => Err(Error::PublicationConflict),
        }
    }

    fn fence(&self, token: &WriteToken) -> FenceToken {
        FenceToken(token.epoch)
    }

    async fn acquire(&self, observed_upstream_finalized_block: Option<u64>) -> Result<WriteToken> {
        let lease = self
            .acquire_publication_with_session(observed_upstream_finalized_block)
            .await?;
        *self.lease.lock().await = Some(lease);
        Ok(lease.as_token())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PublicationLease {
    owner_id: u64,
    session_id: SessionId,
    epoch: u64,
    indexed_finalized_head: u64,
    lease_valid_through_block: u64,
}

impl From<PublicationState> for PublicationLease {
    fn from(value: PublicationState) -> Self {
        Self {
            owner_id: value.owner_id,
            session_id: value.session_id,
            epoch: value.epoch,
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
            epoch: self.epoch,
            indexed_finalized_head: self.indexed_finalized_head,
            lease_valid_through_block: self.lease_valid_through_block,
        }
    }

    fn as_token(self) -> WriteToken {
        WriteToken {
            epoch: self.epoch,
            indexed_finalized_head: self.indexed_finalized_head,
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

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use futures::executor::block_on;

    use crate::domain::types::PublicationState;
    use crate::error::{Error, Result};
    use crate::ingest::authority::WriteAuthority;
    use crate::store::meta::InMemoryMetaStore;
    use crate::store::publication::{CasOutcome, FenceStore, PublicationStore};

    use super::LeaseAuthority;

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

    impl FenceStore for BootstrapRaceStore {
        async fn advance_fence(&self, _min_epoch: u64) -> Result<()> {
            Ok(())
        }

        async fn current_fence(&self) -> Result<u64> {
            Ok(0)
        }
    }

    #[test]
    fn acquire_publication_does_not_accept_foreign_owner_after_bootstrap_race() {
        block_on(async {
            let store = BootstrapRaceStore::new(PublicationState {
                owner_id: 9,
                session_id: [9u8; 16],
                epoch: 1,
                indexed_finalized_head: 0,
                lease_valid_through_block: 500,
            });
            let authority = LeaseAuthority::with_session(store, 7, [7u8; 16], 100, 0);

            let lease = authority
                .acquire(Some(1_000))
                .await
                .expect("acquire publication");

            assert_eq!(lease.epoch, 2);
            assert_eq!(lease.indexed_finalized_head, 0);
        });
    }

    #[test]
    fn fresh_lease_rejects_takeover_until_expiry() {
        block_on(async {
            let store = InMemoryMetaStore::default();
            let first = LeaseAuthority::with_session(store.clone(), 7, [1u8; 16], 50, 0);
            let second = LeaseAuthority::with_session(store, 8, [2u8; 16], 50, 0);

            let _ = first
                .acquire(Some(100))
                .await
                .expect("bootstrap publication");
            let err = second
                .acquire(Some(120))
                .await
                .expect_err("fresh foreign lease should reject takeover");

            assert!(matches!(err, Error::LeaseStillFresh));
        });
    }

    #[test]
    fn same_owner_restart_after_expiry_bumps_epoch_and_session() {
        block_on(async {
            let store = InMemoryMetaStore::default();
            let first = LeaseAuthority::with_session(store.clone(), 7, [1u8; 16], 50, 0);
            let second = LeaseAuthority::with_session(store, 7, [2u8; 16], 50, 0);

            let first_token = first
                .acquire(Some(100))
                .await
                .expect("first acquire publication");
            let second_token = second
                .acquire(Some(151))
                .await
                .expect("same owner restart after expiry");

            assert!(second_token.epoch > first_token.epoch);
        });
    }

    #[test]
    fn same_owner_restart_before_expiry_bumps_epoch_and_session() {
        block_on(async {
            let store = InMemoryMetaStore::default();
            let first = LeaseAuthority::with_session(store.clone(), 7, [1u8; 16], 50, 0);
            let second = LeaseAuthority::with_session(store.clone(), 7, [2u8; 16], 50, 0);

            let first_token = first
                .acquire(Some(100))
                .await
                .expect("first acquire publication");
            let second_token = second
                .acquire(Some(120))
                .await
                .expect("same owner restart before expiry");

            assert!(second_token.epoch > first_token.epoch);

            let state = store
                .load()
                .await
                .expect("load")
                .expect("publication state");
            assert_eq!(state.owner_id, 7);
            assert_eq!(state.session_id, [2u8; 16]);
            assert_eq!(state.epoch, second_token.epoch);
        });
    }

    #[test]
    fn authorize_returns_lease_lost_after_external_takeover() {
        block_on(async {
            let store = InMemoryMetaStore::default();
            let authority = LeaseAuthority::with_session(store.clone(), 7, [1u8; 16], 50, 10);
            let token = authority.acquire(Some(100)).await.expect("acquire");
            let takeover = LeaseAuthority::with_session(store, 8, [2u8; 16], 50, 10);
            let _ = takeover.acquire(Some(151)).await.expect("takeover");

            let err = authority
                .authorize(&token, Some(151))
                .await
                .expect_err("authorize should observe takeover");

            assert!(matches!(err, Error::LeaseLost));
        });
    }

    #[test]
    fn publish_returns_lease_lost_on_epoch_mismatch() {
        block_on(async {
            let store = InMemoryMetaStore::default();
            let authority = LeaseAuthority::with_session(store.clone(), 7, [1u8; 16], 50, 0);
            let token = authority.acquire(Some(100)).await.expect("acquire");
            let takeover = LeaseAuthority::with_session(store, 8, [2u8; 16], 50, 0);
            let _ = takeover.acquire(Some(151)).await.expect("takeover");

            let err = authority
                .publish(&token, 1)
                .await
                .expect_err("publish should fail after takeover");

            assert!(matches!(err, Error::LeaseLost));
        });
    }

    #[test]
    fn publish_returns_publication_conflict_on_head_mismatch() {
        block_on(async {
            let store = InMemoryMetaStore::default();
            let authority = LeaseAuthority::with_session(store.clone(), 7, [1u8; 16], 50, 0);
            let token = authority.acquire(Some(100)).await.expect("acquire");
            let current = store.load().await.expect("load").expect("state");
            let next = PublicationState {
                indexed_finalized_head: 9,
                ..current.clone()
            };
            let _ = store
                .compare_and_set(&current, &next)
                .await
                .expect("mutate state");

            let err = authority
                .publish(&token, 1)
                .await
                .expect_err("publish should reject head mismatch");

            assert!(matches!(err, Error::PublicationConflict));
        });
    }

    #[test]
    fn same_session_acquire_after_expiry_bumps_epoch() {
        block_on(async {
            let store = InMemoryMetaStore::default();
            let authority = LeaseAuthority::with_session(store, 7, [1u8; 16], 50, 0);

            let first = authority.acquire(Some(100)).await.expect("first acquire");
            // valid_through = 100 + 49 = 149; observed 150 > 149 → expired
            let second = authority
                .acquire(Some(150))
                .await
                .expect("re-acquire after expiry");

            assert!(second.epoch > first.epoch);
        });
    }

    #[test]
    fn same_session_acquire_before_expiry_keeps_epoch() {
        block_on(async {
            let store = InMemoryMetaStore::default();
            let authority = LeaseAuthority::with_session(store, 7, [1u8; 16], 50, 0);

            let first = authority.acquire(Some(100)).await.expect("first acquire");
            // valid_through = 149; observed 140 <= 149 → still valid
            let second = authority
                .acquire(Some(140))
                .await
                .expect("re-acquire before expiry");

            assert_eq!(second.epoch, first.epoch);
        });
    }

    #[test]
    fn authorize_returns_lease_lost_after_own_expiry() {
        block_on(async {
            let store = InMemoryMetaStore::default();
            let authority = LeaseAuthority::with_session(store, 7, [1u8; 16], 50, 10);

            let token = authority.acquire(Some(100)).await.expect("acquire");
            // valid_through = 149; observed 150 > 149 → expired, no external takeover
            let err = authority
                .authorize(&token, Some(150))
                .await
                .expect_err("authorize should fail after own expiry");

            assert!(matches!(err, Error::LeaseLost));
        });
    }

    #[test]
    fn lease_blocks_grants_exact_n_blocks() {
        block_on(async {
            let store = InMemoryMetaStore::default();
            let first = LeaseAuthority::with_session(store.clone(), 7, [1u8; 16], 10, 0);
            let second = LeaseAuthority::with_session(store, 8, [2u8; 16], 10, 0);

            let _ = first.acquire(Some(100)).await.expect("bootstrap");
            // valid_through = 100 + 9 = 109; observed 109 <= 109 → still fresh
            let err = second
                .acquire(Some(109))
                .await
                .expect_err("should be still fresh at last valid block");
            assert!(matches!(err, Error::LeaseStillFresh));

            // observed 110 > 109 → expired, takeover allowed
            let token = second
                .acquire(Some(110))
                .await
                .expect("takeover at first expired block");
            assert_eq!(token.epoch, 2);
        });
    }

    #[test]
    fn acquire_fails_closed_without_observed_finalized_block() {
        block_on(async {
            let store = InMemoryMetaStore::default();
            let authority = LeaseAuthority::with_session(store, 7, [1u8; 16], 50, 0);

            let err = authority
                .acquire(None)
                .await
                .expect_err("missing observation should fail closed");

            assert!(matches!(err, Error::LeaseObservationUnavailable));
        });
    }
}
