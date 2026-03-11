use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::domain::types::{PublicationState, SessionId};
use crate::error::{Error, Result};
use crate::store::publication::{CasOutcome, FenceStore, PublicationStore};
use crate::store::traits::FenceToken;

pub const DEFAULT_LEASE_DURATION_MS: u64 = 30_000;

static NEXT_SESSION_NONCE: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublicationLease {
    pub owner_id: u64,
    pub session_id: SessionId,
    pub epoch: u64,
    pub indexed_finalized_head: u64,
    pub lease_expires_at_ms: u64,
}

impl From<PublicationState> for PublicationLease {
    fn from(value: PublicationState) -> Self {
        Self {
            owner_id: value.owner_id,
            session_id: value.session_id,
            epoch: value.epoch,
            indexed_finalized_head: value.indexed_finalized_head,
            lease_expires_at_ms: value.lease_expires_at_ms,
        }
    }
}

impl PublicationLease {
    pub fn as_state(self) -> PublicationState {
        PublicationState {
            owner_id: self.owner_id,
            session_id: self.session_id,
            epoch: self.epoch,
            indexed_finalized_head: self.indexed_finalized_head,
            lease_expires_at_ms: self.lease_expires_at_ms,
        }
    }

    pub fn fence_token(self) -> FenceToken {
        FenceToken(self.epoch)
    }

    pub fn needs_renewal(self, now_ms: u64, renew_skew_ms: u64) -> bool {
        self.lease_expires_at_ms
            .saturating_sub(now_ms)
            .le(&renew_skew_ms)
    }
}

pub fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

pub fn new_session_id(owner_id: u64) -> SessionId {
    let mut session_id = [0u8; 16];
    let now_ms = current_time_ms();
    let nonce = NEXT_SESSION_NONCE.fetch_add(1, Ordering::Relaxed);
    session_id[..8].copy_from_slice(&now_ms.to_be_bytes());
    session_id[8..].copy_from_slice(&(owner_id ^ nonce.rotate_left(17)).to_be_bytes());
    session_id
}

pub async fn bootstrap_publication_state<P: PublicationStore + FenceStore>(
    publication_store: &P,
    owner_id: u64,
    session_id: SessionId,
    now_ms: u64,
    lease_duration_ms: u64,
) -> Result<PublicationLease> {
    let initial = PublicationState {
        owner_id,
        session_id,
        epoch: 1,
        indexed_finalized_head: 0,
        lease_expires_at_ms: now_ms.saturating_add(lease_duration_ms),
    };
    match publication_store.create_if_absent(&initial).await? {
        CasOutcome::Applied(state) => {
            publication_store.advance_fence(state.epoch).await?;
            Ok(state.into())
        }
        CasOutcome::Failed { current: Some(_) } => Err(Error::PublicationConflict),
        CasOutcome::Failed { current: None } => Err(Error::PublicationConflict),
    }
}

pub async fn acquire_publication<P: PublicationStore + FenceStore>(
    publication_store: &P,
    owner_id: u64,
) -> Result<PublicationLease> {
    acquire_publication_with_session(
        publication_store,
        owner_id,
        new_session_id(owner_id),
        current_time_ms(),
        DEFAULT_LEASE_DURATION_MS,
    )
    .await
}

pub async fn acquire_publication_with_session<P: PublicationStore + FenceStore>(
    publication_store: &P,
    owner_id: u64,
    session_id: SessionId,
    now_ms: u64,
    lease_duration_ms: u64,
) -> Result<PublicationLease> {
    let mut current = match publication_store.load().await? {
        Some(state) => state,
        None => {
            let initial = PublicationState {
                owner_id,
                session_id,
                epoch: 1,
                indexed_finalized_head: 0,
                lease_expires_at_ms: now_ms.saturating_add(lease_duration_ms),
            };
            match publication_store.create_if_absent(&initial).await? {
                CasOutcome::Applied(state) => {
                    publication_store.advance_fence(state.epoch).await?;
                    return Ok(state.into());
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
        if current.owner_id == owner_id && current.session_id == session_id {
            let next = PublicationState {
                owner_id,
                session_id,
                epoch: current.epoch,
                indexed_finalized_head: current.indexed_finalized_head,
                lease_expires_at_ms: now_ms.saturating_add(lease_duration_ms),
            };
            if next.lease_expires_at_ms <= current.lease_expires_at_ms {
                return Ok(current.into());
            }

            match publication_store.compare_and_set(&current, &next).await? {
                CasOutcome::Applied(state) => return Ok(state.into()),
                CasOutcome::Failed {
                    current: Some(state),
                } => {
                    current = state;
                    continue;
                }
                CasOutcome::Failed { current: None } => {
                    return Err(Error::PublicationConflict);
                }
            }
        }

        if current.lease_expires_at_ms > now_ms {
            return Err(Error::LeaseStillFresh);
        }

        let next = PublicationState {
            owner_id,
            session_id,
            epoch: current.epoch.saturating_add(1),
            indexed_finalized_head: current.indexed_finalized_head,
            lease_expires_at_ms: now_ms.saturating_add(lease_duration_ms),
        };

        match publication_store.compare_and_set(&current, &next).await? {
            CasOutcome::Applied(state) => {
                publication_store.advance_fence(state.epoch).await?;
                return Ok(state.into());
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

pub async fn renew_publication_if_needed<P: PublicationStore>(
    publication_store: &P,
    lease: PublicationLease,
    now_ms: u64,
    renew_skew_ms: u64,
    lease_duration_ms: u64,
) -> Result<PublicationLease> {
    if !lease.needs_renewal(now_ms, renew_skew_ms) {
        return Ok(lease);
    }

    let mut current = publication_store.load().await?.ok_or(Error::LeaseLost)?;
    loop {
        if current.owner_id != lease.owner_id
            || current.session_id != lease.session_id
            || current.epoch != lease.epoch
        {
            return Err(Error::LeaseLost);
        }

        let next = PublicationState {
            owner_id: current.owner_id,
            session_id: current.session_id,
            epoch: current.epoch,
            indexed_finalized_head: current.indexed_finalized_head,
            lease_expires_at_ms: now_ms.saturating_add(lease_duration_ms),
        };
        if next.lease_expires_at_ms <= current.lease_expires_at_ms {
            return Ok(current.into());
        }

        match publication_store.compare_and_set(&current, &next).await? {
            CasOutcome::Applied(state) => return Ok(state.into()),
            CasOutcome::Failed {
                current: Some(state),
            } => current = state,
            CasOutcome::Failed { current: None } => return Err(Error::LeaseLost),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use futures::executor::block_on;

    use crate::domain::types::PublicationState;
    use crate::error::{Error, Result};
    use crate::store::publication::{CasOutcome, PublicationStore};

    use super::{acquire_publication_with_session, bootstrap_publication_state};

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

    impl crate::store::publication::FenceStore for BootstrapRaceStore {
        async fn advance_fence(&self, _min_epoch: u64) -> Result<()> {
            Ok(())
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
                lease_expires_at_ms: 500,
            });

            let lease = acquire_publication_with_session(&store, 7, [7u8; 16], 1_000, 100)
                .await
                .expect("acquire publication");

            assert_eq!(lease.owner_id, 7);
        });
    }

    #[test]
    fn fresh_lease_rejects_takeover_until_expiry() {
        block_on(async {
            let store = crate::store::meta::InMemoryMetaStore::default();

            let _first = bootstrap_publication_state(&store, 7, [1u8; 16], 100, 50)
                .await
                .expect("bootstrap publication");
            let err = acquire_publication_with_session(&store, 8, [2u8; 16], 120, 50)
                .await
                .expect_err("fresh foreign lease should reject takeover");

            assert!(matches!(err, Error::LeaseStillFresh));
        });
    }

    #[test]
    fn same_owner_restart_after_expiry_bumps_epoch_and_session() {
        block_on(async {
            let store = crate::store::meta::InMemoryMetaStore::default();

            let first = bootstrap_publication_state(&store, 7, [1u8; 16], 100, 50)
                .await
                .expect("first acquire publication");
            let second = acquire_publication_with_session(&store, 7, [2u8; 16], 151, 50)
                .await
                .expect("same owner restart after expiry");

            assert!(second.epoch > first.epoch);
            assert_ne!(second.session_id, first.session_id);
        });
    }
}
