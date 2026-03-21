use std::sync::Mutex;

use futures::executor::block_on;

use crate::error::{Error, Result};
use crate::ingest::authority::{WriteAuthority, WriteSession};
use crate::store::meta::InMemoryMetaStore;
use crate::store::publication::PublicationState;
use crate::store::publication::{CasOutcome, MetaPublicationStore, PublicationStore};

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
        let first = LeaseAuthority::with_session(publication_store(&store), 7, [1u8; 16], 50, 0);
        let second = LeaseAuthority::with_session(publication_store(&store), 8, [2u8; 16], 50, 0);

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
        let first = LeaseAuthority::with_session(publication_store(&store), 7, [1u8; 16], 50, 0);
        let second = LeaseAuthority::with_session(publication_store(&store), 7, [2u8; 16], 50, 0);

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
        let first = LeaseAuthority::with_session(publication_store(&store), 7, [1u8; 16], 50, 0);
        let second = LeaseAuthority::with_session(publication_store(&store), 7, [2u8; 16], 50, 0);

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
        let takeover = LeaseAuthority::with_session(publication_store(&store), 8, [2u8; 16], 50, 0);
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
        let publication_store = publication_store(&store);
        let current = publication_store
            .load()
            .await
            .expect("load")
            .expect("state");
        let next = PublicationState {
            indexed_finalized_head: 9,
            ..current.clone()
        };
        let _ = publication_store
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

        authority
            .begin_write(Some(100))
            .await
            .expect("first acquire");
        let first_session = authority
            .publication_store
            .load()
            .await
            .expect("load")
            .expect("state")
            .session_id;
        // valid_through = 100 + 49 = 149; observed 150 > 149 -> expired
        authority
            .begin_write(Some(150))
            .await
            .expect("reacquire after expiry");
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

        authority
            .begin_write(Some(100))
            .await
            .expect("first acquire");
        let first_session = authority
            .publication_store
            .load()
            .await
            .expect("load")
            .expect("state")
            .session_id;
        // valid_through = 149; observed 140 <= 149 -> still valid
        authority
            .begin_write(Some(140))
            .await
            .expect("reuse before expiry");
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

        authority.begin_write(Some(100)).await.expect("acquire");
        let first_session = authority
            .publication_store
            .load()
            .await
            .expect("load")
            .expect("state")
            .session_id;
        // valid_through = 149; observed 150 > 149 -> expired, no external takeover
        authority
            .begin_write(Some(150))
            .await
            .expect("begin_write should reacquire after own expiry");
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
        let first = LeaseAuthority::with_session(publication_store(&store), 7, [1u8; 16], 10, 0);
        let second = LeaseAuthority::with_session(publication_store(&store), 8, [2u8; 16], 10, 0);

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
