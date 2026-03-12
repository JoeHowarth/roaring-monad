use crate::codec::finalized_state::encode_publication_state;
use crate::domain::keys::PUBLICATION_STATE_KEY;
use crate::domain::types::{PublicationState, SessionId};
use crate::error::{Error, Result};
use crate::ingest::authority::{WriteAuthority, WriteToken};
use crate::store::publication::{CasOutcome, FenceStore, PublicationStore};
use crate::store::traits::{FenceToken, MetaStore, PutCond};

const SINGLE_WRITER_OWNER_ID: u64 = 0;
const SINGLE_WRITER_SESSION_ID: SessionId = *b"single-writer-v1";

#[derive(Debug)]
pub struct SingleWriterAuthority<P> {
    publication_store: P,
    token: futures::lock::Mutex<Option<WriteToken>>,
}

impl<P> SingleWriterAuthority<P> {
    pub fn new(publication_store: P) -> Self {
        Self {
            publication_store,
            token: futures::lock::Mutex::new(None),
        }
    }
}

impl<P: MetaStore + PublicationStore + FenceStore> SingleWriterAuthority<P> {
    fn sentinel_state(epoch: u64, indexed_finalized_head: u64) -> PublicationState {
        PublicationState {
            owner_id: SINGLE_WRITER_OWNER_ID,
            session_id: SINGLE_WRITER_SESSION_ID,
            epoch,
            indexed_finalized_head,
            lease_expires_at_ms: u64::MAX,
        }
    }

    fn is_sentinel_state(state: &PublicationState) -> bool {
        state.owner_id == SINGLE_WRITER_OWNER_ID
            && state.session_id == SINGLE_WRITER_SESSION_ID
            && state.lease_expires_at_ms == u64::MAX
    }

    fn mode_conflict() -> Error {
        Error::ModeConflict("single-writer authority cannot take over lease-managed state")
    }
}

impl<P: MetaStore + PublicationStore + FenceStore> WriteAuthority for SingleWriterAuthority<P> {
    async fn authorize(&self, current: &WriteToken, _now_ms: u64) -> Result<WriteToken> {
        let guard = self.token.lock().await;
        match *guard {
            Some(token) if token == *current => Ok(token),
            _ => Err(Error::PublicationConflict),
        }
    }

    async fn publish(&self, current: &WriteToken, new_head: u64) -> Result<WriteToken> {
        let mut guard = self.token.lock().await;
        let token = (*guard).ok_or(Error::PublicationConflict)?;
        if token != *current {
            return Err(Error::PublicationConflict);
        }

        let current_state = self
            .publication_store
            .load()
            .await?
            .ok_or_else(Self::mode_conflict)?;
        if !Self::is_sentinel_state(&current_state) {
            return Err(Self::mode_conflict());
        }
        if current_state.epoch != current.epoch
            || current_state.indexed_finalized_head != current.indexed_finalized_head
        {
            return Err(Error::PublicationConflict);
        }
        if new_head <= current_state.indexed_finalized_head {
            return Err(Error::PublicationConflict);
        }

        let next = Self::sentinel_state(current.epoch, new_head);
        let result = self
            .publication_store
            .put(
                PUBLICATION_STATE_KEY,
                encode_publication_state(&next),
                PutCond::Any,
                FenceToken(current.epoch),
            )
            .await?;
        if !result.applied {
            return Err(Error::PublicationConflict);
        }

        let next_token = WriteToken {
            epoch: current.epoch,
            indexed_finalized_head: new_head,
        };
        *guard = Some(next_token);
        Ok(next_token)
    }

    fn fence(&self, token: &WriteToken) -> FenceToken {
        FenceToken(token.epoch)
    }

    async fn acquire(&self, _now_ms: u64) -> Result<WriteToken> {
        loop {
            let current_fence = self.publication_store.current_fence().await?.max(1);
            match self.publication_store.load().await? {
                None => {
                    let initial = Self::sentinel_state(current_fence, 0);
                    match self.publication_store.create_if_absent(&initial).await? {
                        CasOutcome::Applied(state) => {
                            self.publication_store.advance_fence(state.epoch).await?;
                            let token = WriteToken {
                                epoch: state.epoch,
                                indexed_finalized_head: state.indexed_finalized_head,
                            };
                            *self.token.lock().await = Some(token);
                            return Ok(token);
                        }
                        CasOutcome::Failed { current: Some(_) } => continue,
                        CasOutcome::Failed { current: None } => {
                            return Err(Error::PublicationConflict);
                        }
                    }
                }
                Some(state) => {
                    if !Self::is_sentinel_state(&state) {
                        return Err(Self::mode_conflict());
                    }

                    let epoch = state.epoch.max(current_fence);
                    if state.epoch != epoch {
                        let next = Self::sentinel_state(epoch, state.indexed_finalized_head);
                        match self
                            .publication_store
                            .compare_and_set(&state, &next)
                            .await?
                        {
                            CasOutcome::Applied(state) => {
                                self.publication_store.advance_fence(state.epoch).await?;
                                let token = WriteToken {
                                    epoch: state.epoch,
                                    indexed_finalized_head: state.indexed_finalized_head,
                                };
                                *self.token.lock().await = Some(token);
                                return Ok(token);
                            }
                            CasOutcome::Failed { current: Some(_) } => continue,
                            CasOutcome::Failed { current: None } => {
                                return Err(Error::PublicationConflict);
                            }
                        }
                    }

                    self.publication_store.advance_fence(epoch).await?;
                    let token = WriteToken {
                        epoch,
                        indexed_finalized_head: state.indexed_finalized_head,
                    };
                    *self.token.lock().await = Some(token);
                    return Ok(token);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;

    use crate::codec::finalized_state::decode_publication_state;
    use crate::core::state::load_finalized_head_state;
    use crate::domain::keys::PUBLICATION_STATE_KEY;
    use crate::domain::types::PublicationState;
    use crate::error::Error;
    use crate::ingest::authority::lease::LeaseAuthority;
    use crate::ingest::authority::{SingleWriterAuthority, WriteAuthority};
    use crate::store::meta::InMemoryMetaStore;
    use crate::store::publication::PublicationStore;
    use crate::store::traits::MetaStore;

    #[test]
    fn acquire_returns_head_zero_on_empty_store() {
        block_on(async {
            let store = InMemoryMetaStore::default();
            let authority = SingleWriterAuthority::new(store.clone());

            let token = authority.acquire(0).await.expect("acquire");

            assert_eq!(token.epoch, 1);
            assert_eq!(token.indexed_finalized_head, 0);
        });
    }

    #[test]
    fn acquire_returns_current_head_on_single_writer_state() {
        block_on(async {
            let store = InMemoryMetaStore::default();
            let authority = SingleWriterAuthority::new(store.clone());
            let first = authority.acquire(0).await.expect("first acquire");
            let _ = authority.publish(&first, 7).await.expect("publish");
            let authority = SingleWriterAuthority::new(store);

            let token = authority.acquire(0).await.expect("reacquire");

            assert_eq!(token.indexed_finalized_head, 7);
        });
    }

    #[test]
    fn acquire_returns_mode_conflict_on_lease_owned_state() {
        block_on(async {
            let store = InMemoryMetaStore::default();
            let lease = LeaseAuthority::with_session(store.clone(), 7, [1u8; 16], 50, 0);
            let _ = lease.acquire(100).await.expect("lease acquire");
            let authority = SingleWriterAuthority::new(store);

            let err = authority
                .acquire(0)
                .await
                .expect_err("single writer should reject lease-managed state");

            assert!(matches!(err, Error::ModeConflict(_)));
        });
    }

    #[test]
    fn acquire_after_prior_lease_run_picks_up_the_higher_fence_epoch() {
        block_on(async {
            let store = InMemoryMetaStore::default();
            let first = LeaseAuthority::with_session(store.clone(), 7, [1u8; 16], 50, 0);
            let _ = first.acquire(100).await.expect("first lease acquire");
            let second = LeaseAuthority::with_session(store.clone(), 7, [2u8; 16], 50, 0);
            let second = second.acquire(151).await.expect("takeover");
            let state = store.load().await.expect("load").expect("state");
            store
                .compare_and_set(
                    &state,
                    &PublicationState {
                        owner_id: 0,
                        session_id: *b"single-writer-v1",
                        epoch: 1,
                        indexed_finalized_head: second.indexed_finalized_head,
                        lease_expires_at_ms: u64::MAX,
                    },
                )
                .await
                .expect("seed sentinel");
            let authority = SingleWriterAuthority::new(store);

            let token = authority.acquire(0).await.expect("single writer acquire");

            assert_eq!(token.epoch, 2);
        });
    }

    #[test]
    fn publish_advances_head_and_rejects_regression() {
        block_on(async {
            let store = InMemoryMetaStore::default();
            let authority = SingleWriterAuthority::new(store);
            let token = authority.acquire(0).await.expect("acquire");
            let next = authority.publish(&token, 3).await.expect("publish");

            assert_eq!(next.indexed_finalized_head, 3);
            let err = authority
                .publish(&next, 3)
                .await
                .expect_err("publish should reject regression");
            assert!(matches!(err, Error::PublicationConflict));
        });
    }

    #[test]
    fn fence_returns_the_acquired_epoch() {
        block_on(async {
            let store = InMemoryMetaStore::default();
            let authority = SingleWriterAuthority::new(store);
            let token = authority.acquire(0).await.expect("acquire");

            assert_eq!(authority.fence(&token).0, token.epoch);
        });
    }

    #[test]
    fn readers_can_load_the_head_written_by_single_writer() {
        block_on(async {
            let store = InMemoryMetaStore::default();
            let authority = SingleWriterAuthority::new(store.clone());
            let token = authority.acquire(0).await.expect("acquire");
            let _ = authority.publish(&token, 5).await.expect("publish");

            let head = load_finalized_head_state(&store)
                .await
                .expect("load finalized head state");
            let record = store
                .get(PUBLICATION_STATE_KEY)
                .await
                .expect("publication state get")
                .expect("publication state");
            let state = decode_publication_state(&record.value).expect("decode publication state");

            assert_eq!(head.indexed_finalized_head, 5);
            assert_eq!(state.owner_id, 0);
            assert_eq!(state.session_id, *b"single-writer-v1");
        });
    }
}
