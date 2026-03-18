use rand::random;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::domain::types::SessionId;

mod acquisition;
mod lifecycle;

#[cfg(test)]
mod tests;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PublicationLease {
    owner_id: u64,
    session_id: SessionId,
    epoch: u64,
    indexed_finalized_head: u64,
    lease_valid_through_block: u64,
}

impl From<crate::domain::types::PublicationState> for PublicationLease {
    fn from(value: crate::domain::types::PublicationState) -> Self {
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
    fn as_state(self) -> crate::domain::types::PublicationState {
        crate::domain::types::PublicationState {
            owner_id: self.owner_id,
            session_id: self.session_id,
            epoch: self.epoch,
            indexed_finalized_head: self.indexed_finalized_head,
            lease_valid_through_block: self.lease_valid_through_block,
        }
    }

    fn as_token(self) -> crate::ingest::authority::WriteToken {
        crate::ingest::authority::WriteToken {
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

fn require_observed_finalized_block(
    observed_upstream_finalized_block: Option<u64>,
) -> crate::error::Result<u64> {
    observed_upstream_finalized_block.ok_or(crate::error::Error::LeaseObservationUnavailable)
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
