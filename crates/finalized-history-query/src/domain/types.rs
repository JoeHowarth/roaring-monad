use serde::{Deserialize, Serialize};

use crate::store::publication::FinalizedHeadState;

pub type SessionId = [u8; 16];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct PublicationState {
    pub owner_id: u64,
    pub session_id: SessionId,
    pub indexed_finalized_head: u64,
    pub lease_valid_through_block: u64,
}

impl PublicationState {
    pub fn finalized_head_state(&self) -> FinalizedHeadState {
        FinalizedHeadState {
            indexed_finalized_head: self.indexed_finalized_head,
        }
    }
}
