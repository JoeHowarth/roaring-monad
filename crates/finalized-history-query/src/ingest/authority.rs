use crate::domain::types::SessionId;
use crate::error::Result;

pub mod lease;
pub mod read_only;

pub use lease::LeaseAuthority;
pub use read_only::ReadOnlyAuthority;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteToken {
    pub session_id: SessionId,
    pub indexed_finalized_head: u64,
}

#[allow(async_fn_in_trait)]
pub trait WriteAuthority: Send + Sync {
    async fn authorize(
        &self,
        current: &WriteToken,
        observed_upstream_finalized_block: Option<u64>,
    ) -> Result<WriteToken>;

    async fn publish(&self, current: &WriteToken, new_head: u64) -> Result<WriteToken>;

    async fn acquire(&self, observed_upstream_finalized_block: Option<u64>) -> Result<WriteToken>;
}
