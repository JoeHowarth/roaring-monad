use crate::error::Result;

pub mod lease;
pub mod read_only;

pub use lease::LeaseAuthority;
pub use read_only::ReadOnlyAuthority;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthorityState {
    pub indexed_finalized_head: u64,
}

#[allow(async_fn_in_trait)]
pub trait WriteAuthority: Send + Sync {
    async fn ensure_writer(
        &self,
        observed_upstream_finalized_block: Option<u64>,
    ) -> Result<AuthorityState>;

    async fn publish(&self, new_head: u64) -> Result<()>;

    async fn clear(&self);
}
