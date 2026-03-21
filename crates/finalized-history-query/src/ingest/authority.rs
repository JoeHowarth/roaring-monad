use crate::error::Result;

pub mod lease;
pub mod read_only;

pub use lease::LeaseAuthority;
pub use read_only::ReadOnlyAuthority;

#[allow(async_fn_in_trait)]
pub trait WriteAuthority: Send + Sync {
    async fn authorize(&self, observed_upstream_finalized_block: Option<u64>) -> Result<u64>;

    async fn publish(&self, new_head: u64) -> Result<()>;

    async fn acquire(&self, observed_upstream_finalized_block: Option<u64>) -> Result<u64>;
}
