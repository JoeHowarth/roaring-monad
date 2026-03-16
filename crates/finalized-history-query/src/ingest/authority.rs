use crate::error::Result;
use crate::store::traits::FenceToken;

pub mod lease;
pub mod read_only;
pub mod single_writer;

pub use lease::LeaseAuthority;
pub use read_only::ReadOnlyAuthority;
pub use single_writer::SingleWriterAuthority;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteToken {
    pub epoch: u64,
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

    fn fence(&self, token: &WriteToken) -> FenceToken;

    async fn acquire(&self, observed_upstream_finalized_block: Option<u64>) -> Result<WriteToken>;
}
