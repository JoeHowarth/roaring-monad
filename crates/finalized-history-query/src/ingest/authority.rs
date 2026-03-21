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
pub trait WriteSession: Send {
    fn state(&self) -> AuthorityState;

    async fn publish(
        self,
        new_head: u64,
        observed_upstream_finalized_block: Option<u64>,
    ) -> Result<()>;
}

#[allow(async_fn_in_trait)]
pub trait WriteAuthority: Send + Sync {
    type Session<'a>: WriteSession + 'a
    where
        Self: 'a;

    async fn begin_write(
        &self,
        observed_upstream_finalized_block: Option<u64>,
    ) -> Result<Self::Session<'_>>;
}
