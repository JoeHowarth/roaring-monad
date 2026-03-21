use crate::error::Result;
use std::marker::PhantomData;

pub mod lease;

pub use lease::LeaseAuthority;

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

#[derive(Debug, Default, Clone, Copy)]
pub struct ReadOnlyAuthority;

pub struct ReadOnlyWriteSession<'a> {
    _marker: PhantomData<&'a ReadOnlyAuthority>,
}

impl ReadOnlyAuthority {
    fn writer_mode_error() -> crate::error::Error {
        crate::error::Error::ReadOnlyMode("reader-only service cannot acquire write authority")
    }
}

impl WriteSession for ReadOnlyWriteSession<'_> {
    fn state(&self) -> AuthorityState {
        unreachable!("reader-only authority never yields a write session")
    }

    async fn publish(
        self,
        _new_head: u64,
        _observed_upstream_finalized_block: Option<u64>,
    ) -> Result<()> {
        unreachable!("reader-only authority never yields a write session")
    }
}

impl WriteAuthority for ReadOnlyAuthority {
    type Session<'a>
        = ReadOnlyWriteSession<'a>
    where
        Self: 'a;

    async fn begin_write(
        &self,
        _observed_upstream_finalized_block: Option<u64>,
    ) -> Result<Self::Session<'_>> {
        Err(Self::writer_mode_error())
    }
}
