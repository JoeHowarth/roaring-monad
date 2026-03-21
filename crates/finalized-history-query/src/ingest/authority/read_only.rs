use std::marker::PhantomData;

use crate::error::{Error, Result};
use crate::ingest::authority::{AuthorityState, WriteAuthority, WriteSession};

#[derive(Debug, Default, Clone, Copy)]
pub struct ReadOnlyAuthority;

pub struct ReadOnlyWriteSession<'a> {
    _marker: PhantomData<&'a ReadOnlyAuthority>,
}

impl ReadOnlyAuthority {
    fn writer_mode_error() -> Error {
        Error::ReadOnlyMode("reader-only service cannot acquire write authority")
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
