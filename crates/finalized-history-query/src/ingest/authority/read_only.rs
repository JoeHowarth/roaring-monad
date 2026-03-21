use crate::error::{Error, Result};
use crate::ingest::authority::WriteAuthority;

#[derive(Debug, Default, Clone, Copy)]
pub struct ReadOnlyAuthority;

impl ReadOnlyAuthority {
    fn writer_mode_error() -> Error {
        Error::ReadOnlyMode("reader-only service cannot acquire write authority")
    }
}

impl WriteAuthority for ReadOnlyAuthority {
    async fn authorize(&self, _observed_upstream_finalized_block: Option<u64>) -> Result<u64> {
        Err(Self::writer_mode_error())
    }

    async fn publish(&self, _new_head: u64) -> Result<()> {
        Err(Self::writer_mode_error())
    }

    async fn acquire(&self, _observed_upstream_finalized_block: Option<u64>) -> Result<u64> {
        Err(Self::writer_mode_error())
    }
}
