use crate::block::FinalizedBlock;
use crate::config::Config;
use crate::error::{Error, Result};
use crate::runtime::Runtime;
use crate::store::traits::{BlobStore, MetaStore};
use crate::traces::types::TraceStartupState;

#[derive(Debug, Clone, Copy, Default)]
pub struct TracesFamily;

impl TracesFamily {
    pub async fn load_startup_state<M: MetaStore, B: BlobStore>(
        &self,
        _runtime: &Runtime<M, B>,
        _indexed_finalized_head: u64,
    ) -> Result<TraceStartupState> {
        Ok(TraceStartupState)
    }

    pub async fn ingest_block<M: MetaStore, B: BlobStore>(
        &self,
        _config: &Config,
        _runtime: &Runtime<M, B>,
        _state: &mut TraceStartupState,
        block: &FinalizedBlock,
    ) -> Result<usize> {
        if !block.traces.is_empty() {
            return Err(Error::Unsupported("trace ingest not implemented"));
        }
        Ok(0)
    }
}
