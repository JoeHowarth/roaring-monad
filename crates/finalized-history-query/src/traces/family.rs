use crate::block::FinalizedBlock;
use crate::config::Config;
use crate::error::{Error, Result};
use crate::family::Family;
use crate::runtime::Runtime;
use crate::store::traits::{BlobStore, MetaStore};
use crate::traces::types::TraceStartupState;

#[derive(Debug, Clone, Copy, Default)]
pub struct TracesFamily;

impl<M: MetaStore, B: BlobStore> Family<M, B> for TracesFamily {
    type State = TraceStartupState;

    async fn load_startup_state(
        &self,
        _runtime: &Runtime<M, B>,
        _indexed_finalized_head: u64,
    ) -> Result<Self::State> {
        Ok(TraceStartupState)
    }

    async fn ingest_block(
        &self,
        _config: &Config,
        _runtime: &Runtime<M, B>,
        _state: &mut Self::State,
        block: &FinalizedBlock,
    ) -> Result<usize> {
        if !block.traces.is_empty() {
            return Err(Error::Unsupported("trace ingest not implemented"));
        }
        Ok(0)
    }
}
