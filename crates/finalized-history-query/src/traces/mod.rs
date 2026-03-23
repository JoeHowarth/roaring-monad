pub mod codec;
pub mod keys;
pub mod state;
pub mod table_specs;
pub mod types;

use crate::config::Config;
use crate::core::ids::TraceId;
use crate::error::{Error, Result};
use crate::family::FinalizedBlock;
use crate::runtime::Runtime;
use crate::store::traits::{BlobStore, MetaStore};

pub use types::{Trace, TraceSequencingState, TraceStartupState};

#[derive(Debug, Clone, Copy, Default)]
pub struct TracesFamily;

impl TracesFamily {
    pub async fn load_startup_state<M: MetaStore, B: BlobStore>(
        &self,
        runtime: &Runtime<M, B>,
        indexed_finalized_head: u64,
    ) -> Result<TraceStartupState> {
        let next_trace_id = state::derive_next_trace_id(runtime.tables(), indexed_finalized_head)
            .await
            .or_else(|err| {
                if indexed_finalized_head == 0 && matches!(err, Error::NotFound) {
                    Ok(0)
                } else {
                    Err(err)
                }
            })?;
        Ok(TraceSequencingState {
            next_trace_id: TraceId::new(next_trace_id),
        })
    }

    pub async fn ingest_block<M: MetaStore, B: BlobStore>(
        &self,
        _config: &Config,
        _runtime: &Runtime<M, B>,
        _state: &mut TraceStartupState,
        block: &FinalizedBlock,
    ) -> Result<usize> {
        if !block.trace_rlp.is_empty() {
            return Err(Error::Unsupported("trace ingest not implemented"));
        }
        Ok(0)
    }
}
