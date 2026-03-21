use crate::block::FinalizedBlock;
use crate::config::Config;
use crate::error::Result;
use crate::logs::family::LogsFamily;
use crate::logs::types::LogSequencingState;
use crate::runtime::Runtime;
use crate::store::publication::{FinalizedHeadState, PublicationStore};
use crate::store::traits::{BlobStore, MetaStore};
use crate::traces::family::TracesFamily;
use crate::traces::types::TraceStartupState;
use crate::txs::family::TxsFamily;
use crate::txs::types::TxStartupState;

#[derive(Debug, Clone)]
pub struct StartupState {
    pub head_state: FinalizedHeadState,
    pub family_states: FamilyStates,
    pub warm_streams: usize,
}

#[derive(Debug, Clone)]
pub struct FamilyStates {
    pub logs: LogSequencingState,
    pub txs: TxStartupState,
    pub traces: TraceStartupState,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FamilyBlockWrites {
    pub logs: usize,
    pub txs: usize,
    pub traces: usize,
}

impl core::ops::AddAssign for FamilyBlockWrites {
    fn add_assign(&mut self, rhs: Self) {
        self.logs = self.logs.saturating_add(rhs.logs);
        self.txs = self.txs.saturating_add(rhs.txs);
        self.traces = self.traces.saturating_add(rhs.traces);
    }
}

#[allow(async_fn_in_trait)]
pub trait Family<M: MetaStore, B: BlobStore>: Send + Sync {
    type State;

    async fn load_startup_state(
        &self,
        runtime: &Runtime<M, B>,
        indexed_finalized_head: u64,
    ) -> Result<Self::State>;

    async fn ingest_block(
        &self,
        config: &Config,
        runtime: &Runtime<M, B>,
        state: &mut Self::State,
        block: &FinalizedBlock,
    ) -> Result<usize>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Families {
    pub logs: LogsFamily,
    pub txs: TxsFamily,
    pub traces: TracesFamily,
}

impl Families {
    pub async fn load_startup_state<M, B>(
        &self,
        runtime: &Runtime<M, B>,
        indexed_finalized_head: u64,
    ) -> Result<FamilyStates>
    where
        M: MetaStore,
        B: BlobStore,
    {
        Ok(FamilyStates {
            logs: self
                .logs
                .load_startup_state(runtime, indexed_finalized_head)
                .await?,
            txs: self
                .txs
                .load_startup_state(runtime, indexed_finalized_head)
                .await?,
            traces: self
                .traces
                .load_startup_state(runtime, indexed_finalized_head)
                .await?,
        })
    }

    pub async fn ingest_block<M, B>(
        &self,
        config: &Config,
        runtime: &Runtime<M, B>,
        states: &mut FamilyStates,
        block: &FinalizedBlock,
    ) -> Result<FamilyBlockWrites>
    where
        M: MetaStore,
        B: BlobStore,
    {
        Ok(FamilyBlockWrites {
            logs: self
                .logs
                .ingest_block(config, runtime, &mut states.logs, block)
                .await?,
            txs: self
                .txs
                .ingest_block(config, runtime, &mut states.txs, block)
                .await?,
            traces: self
                .traces
                .ingest_block(config, runtime, &mut states.traces, block)
                .await?,
        })
    }
}

pub async fn startup_state<M, P, B>(
    runtime: &Runtime<M, B>,
    publication_store: &P,
    families: &Families,
    warm_streams: usize,
) -> Result<StartupState>
where
    M: MetaStore,
    P: PublicationStore,
    B: BlobStore,
{
    let head_state = publication_store.load_finalized_head_state().await?;
    let family_states = families
        .load_startup_state(runtime, head_state.indexed_finalized_head)
        .await?;
    Ok(StartupState {
        head_state,
        family_states,
        warm_streams,
    })
}
