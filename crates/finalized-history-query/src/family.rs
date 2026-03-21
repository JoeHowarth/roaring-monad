use crate::block::FinalizedBlock;
use crate::config::Config;
use crate::error::Result;
use crate::runtime::Runtime;
use crate::store::publication::{FinalizedHeadState, PublicationStore};
use crate::store::traits::{BlobStore, MetaStore};

#[derive(Debug, Clone)]
pub struct StartupState<L, T, R> {
    pub head_state: FinalizedHeadState,
    pub family_states: FamilyStates<L, T, R>,
    pub warm_streams: usize,
}

#[derive(Debug, Clone)]
pub struct FamilyStates<L, T, R> {
    pub logs: L,
    pub txs: T,
    pub traces: R,
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

#[derive(Debug, Clone, Copy)]
pub struct Families<L, T, R> {
    pub logs: L,
    pub txs: T,
    pub traces: R,
}

impl<L, T, R> Families<L, T, R> {
    pub const fn new(logs: L, txs: T, traces: R) -> Self {
        Self { logs, txs, traces }
    }
}

impl Default
    for Families<
        crate::logs::family::LogsFamily,
        crate::txs::family::TxsFamily,
        crate::traces::family::TracesFamily,
    >
{
    fn default() -> Self {
        Self::new(
            crate::logs::family::LogsFamily,
            crate::txs::family::TxsFamily,
            crate::traces::family::TracesFamily,
        )
    }
}

impl<L, T, R> Families<L, T, R> {
    pub async fn load_startup_state<M, B>(
        &self,
        runtime: &Runtime<M, B>,
        indexed_finalized_head: u64,
    ) -> Result<FamilyStates<L::State, T::State, R::State>>
    where
        M: MetaStore,
        B: BlobStore,
        L: Family<M, B>,
        T: Family<M, B>,
        R: Family<M, B>,
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
        states: &mut FamilyStates<L::State, T::State, R::State>,
        block: &FinalizedBlock,
    ) -> Result<FamilyBlockWrites>
    where
        M: MetaStore,
        B: BlobStore,
        L: Family<M, B>,
        T: Family<M, B>,
        R: Family<M, B>,
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

pub async fn startup_state<M, P, B, L, T, R>(
    runtime: &Runtime<M, B>,
    publication_store: &P,
    families: &Families<L, T, R>,
    warm_streams: usize,
) -> Result<StartupState<L::State, T::State, R::State>>
where
    M: MetaStore,
    P: PublicationStore,
    B: BlobStore,
    L: Family<M, B>,
    T: Family<M, B>,
    R: Family<M, B>,
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
