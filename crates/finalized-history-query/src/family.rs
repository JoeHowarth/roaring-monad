use crate::config::Config;
use crate::error::Result;
use crate::runtime::Runtime;
use crate::store::publication::{FinalizedHeadState, PublicationStore};
use crate::store::traits::{BlobStore, MetaStore};

#[derive(Debug, Clone)]
pub struct StartupState<S> {
    pub head_state: FinalizedHeadState,
    pub family_state: S,
    pub warm_streams: usize,
}

pub trait FinalizedBlock {
    fn block_num(&self) -> u64;
    fn block_hash(&self) -> &[u8; 32];
    fn parent_hash(&self) -> &[u8; 32];
}

impl FinalizedBlock for crate::block::Block {
    fn block_num(&self) -> u64 {
        self.block_num
    }

    fn block_hash(&self) -> &[u8; 32] {
        &self.block_hash
    }

    fn parent_hash(&self) -> &[u8; 32] {
        &self.parent_hash
    }
}

#[allow(async_fn_in_trait)]
pub trait BlockFamily<M: MetaStore, B: BlobStore>: Send + Sync {
    type Block: FinalizedBlock;
    type State;
    type Outcome;

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
        block: &Self::Block,
    ) -> Result<()>;

    fn finish_outcome(
        &self,
        indexed_finalized_head: u64,
        blocks: &[Self::Block],
        state: &Self::State,
    ) -> Self::Outcome;
}

pub async fn startup_state<M, P, B, F>(
    runtime: &Runtime<M, B>,
    publication_store: &P,
    family: &F,
    warm_streams: usize,
) -> Result<StartupState<F::State>>
where
    M: MetaStore,
    P: PublicationStore,
    B: BlobStore,
    F: BlockFamily<M, B>,
{
    let head_state = publication_store.load_finalized_head_state().await?;
    let family_state = family
        .load_startup_state(runtime, head_state.indexed_finalized_head)
        .await?;
    Ok(StartupState {
        head_state,
        family_state,
        warm_streams,
    })
}
