use crate::config::Config;
use crate::error::Result;
use crate::store::publication::{FinalizedHeadState, PublicationStore};
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;

#[derive(Debug, Clone)]
pub struct StartupState<S> {
    pub head_state: FinalizedHeadState,
    pub family_state: S,
    pub warm_streams: usize,
}

#[allow(async_fn_in_trait)]
pub trait StartupFamily<M: MetaStore, B: BlobStore>: Send + Sync {
    type State;

    async fn load_startup_state(
        &self,
        tables: &Tables<M, B>,
        indexed_finalized_head: u64,
    ) -> Result<Self::State>;
}

#[allow(async_fn_in_trait)]
pub trait IngestFamily<M: MetaStore, B: BlobStore>: Send + Sync {
    type Block;
    type Outcome;

    fn indexed_finalized_head(&self, blocks: &[Self::Block]) -> u64;

    async fn ingest_finalized_blocks(
        &self,
        config: &Config,
        tables: &Tables<M, B>,
        meta_store: &M,
        blob_store: &B,
        indexed_finalized_head: u64,
        blocks: &[Self::Block],
    ) -> Result<Self::Outcome>;
}

pub async fn startup_state<M, P, B, F>(
    tables: &Tables<M, B>,
    publication_store: &P,
    family: &F,
    warm_streams: usize,
) -> Result<StartupState<F::State>>
where
    M: MetaStore,
    P: PublicationStore,
    B: BlobStore,
    F: StartupFamily<M, B>,
{
    let head_state = publication_store.load_finalized_head_state().await?;
    let family_state = family
        .load_startup_state(tables, head_state.indexed_finalized_head)
        .await?;
    Ok(StartupState {
        head_state,
        family_state,
        warm_streams,
    })
}
