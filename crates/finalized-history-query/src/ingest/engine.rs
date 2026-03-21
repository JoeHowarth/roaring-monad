use crate::config::Config;
use crate::error::{Error, Result};
use crate::family::IngestFamily;
use crate::ingest::authority::{WriteAuthority, WriteSession};
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;

pub struct IngestEngine<A: WriteAuthority, M: MetaStore, B: BlobStore, F> {
    pub config: Config,
    pub authority: A,
    pub meta_store: M,
    pub blob_store: B,
    pub family: F,
}

impl<A, M, B, F> IngestEngine<A, M, B, F>
where
    A: WriteAuthority,
    M: MetaStore + Clone,
    B: BlobStore + Clone,
    F: IngestFamily<M, B>,
{
    pub fn new(config: Config, authority: A, meta_store: M, blob_store: B, family: F) -> Self {
        Self {
            config,
            authority,
            meta_store,
            blob_store,
            family,
        }
    }

    pub async fn ingest_finalized_block(&self, block: &F::Block) -> Result<F::Outcome> {
        self.ingest_finalized_blocks(core::slice::from_ref(block))
            .await
    }

    pub async fn ingest_finalized_blocks(&self, blocks: &[F::Block]) -> Result<F::Outcome> {
        if blocks.is_empty() {
            return Err(Error::InvalidParams("ingest requires at least one block"));
        }

        let session = self
            .authority
            .begin_write(self.config.observe_upstream_finalized_block.as_ref()())
            .await?;
        let indexed_finalized_head = session.state().indexed_finalized_head;

        let tables = Tables::without_cache(
            std::sync::Arc::new(self.meta_store.clone()),
            std::sync::Arc::new(self.blob_store.clone()),
        );
        let outcome = self
            .family
            .ingest_finalized_blocks(
                &self.config,
                &tables,
                &self.meta_store,
                &self.blob_store,
                indexed_finalized_head,
                blocks,
            )
            .await?;

        session
            .publish(
                self.family.indexed_finalized_head(blocks),
                self.config.observe_upstream_finalized_block.as_ref()(),
            )
            .await?;

        Ok(outcome)
    }
}
