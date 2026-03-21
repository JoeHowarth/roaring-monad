use crate::config::Config;
use crate::error::{Error, Result};
use crate::family::IngestFamily;
use crate::ingest::authority::{WriteAuthority, WriteSession};
use crate::runtime::Runtime;
use crate::store::traits::{BlobStore, MetaStore};

pub struct IngestEngine<A: WriteAuthority, F> {
    pub config: Config,
    pub authority: A,
    pub family: F,
}

impl<A, F> IngestEngine<A, F>
where
    A: WriteAuthority,
{
    pub fn new(config: Config, authority: A, family: F) -> Self {
        Self {
            config,
            authority,
            family,
        }
    }

    pub async fn ingest_finalized_block<M, B>(
        &self,
        runtime: &Runtime<M, B>,
        block: &F::Block,
    ) -> Result<F::Outcome>
    where
        M: MetaStore,
        B: BlobStore,
        F: IngestFamily<M, B>,
    {
        self.ingest_finalized_blocks(runtime, core::slice::from_ref(block))
            .await
    }

    pub async fn ingest_finalized_blocks<M, B>(
        &self,
        runtime: &Runtime<M, B>,
        blocks: &[F::Block],
    ) -> Result<F::Outcome>
    where
        M: MetaStore,
        B: BlobStore,
        F: IngestFamily<M, B>,
    {
        if blocks.is_empty() {
            return Err(Error::InvalidParams("ingest requires at least one block"));
        }

        let session = self
            .authority
            .begin_write(self.config.observe_upstream_finalized_block.as_ref()())
            .await?;
        let indexed_finalized_head = session.state().indexed_finalized_head;

        let outcome = self
            .family
            .ingest_finalized_blocks(
                &self.config,
                runtime.tables(),
                runtime.meta_store(),
                runtime.blob_store(),
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
