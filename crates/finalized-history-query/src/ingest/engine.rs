use crate::config::Config;
use crate::core::state::load_block_identity;
use crate::error::{Error, Result};
use crate::family::{BlockFamily, FinalizedBlock};
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
        F: BlockFamily<M, B>,
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
        F: BlockFamily<M, B>,
    {
        if blocks.is_empty() {
            return Err(Error::InvalidParams("ingest requires at least one block"));
        }

        let session = self
            .authority
            .begin_write(self.config.observe_upstream_finalized_block.as_ref()())
            .await?;
        let indexed_finalized_head = session.state().indexed_finalized_head;
        validate_block_sequence(runtime, blocks, indexed_finalized_head).await?;
        let mut state = self
            .family
            .load_startup_state(runtime, indexed_finalized_head)
            .await?;

        for block in blocks {
            self.family
                .ingest_block(&self.config, runtime, &mut state, block)
                .await?;
        }

        let indexed_finalized_head = blocks
            .last()
            .map(FinalizedBlock::block_num)
            .expect("ingest requires at least one block");
        let outcome = self
            .family
            .finish_outcome(indexed_finalized_head, blocks, &state);

        session
            .publish(
                indexed_finalized_head,
                self.config.observe_upstream_finalized_block.as_ref()(),
            )
            .await?;

        Ok(outcome)
    }
}

async fn validate_block_sequence<M, B, T>(
    runtime: &Runtime<M, B>,
    blocks: &[T],
    indexed_finalized_head: u64,
) -> Result<()>
where
    M: MetaStore,
    B: BlobStore,
    T: FinalizedBlock,
{
    let expected_first = indexed_finalized_head.saturating_add(1);
    if blocks[0].block_num() != expected_first {
        return Err(Error::InvalidSequence {
            expected: expected_first,
            got: blocks[0].block_num(),
        });
    }

    let expected_parent = if indexed_finalized_head == 0 {
        [0u8; 32]
    } else {
        load_block_identity(runtime.tables(), indexed_finalized_head)
            .await?
            .ok_or(Error::NotFound)?
            .hash
    };
    if *blocks[0].parent_hash() != expected_parent {
        return Err(Error::InvalidParent);
    }

    for pair in blocks.windows(2) {
        let current = &pair[0];
        let next = &pair[1];
        let expected_block_num = current.block_num().saturating_add(1);
        if next.block_num() != expected_block_num {
            return Err(Error::InvalidSequence {
                expected: expected_block_num,
                got: next.block_num(),
            });
        }
        if next.parent_hash() != current.block_hash() {
            return Err(Error::InvalidParent);
        }
    }

    Ok(())
}
