use crate::api::IngestOutcome;
use crate::config::Config;
use crate::core::state::load_block_identity;
use crate::error::{Error, Result};
use crate::family::{Families, FamilyBlockWrites, FamilyStates, FinalizedBlock};
use crate::ingest::authority::{WriteAuthority, WriteSession};
use crate::ingest::open_pages::repair_sealed_open_bitmap_pages;
use crate::runtime::Runtime;
use crate::store::traits::{BlobStore, MetaStore};

pub struct IngestEngine<A: WriteAuthority> {
    pub config: Config,
    pub authority: A,
    pub families: Families,
}

impl<A> IngestEngine<A>
where
    A: WriteAuthority,
{
    async fn begin_recovered_write<'a, M, B>(
        &'a self,
        runtime: &Runtime<M, B>,
    ) -> Result<(A::Session<'a>, FamilyStates)>
    where
        M: MetaStore,
        B: BlobStore,
    {
        let session = self
            .authority
            .begin_write(self.config.observe_upstream_finalized_block.as_ref()())
            .await?;
        let state = session.state();
        let family_states = self
            .families
            .load_startup_state(runtime, state.indexed_finalized_head)
            .await?;

        if state.needs_recovery {
            repair_sealed_open_bitmap_pages(
                &runtime.tables,
                family_states.logs.next_log_id.get(),
                family_states.traces.next_trace_id.get(),
            )
            .await?;
        }

        Ok((session, family_states))
    }

    pub fn new(config: Config, authority: A, families: Families) -> Self {
        Self {
            config,
            authority,
            families,
        }
    }

    pub async fn ingest_finalized_block<M, B>(
        &self,
        runtime: &Runtime<M, B>,
        block: &FinalizedBlock,
    ) -> Result<IngestOutcome>
    where
        M: MetaStore,
        B: BlobStore,
    {
        self.ingest_finalized_blocks(runtime, core::slice::from_ref(block))
            .await
    }

    pub async fn ingest_finalized_blocks<M, B>(
        &self,
        runtime: &Runtime<M, B>,
        blocks: &[FinalizedBlock],
    ) -> Result<IngestOutcome>
    where
        M: MetaStore,
        B: BlobStore,
    {
        if blocks.is_empty() {
            return Err(Error::InvalidParams("ingest requires at least one block"));
        }

        let (session, mut family_states) = self.begin_recovered_write(runtime).await?;
        let indexed_finalized_head = session.state().indexed_finalized_head;
        validate_block_sequence(runtime, blocks, indexed_finalized_head).await?;
        let mut writes = FamilyBlockWrites::default();

        for block in blocks {
            writes += self
                .families
                .ingest_block(&self.config, runtime, &mut family_states, block)
                .await?;
        }

        let indexed_finalized_head = blocks
            .last()
            .map(|block| block.block_num)
            .expect("ingest requires at least one block");
        session
            .publish(
                indexed_finalized_head,
                self.config.observe_upstream_finalized_block.as_ref()(),
            )
            .await?;

        Ok(IngestOutcome {
            indexed_finalized_head,
            written_logs: writes.logs,
            written_txs: writes.txs,
            written_traces: writes.traces,
        })
    }
}

async fn validate_block_sequence<M, B>(
    runtime: &Runtime<M, B>,
    blocks: &[FinalizedBlock],
    indexed_finalized_head: u64,
) -> Result<()>
where
    M: MetaStore,
    B: BlobStore,
{
    let expected_first = indexed_finalized_head.saturating_add(1);
    if blocks[0].block_num != expected_first {
        return Err(Error::InvalidSequence {
            expected: expected_first,
            got: blocks[0].block_num,
        });
    }

    let expected_parent = if indexed_finalized_head == 0 {
        [0u8; 32]
    } else {
        load_block_identity(&runtime.tables, indexed_finalized_head)
            .await?
            .ok_or(Error::NotFound)?
            .hash
    };
    if blocks[0].parent_hash != expected_parent {
        return Err(Error::InvalidParent);
    }

    for pair in blocks.windows(2) {
        let current = &pair[0];
        let next = &pair[1];
        let expected_block_num = current.block_num.saturating_add(1);
        if next.block_num != expected_block_num {
            return Err(Error::InvalidSequence {
                expected: expected_block_num,
                got: next.block_num,
            });
        }
        if next.parent_hash != current.block_hash {
            return Err(Error::InvalidParent);
        }
    }

    Ok(())
}
