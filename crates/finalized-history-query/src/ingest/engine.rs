use crate::config::Config;
use crate::core::state::{derive_next_log_id, load_block_identity};
use crate::domain::types::{Block, IngestOutcome, PublicationState};
use crate::error::{Error, Result};
use crate::ingest::open_pages::{
    OpenStreamPage, collect_newly_sealed_open_stream_pages, delete_open_stream_page,
    mark_open_stream_page_if_absent,
};
use crate::ingest::publication::PublicationLease;
use crate::logs::ingest::{
    compact_newly_sealed_directory, compact_stream_page, parse_stream_shard, persist_log_artifacts,
    persist_log_block_metadata, persist_log_directory_fragments, persist_stream_fragments,
};
use crate::store::publication::{CasOutcome, PublicationStore};
use crate::store::traits::{BlobStore, MetaStore};

pub struct IngestEngine<M: MetaStore + PublicationStore, B: BlobStore> {
    pub config: Config,
    pub meta_store: M,
    pub blob_store: B,
}

impl<M: MetaStore + PublicationStore, B: BlobStore> IngestEngine<M, B> {
    pub fn new(config: Config, meta_store: M, blob_store: B) -> Self {
        Self {
            config,
            meta_store,
            blob_store,
        }
    }

    pub async fn ingest_finalized_block(
        &self,
        block: &Block,
        lease: PublicationLease,
    ) -> Result<(IngestOutcome, PublicationLease)> {
        self.ingest_finalized_blocks(core::slice::from_ref(block), lease)
            .await
    }

    pub async fn ingest_finalized_blocks(
        &self,
        blocks: &[Block],
        lease: PublicationLease,
    ) -> Result<(IngestOutcome, PublicationLease)> {
        let Some(last_block) = blocks.last() else {
            return Err(Error::InvalidParams("ingest requires at least one block"));
        };

        validate_block_sequence(&self.meta_store, blocks, lease).await?;

        let from_next_log_id =
            derive_next_log_id(&self.meta_store, lease.indexed_finalized_head).await?;
        let mut next_log_id = from_next_log_id;
        let mut opened_during = Vec::<OpenStreamPage>::new();

        for block in blocks {
            persist_log_artifacts(
                &self.config,
                &self.meta_store,
                &self.blob_store,
                block.block_num,
                &block.logs,
                next_log_id,
                lease.owner_id,
            )
            .await?;
            persist_log_block_metadata(&self.meta_store, block, next_log_id, lease.owner_id)
                .await?;
            persist_log_directory_fragments(
                &self.meta_store,
                block.block_num,
                next_log_id,
                block.logs.len() as u32,
                lease.owner_id,
            )
            .await?;
            let touched_pages = persist_stream_fragments(
                &self.meta_store,
                &self.blob_store,
                block,
                next_log_id,
                lease.owner_id,
            )
            .await?;
            opened_during.extend(touched_pages.into_iter().filter_map(
                |(stream_id, page_start)| {
                    parse_stream_shard(&stream_id).map(|shard| OpenStreamPage {
                        shard,
                        page_start_local: page_start,
                        stream_id,
                    })
                },
            ));
            next_log_id = next_log_id.saturating_add(block.logs.len() as u64);
        }

        for page in opened_during
            .iter()
            .filter(|page| !page.is_sealed_at(next_log_id))
        {
            mark_open_stream_page_if_absent(&self.meta_store, page, lease.owner_id).await?;
        }

        compact_newly_sealed_directory(
            &self.meta_store,
            from_next_log_id,
            next_log_id,
            lease.owner_id,
        )
        .await?;

        for page in collect_newly_sealed_open_stream_pages(
            &self.meta_store,
            &opened_during,
            from_next_log_id,
            next_log_id,
        )
        .await?
        {
            let _ = compact_stream_page(
                &self.meta_store,
                &self.blob_store,
                &page.stream_id,
                page.page_start_local,
                lease.owner_id,
            )
            .await?;
            delete_open_stream_page(&self.meta_store, &page, lease.owner_id).await?;
        }

        let expected_state = lease.as_state();
        let next_lease = PublicationLease {
            owner_id: lease.owner_id,
            epoch: lease.epoch,
            indexed_finalized_head: last_block.block_num,
        };
        let next_state = PublicationState {
            owner_id: next_lease.owner_id,
            epoch: next_lease.epoch,
            indexed_finalized_head: next_lease.indexed_finalized_head,
        };
        match self
            .meta_store
            .compare_and_set(&expected_state, &next_state)
            .await?
        {
            CasOutcome::Applied(_) => {}
            CasOutcome::Failed {
                current: Some(current),
            } => {
                if current.owner_id != lease.owner_id || current.epoch != lease.epoch {
                    return Err(Error::LeaseLost);
                }
                return Err(Error::PublicationConflict);
            }
            CasOutcome::Failed { current: None } => {
                return Err(Error::PublicationConflict);
            }
        }

        Ok((
            IngestOutcome {
                indexed_finalized_head: last_block.block_num,
                written_logs: blocks.iter().map(|block| block.logs.len()).sum(),
            },
            next_lease,
        ))
    }

    pub async fn run_periodic_maintenance(&self, _writer_id: u64) -> Result<MaintenanceStats> {
        Ok(MaintenanceStats::default())
    }
}

async fn validate_block_sequence<M: MetaStore>(
    meta_store: &M,
    blocks: &[Block],
    lease: PublicationLease,
) -> Result<()> {
    let expected_first = lease.indexed_finalized_head.saturating_add(1);
    if blocks[0].block_num != expected_first {
        return Err(Error::InvalidSequence {
            expected: expected_first,
            got: blocks[0].block_num,
        });
    }

    let expected_parent = if lease.indexed_finalized_head == 0 {
        [0u8; 32]
    } else {
        load_block_identity(meta_store, lease.indexed_finalized_head)
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

#[derive(Debug, Clone, Copy, Default)]
pub struct MaintenanceStats {
    pub flushed_streams: u64,
    pub sealed_streams: u64,
}
