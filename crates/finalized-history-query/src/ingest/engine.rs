use std::collections::BTreeSet;

use crate::config::Config;
use crate::core::state::{derive_next_log_id, load_block_identity};
use crate::domain::types::{Block, IngestOutcome, PublicationState};
use crate::error::{Error, Result};
use crate::ingest::open_pages::{
    OpenStreamPage, list_all_open_stream_pages, mark_open_stream_page_if_absent,
    repair_open_stream_page_markers,
};
use crate::ingest::publication::acquire_publication;
use crate::ingest::recovery::cleanup_unpublished_suffix;
use crate::logs::ingest::{
    compact_sealed_directory, compact_stream_page, parse_stream_shard, persist_log_artifacts,
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
        writer_id: u64,
    ) -> Result<IngestOutcome> {
        let lease = acquire_publication(&self.meta_store, writer_id).await?;
        let _cleaned = cleanup_unpublished_suffix(
            &self.meta_store,
            &self.blob_store,
            lease.indexed_finalized_head,
            writer_id,
        )
        .await?;

        let first_log_id =
            derive_next_log_id(&self.meta_store, lease.indexed_finalized_head).await?;
        repair_open_stream_page_markers(
            &self.meta_store,
            &self.blob_store,
            first_log_id,
            writer_id,
        )
        .await?;

        let expected = lease.indexed_finalized_head.saturating_add(1);
        if block.block_num != expected {
            return Err(Error::InvalidSequence {
                expected,
                got: block.block_num,
            });
        }

        if block.block_num > 0 {
            let expected_parent = if lease.indexed_finalized_head == 0 {
                [0u8; 32]
            } else {
                load_block_identity(&self.meta_store, lease.indexed_finalized_head)
                    .await?
                    .ok_or(Error::NotFound)?
                    .hash
            };
            if block.parent_hash != expected_parent {
                return Err(Error::InvalidParent);
            }
        }

        let next_log_id = first_log_id.saturating_add(block.logs.len() as u64);

        persist_log_artifacts(
            &self.config,
            &self.meta_store,
            &self.blob_store,
            block.block_num,
            &block.logs,
            first_log_id,
            writer_id,
        )
        .await?;
        persist_log_block_metadata(&self.meta_store, block, first_log_id, writer_id).await?;
        persist_log_directory_fragments(
            &self.meta_store,
            block.block_num,
            first_log_id,
            block.logs.len() as u32,
            writer_id,
        )
        .await?;
        let touched_pages = persist_stream_fragments(
            &self.meta_store,
            &self.blob_store,
            block,
            first_log_id,
            writer_id,
        )
        .await?;

        let opened_during = touched_pages
            .iter()
            .filter_map(|(stream_id, page_start_local)| {
                parse_stream_shard(stream_id).map(|shard| OpenStreamPage {
                    shard,
                    page_start_local: *page_start_local,
                    stream_id: stream_id.clone(),
                })
            })
            .collect::<Vec<_>>();
        for page in opened_during
            .iter()
            .filter(|page| !page.is_sealed_at(next_log_id))
        {
            mark_open_stream_page_if_absent(&self.meta_store, page, writer_id).await?;
        }

        compact_sealed_directory(
            &self.meta_store,
            first_log_id,
            block.logs.len() as u32,
            next_log_id,
            writer_id,
        )
        .await?;

        for page in collect_pages_to_seal(&self.meta_store, &opened_during, next_log_id).await? {
            let _ = compact_stream_page(
                &self.meta_store,
                &self.blob_store,
                &page.stream_id,
                page.page_start_local,
                writer_id,
            )
            .await?;
            crate::ingest::open_pages::delete_open_stream_page(&self.meta_store, &page, writer_id)
                .await?;
        }

        let expected_state = lease.as_state();
        let next_state = PublicationState {
            owner_id: lease.owner_id,
            epoch: lease.epoch,
            indexed_finalized_head: block.block_num,
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

        Ok(IngestOutcome {
            indexed_finalized_head: block.block_num,
            written_logs: block.logs.len(),
        })
    }

    pub async fn run_periodic_maintenance(&self, _writer_id: u64) -> Result<MaintenanceStats> {
        Ok(MaintenanceStats::default())
    }
}

async fn collect_pages_to_seal<M: MetaStore>(
    meta_store: &M,
    opened_during: &[OpenStreamPage],
    next_log_id: u64,
) -> Result<Vec<OpenStreamPage>> {
    let mut pages = list_all_open_stream_pages(meta_store)
        .await?
        .into_iter()
        .collect::<BTreeSet<_>>();
    pages.extend(opened_during.iter().cloned());
    Ok(pages
        .into_iter()
        .filter(|page| page.is_sealed_at(next_log_id))
        .collect())
}

#[derive(Debug, Clone, Copy, Default)]
pub struct MaintenanceStats {
    pub flushed_streams: u64,
    pub sealed_streams: u64,
}
