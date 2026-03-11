use crate::codec::finalized_state::{
    decode_indexed_head, encode_indexed_head, encode_writer_lease,
};
use crate::config::{Config, IngestMode};
use crate::core::state::{derive_next_log_id, load_block_identity};
use crate::domain::keys::{INDEXED_HEAD_KEY, WRITER_LEASE_KEY, chunk_blob_key};
use crate::domain::types::{Block, IndexedHead, IngestOutcome, WriterLease};
use crate::error::{Error, Result};
use crate::logs::ingest::{
    compact_sealed_directory, compact_sealed_stream_pages, persist_log_artifacts,
    persist_log_block_metadata, persist_log_directory_fragments, persist_stream_fragments,
};
use crate::store::traits::{BlobStore, FenceToken, MetaStore, PutCond};
use crate::streams::chunk::{ChunkBlob, decode_chunk};

pub struct IngestEngine<M: MetaStore, B: BlobStore> {
    pub config: Config,
    pub meta_store: M,
    pub blob_store: B,
}

impl<M: MetaStore, B: BlobStore> IngestEngine<M, B> {
    pub fn new(config: Config, meta_store: M, blob_store: B) -> Self {
        Self {
            config,
            meta_store,
            blob_store,
        }
    }

    pub async fn ingest_finalized_block(&self, block: &Block, epoch: u64) -> Result<IngestOutcome> {
        let (head, head_version) = self.load_indexed_head().await?;
        let expected = head.indexed_finalized_head + 1;
        if block.block_num != expected {
            return Err(Error::InvalidSequence {
                expected,
                got: block.block_num,
            });
        }

        if block.block_num > 0 {
            let expected_parent = if head.indexed_finalized_head == 0 {
                [0u8; 32]
            } else {
                load_block_identity(&self.meta_store, head.indexed_finalized_head)
                    .await?
                    .ok_or(Error::NotFound)?
                    .hash
            };
            if block.parent_hash != expected_parent {
                return Err(Error::InvalidParent);
            }
        }

        let first_log_id =
            derive_next_log_id(&self.meta_store, head.indexed_finalized_head).await?;
        let next_log_id = first_log_id + block.logs.len() as u64;

        self.meta_store
            .put(
                WRITER_LEASE_KEY,
                encode_writer_lease(&WriterLease {
                    owner_id: epoch,
                    epoch,
                }),
                PutCond::Any,
                FenceToken(epoch),
            )
            .await?;

        persist_log_artifacts(
            &self.config,
            &self.meta_store,
            &self.blob_store,
            block.block_num,
            &block.logs,
            first_log_id,
            epoch,
        )
        .await?;
        persist_log_block_metadata(&self.meta_store, block, first_log_id, epoch).await?;
        persist_log_directory_fragments(
            &self.meta_store,
            block.block_num,
            first_log_id,
            block.logs.len() as u32,
            epoch,
        )
        .await?;
        let touched_pages = persist_stream_fragments(
            &self.meta_store,
            &self.blob_store,
            block,
            first_log_id,
            epoch,
        )
        .await?;
        compact_sealed_directory(
            &self.meta_store,
            first_log_id,
            block.logs.len() as u32,
            next_log_id,
            epoch,
        )
        .await?;
        compact_sealed_stream_pages(
            &self.meta_store,
            &self.blob_store,
            &touched_pages,
            next_log_id,
            epoch,
        )
        .await?;

        self.store_indexed_head(
            &IndexedHead {
                indexed_finalized_head: block.block_num,
            },
            head_version,
            epoch,
        )
        .await?;

        Ok(IngestOutcome {
            indexed_finalized_head: block.block_num,
            written_logs: block.logs.len(),
        })
    }

    pub async fn run_periodic_maintenance(&self, _epoch: u64) -> Result<MaintenanceStats> {
        Ok(MaintenanceStats::default())
    }

    async fn load_indexed_head(&self) -> Result<(IndexedHead, Option<u64>)> {
        match self.meta_store.get(INDEXED_HEAD_KEY).await? {
            Some(rec) => Ok((decode_indexed_head(&rec.value)?, Some(rec.version))),
            None => Ok((IndexedHead::default(), None)),
        }
    }

    async fn store_indexed_head(
        &self,
        next: &IndexedHead,
        current_version: Option<u64>,
        epoch: u64,
    ) -> Result<()> {
        let (cond, strict_applied_check) = match self.config.ingest_mode {
            IngestMode::StrictCas => (
                match current_version {
                    Some(v) => PutCond::IfVersion(v),
                    None => PutCond::IfAbsent,
                },
                true,
            ),
            IngestMode::SingleWriterFast => (PutCond::Any, false),
        };
        let result = self
            .meta_store
            .put(
                INDEXED_HEAD_KEY,
                encode_indexed_head(next),
                cond,
                FenceToken(epoch),
            )
            .await?;
        if strict_applied_check && !result.applied {
            return Err(Error::CasConflict);
        }
        Ok(())
    }
}

#[allow(dead_code)]
async fn load_chunk_if_present<B: BlobStore>(
    blob_store: &B,
    stream: &str,
    seq: u64,
) -> Result<Option<ChunkBlob>> {
    let bytes = blob_store.get_blob(&chunk_blob_key(stream, seq)).await?;
    match bytes {
        Some(v) => Ok(Some(decode_chunk(&v)?)),
        None => Ok(None),
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct MaintenanceStats {
    pub flushed_streams: u64,
    pub sealed_streams: u64,
}
