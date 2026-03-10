use std::collections::HashMap;

use futures::stream::{FuturesUnordered, StreamExt};

use crate::codec::finalized_state::{decode_block_meta, decode_meta_state, encode_meta_state};
use crate::config::{Config, IngestMode};
use crate::domain::keys::{META_STATE_KEY, block_meta_key, chunk_blob_key};
use crate::domain::types::{Block, BlockMeta, IngestOutcome, MetaState};
use crate::error::{Error, Result};
use crate::logs::ingest::{
    collect_stream_appends, persist_log_artifacts, persist_log_block_metadata,
};
use crate::store::traits::{BlobStore, FenceToken, MetaStore, PutCond};
use crate::streams::chunk::{ChunkBlob, decode_chunk};
use crate::streams::keys::parse_stream_from_tail_key;
use crate::streams::writer::{CachedStreamState, StreamWriter};

pub struct IngestEngine<M: MetaStore, B: BlobStore> {
    pub config: Config,
    pub meta_store: M,
    pub blob_store: B,
    stream_state_cache: std::sync::RwLock<HashMap<String, CachedStreamState>>,
}

impl<M: MetaStore, B: BlobStore> IngestEngine<M, B> {
    pub fn new(config: Config, meta_store: M, blob_store: B) -> Self {
        Self {
            config,
            meta_store,
            blob_store,
            stream_state_cache: std::sync::RwLock::new(HashMap::new()),
        }
    }

    pub async fn ingest_finalized_block(&self, block: &Block, epoch: u64) -> Result<IngestOutcome> {
        let (state, state_version) = self.load_state().await?;
        let expected = state.indexed_finalized_head + 1;
        if block.block_num != expected {
            return Err(Error::InvalidSequence {
                expected,
                got: block.block_num,
            });
        }

        if block.block_num > 0 {
            let expected_parent = if state.indexed_finalized_head == 0 {
                [0u8; 32]
            } else {
                self.load_block_meta(state.indexed_finalized_head)
                    .await?
                    .block_hash
            };
            if block.parent_hash != expected_parent {
                return Err(Error::InvalidParent);
            }
        }

        let first_log_id = state.next_log_id;
        persist_log_artifacts(
            &self.config,
            &self.meta_store,
            &self.blob_store,
            &block.logs,
            first_log_id,
            epoch,
        )
        .await?;
        persist_log_block_metadata(&self.meta_store, block, first_log_id, epoch).await?;

        let appends = collect_stream_appends(block, first_log_id);
        let stream_writer = StreamWriter::new(
            &self.config,
            &self.meta_store,
            &self.blob_store,
            &self.stream_state_cache,
        );
        let mut appends_in_flight = FuturesUnordered::new();
        for (stream_id, values) in appends {
            let writer = stream_writer;
            appends_in_flight
                .push(async move { writer.apply_appends(&stream_id, &values, epoch).await });
            if appends_in_flight.len() >= self.config.stream_append_concurrency.max(1) {
                match appends_in_flight.next().await {
                    Some(Ok(_)) => {}
                    Some(Err(e)) => return Err(e),
                    None => break,
                }
            }
        }
        while let Some(res) = appends_in_flight.next().await {
            let _ = res?;
        }

        let next = MetaState {
            indexed_finalized_head: block.block_num,
            next_log_id: first_log_id + block.logs.len() as u64,
            writer_epoch: epoch,
        };

        self.store_state(&next, state_version, epoch).await?;

        Ok(IngestOutcome {
            indexed_finalized_head: block.block_num,
            written_logs: block.logs.len(),
        })
    }

    pub async fn run_periodic_maintenance(&self, epoch: u64) -> Result<MaintenanceStats> {
        let stream_writer = StreamWriter::new(
            &self.config,
            &self.meta_store,
            &self.blob_store,
            &self.stream_state_cache,
        );
        stream_writer.clear_cache();
        let mut stats = MaintenanceStats::default();
        let page = self
            .meta_store
            .list_prefix(b"tails/", None, usize::MAX)
            .await?;
        for key in page.keys {
            if let Some(stream) = parse_stream_from_tail_key(&key) {
                let sealed = stream_writer.apply_appends(&stream, &[], epoch).await?;
                if sealed {
                    stats.sealed_streams = stats.sealed_streams.saturating_add(1);
                }
                stats.flushed_streams = stats.flushed_streams.saturating_add(1);
            }
        }
        Ok(stats)
    }

    async fn load_state(&self) -> Result<(MetaState, Option<u64>)> {
        match self.meta_store.get(META_STATE_KEY).await? {
            Some(rec) => Ok((decode_meta_state(&rec.value)?, Some(rec.version))),
            None => Ok((MetaState::default(), None)),
        }
    }

    async fn store_state(
        &self,
        next: &MetaState,
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
        let r = self
            .meta_store
            .put(
                META_STATE_KEY,
                encode_meta_state(next),
                cond,
                FenceToken(epoch),
            )
            .await?;
        if strict_applied_check && !r.applied {
            return Err(Error::CasConflict);
        }
        Ok(())
    }

    async fn load_block_meta(&self, block_num: u64) -> Result<BlockMeta> {
        let rec = self
            .meta_store
            .get(&block_meta_key(block_num))
            .await?
            .ok_or(Error::NotFound)?;
        decode_block_meta(&rec.value)
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
