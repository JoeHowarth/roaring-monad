use bytes::Bytes;

use crate::config::Config;
use crate::domain::keys::{block_hash_to_num_key, block_meta_key, log_key};
use crate::domain::types::{Block, BlockMeta, IngestOutcome, MetaState};
use crate::error::{Error, Result};
use crate::store::traits::{FenceToken, MetaStore, PutCond};

const META_STATE_KEY: &[u8] = b"meta/state";

pub struct IngestEngine<S: MetaStore> {
    pub config: Config,
    pub meta_store: S,
}

impl<S: MetaStore> IngestEngine<S> {
    pub fn new(config: Config, meta_store: S) -> Self {
        Self { config, meta_store }
    }

    pub async fn ingest_finalized_block(&self, block: &Block, epoch: u64) -> Result<IngestOutcome> {
        let state = self.load_state().await?;
        let expected = state.indexed_finalized_head + 1;
        if block.block_num != expected {
            return Err(Error::InvalidSequence {
                expected,
                got: block.block_num,
            });
        }
        if block.block_num > 0 && block.parent_hash != self.load_parent_hash(state.indexed_finalized_head).await? {
            return Err(Error::InvalidParent);
        }

        let first_log_id = state.next_log_id;
        for (i, log) in block.logs.iter().enumerate() {
            let key = log_key(first_log_id + i as u64);
            let bytes = Bytes::from(log.data.clone());
            let _ = self
                .meta_store
                .put(&key, bytes, PutCond::Any, FenceToken(epoch))
                .await?;
        }

        let meta = BlockMeta {
            block_hash: block.block_hash,
            parent_hash: block.parent_hash,
            first_log_id,
            count: block.logs.len() as u32,
        };

        let block_key = block_meta_key(block.block_num);
        let block_meta_bytes = Bytes::from(format!("{}:{}", meta.first_log_id, meta.count));
        let _ = self
            .meta_store
            .put(&block_key, block_meta_bytes, PutCond::Any, FenceToken(epoch))
            .await?;

        let hash_key = block_hash_to_num_key(&block.block_hash);
        let _ = self
            .meta_store
            .put(
                &hash_key,
                Bytes::copy_from_slice(&block.block_num.to_be_bytes()),
                PutCond::Any,
                FenceToken(epoch),
            )
            .await?;

        let next = MetaState {
            indexed_finalized_head: block.block_num,
            next_log_id: first_log_id + block.logs.len() as u64,
            writer_epoch: epoch,
        };
        self.store_state(&state, &next, epoch).await?;

        Ok(IngestOutcome {
            indexed_finalized_head: block.block_num,
            written_logs: block.logs.len(),
        })
    }

    async fn load_state(&self) -> Result<MetaState> {
        match self.meta_store.get(META_STATE_KEY).await? {
            Some(r) => decode_state(&r.value),
            None => Ok(MetaState::default()),
        }
    }

    async fn store_state(&self, current: &MetaState, next: &MetaState, epoch: u64) -> Result<()> {
        let cond = if current == &MetaState::default() {
            PutCond::IfAbsent
        } else {
            // In this scaffold we do not persist a separate state version.
            PutCond::Any
        };
        let _ = self
            .meta_store
            .put(META_STATE_KEY, encode_state(next), cond, FenceToken(epoch))
            .await?;
        Ok(())
    }

    async fn load_parent_hash(&self, _block_num: u64) -> Result<[u8; 32]> {
        Ok([0u8; 32])
    }
}

fn encode_state(state: &MetaState) -> Bytes {
    let mut out = Vec::with_capacity(24);
    out.extend_from_slice(&state.indexed_finalized_head.to_be_bytes());
    out.extend_from_slice(&state.next_log_id.to_be_bytes());
    out.extend_from_slice(&state.writer_epoch.to_be_bytes());
    Bytes::from(out)
}

fn decode_state(bytes: &Bytes) -> Result<MetaState> {
    if bytes.len() != 24 {
        return Err(Error::Backend("invalid meta/state bytes".to_string()));
    }
    let mut a = [0u8; 8];
    let mut b = [0u8; 8];
    let mut c = [0u8; 8];
    a.copy_from_slice(&bytes[0..8]);
    b.copy_from_slice(&bytes[8..16]);
    c.copy_from_slice(&bytes[16..24]);
    Ok(MetaState {
        indexed_finalized_head: u64::from_be_bytes(a),
        next_log_id: u64::from_be_bytes(b),
        writer_epoch: u64::from_be_bytes(c),
    })
}
