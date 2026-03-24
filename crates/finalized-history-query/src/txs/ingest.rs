use std::collections::BTreeMap;

use crate::config::Config;
use crate::error::Result;
use crate::family::FinalizedBlock;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;

pub fn collect_stream_appends(
    _block: &FinalizedBlock,
    _first_tx_id: u64,
) -> BTreeMap<String, Vec<u32>> {
    todo!("tx stream fanout planning is not implemented")
}

pub async fn persist_stream_fragments<M: MetaStore, B: BlobStore>(
    _tables: &Tables<M, B>,
    _block: &FinalizedBlock,
    _first_tx_id: u64,
) -> Result<Vec<(String, u32)>> {
    todo!("tx stream-fragment persistence is not implemented")
}

pub async fn persist_tx_artifacts<M: MetaStore, B: BlobStore>(
    _config: &Config,
    _tables: &Tables<M, B>,
    _block: &FinalizedBlock,
    _first_tx_id: u64,
) -> Result<usize> {
    todo!("tx authoritative artifact persistence is not implemented")
}
