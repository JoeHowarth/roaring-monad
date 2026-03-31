use crate::error::Result;
use crate::store::{BlobStore, MetaStore};

pub async fn load_published_head<M: MetaStore>(meta_store: &M) -> Result<Option<u64>> {
    let _ = meta_store;
    todo!("load publication-state metadata for the current head")
}

pub async fn load_block_record<M: MetaStore>(
    meta_store: &M,
    block_number: u64,
) -> Result<Option<crate::core::state::BlockRecord>> {
    let _ = (meta_store, block_number);
    todo!("load a shared block record by block number")
}

pub async fn store_block_record<M: MetaStore>(
    meta_store: &M,
    block_number: u64,
    block_record: &crate::core::state::BlockRecord,
) -> Result<()> {
    let _ = (meta_store, block_number, block_record);
    todo!("store a shared block record by block number")
}

pub async fn load_log_block_header<M: MetaStore>(
    meta_store: &M,
    block_number: u64,
) -> Result<Option<crate::logs::LogBlockHeader>> {
    let _ = (meta_store, block_number);
    todo!("load a logs block header by block number")
}

pub async fn store_log_block_header<M: MetaStore>(
    meta_store: &M,
    block_number: u64,
    block_log_header: &crate::logs::LogBlockHeader,
) -> Result<()> {
    let _ = (meta_store, block_number, block_log_header);
    todo!("store a logs block header by block number")
}

pub async fn load_log_block_blob<B: BlobStore>(
    blob_store: &B,
    block_number: u64,
) -> Result<Option<bytes::Bytes>> {
    let _ = (blob_store, block_number);
    todo!("load a logs block blob by block number")
}

pub async fn store_log_block_blob<B: BlobStore>(
    blob_store: &B,
    block_number: u64,
    block_log_blob: Vec<u8>,
) -> Result<()> {
    let _ = (blob_store, block_number, block_log_blob);
    todo!("store a logs block blob by block number")
}

pub async fn store_publication_state<M: MetaStore>(
    meta_store: &M,
    state: crate::core::state::PublicationState,
) -> Result<()> {
    let _ = (meta_store, state);
    todo!("store publication-state metadata")
}
