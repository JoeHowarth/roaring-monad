use bytes::Bytes;

use crate::core::state::{BlockRecord, PublicationState};
use crate::error::Result;
use crate::logs::LogBlockHeader;
use crate::store::{BlobStore, BlobTableId, MetaStore, TableId};

pub const PUBLICATION_STATE_TABLE: TableId = TableId::new("publication_state");
pub const BLOCK_RECORD_TABLE: TableId = TableId::new("block_record");
pub const BLOCK_LOG_HEADER_TABLE: TableId = TableId::new("block_log_header");
pub const BLOCK_LOG_BLOB_TABLE: BlobTableId = BlobTableId::new("block_log_blob");
pub const PUBLICATION_STATE_KEY: &[u8] = b"state";

pub fn block_number_key(block_number: u64) -> [u8; 8] {
    block_number.to_be_bytes()
}

pub async fn load_published_head<M: MetaStore>(meta_store: &M) -> Result<Option<u64>> {
    let Some(record) = meta_store
        .get(PUBLICATION_STATE_TABLE, PUBLICATION_STATE_KEY)
        .await?
    else {
        return Ok(None);
    };

    Ok(Some(PublicationState::decode(&record.value)?.indexed_finalized_head))
}

pub async fn store_publication_state<M: MetaStore>(
    meta_store: &M,
    state: PublicationState,
) -> Result<()> {
    meta_store
        .put(
            PUBLICATION_STATE_TABLE,
            PUBLICATION_STATE_KEY,
            Bytes::from(state.encode()),
        )
        .await?;
    Ok(())
}

pub async fn load_block_record<M: MetaStore>(
    meta_store: &M,
    block_number: u64,
) -> Result<Option<BlockRecord>> {
    let key = block_number_key(block_number);
    let Some(record) = meta_store.get(BLOCK_RECORD_TABLE, &key).await? else {
        return Ok(None);
    };
    Ok(Some(BlockRecord::decode(&record.value)?))
}

pub async fn store_block_record<M: MetaStore>(
    meta_store: &M,
    block_number: u64,
    block_record: &BlockRecord,
) -> Result<()> {
    let key = block_number_key(block_number);
    meta_store
        .put(BLOCK_RECORD_TABLE, &key, Bytes::from(block_record.encode()))
        .await?;
    Ok(())
}

pub async fn load_log_block_header<M: MetaStore>(
    meta_store: &M,
    block_number: u64,
) -> Result<Option<LogBlockHeader>> {
    let key = block_number_key(block_number);
    let Some(record) = meta_store.get(BLOCK_LOG_HEADER_TABLE, &key).await? else {
        return Ok(None);
    };
    Ok(Some(LogBlockHeader::decode(&record.value)?))
}

pub async fn store_log_block_header<M: MetaStore>(
    meta_store: &M,
    block_number: u64,
    block_log_header: &LogBlockHeader,
) -> Result<()> {
    let key = block_number_key(block_number);
    meta_store
        .put(
            BLOCK_LOG_HEADER_TABLE,
            &key,
            Bytes::from(block_log_header.encode()?),
        )
        .await?;
    Ok(())
}

pub async fn load_log_block_blob<B: BlobStore>(
    blob_store: &B,
    block_number: u64,
) -> Result<Option<alloy_primitives::Bytes>> {
    let key = block_number_key(block_number);
    Ok(blob_store
        .get_blob(BLOCK_LOG_BLOB_TABLE, &key)
        .await?
        .map(Into::into))
}

pub async fn store_log_block_blob<B: BlobStore>(
    blob_store: &B,
    block_number: u64,
    block_log_blob: Vec<u8>,
) -> Result<()> {
    let key = block_number_key(block_number);
    blob_store
        .put_blob(BLOCK_LOG_BLOB_TABLE, &key, Bytes::from(block_log_blob))
        .await?;
    Ok(())
}
