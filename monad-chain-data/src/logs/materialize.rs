use alloy_primitives::{Address, B256, Bytes};

use crate::core::refs::BlockRef;
use crate::core::page::QueryOrder;
use crate::error::{MonadChainDataError, Result};
use crate::kernel::storage;
use crate::store::{BlobStore, MetaStore};

use super::{LogBlockHeader, LogEntry};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LogFilter {
    pub address: Option<Address>,
    pub topic0: Option<B256>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryLogsRequest {
    pub from_block: Option<u64>,
    pub to_block: Option<u64>,
    pub order: QueryOrder,
    pub limit: usize,
    pub filter: LogFilter,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryLogsResponse {
    pub logs: Vec<LogEntry>,
    pub from_block: BlockRef,
    pub to_block: BlockRef,
    pub cursor_block: BlockRef,
}

pub fn matches_filter(log: &LogEntry, filter: &LogFilter) -> bool {
    if let Some(address) = filter.address {
        if log.address != address {
            return false;
        }
    }

    if let Some(topic0) = filter.topic0 {
        if log.topics.first().copied() != Some(topic0) {
            return false;
        }
    }

    true
}

pub async fn load_filtered_block_logs_for_block<M: MetaStore, B: BlobStore>(
    meta_store: &M,
    blob_store: &B,
    block_number: u64,
    request: &QueryLogsRequest,
) -> Result<(BlockRef, Vec<LogEntry>)> {
    let block_record = storage::load_block_record(meta_store, block_number)
        .await?
        .ok_or(MonadChainDataError::MissingData("missing block record"))?;
    let block_ref = BlockRef::from(&block_record);

    let header = storage::load_log_block_header(meta_store, block_number)
        .await?
        .ok_or(MonadChainDataError::MissingData("missing block log header"))?;
    let blob = storage::load_log_block_blob(blob_store, block_number)
        .await?
        .ok_or(MonadChainDataError::MissingData("missing block log blob"))?;

    let logs = load_filtered_block_logs(&header, &blob, request)?;

    Ok((block_ref, logs))
}

pub fn load_filtered_block_logs(
    header: &LogBlockHeader,
    blob: &Bytes,
    request: &QueryLogsRequest,
) -> Result<Vec<LogEntry>> {
    let mut logs = Vec::new();

    if request.order == QueryOrder::Ascending {
        for ordinal in 0..header.log_count() {
            let log = decode_log_at(header, blob.as_ref(), ordinal)?;
            if matches_filter(&log, &request.filter) {
                logs.push(log);
            }
        }
    } else {
        for ordinal in (0..header.log_count()).rev() {
            let log = decode_log_at(header, blob.as_ref(), ordinal)?;
            if matches_filter(&log, &request.filter) {
                logs.push(log);
            }
        }
    }

    Ok(logs)
}

pub fn decode_log_at(header: &LogBlockHeader, blob: &[u8], ordinal: usize) -> Result<LogEntry> {
    if ordinal + 1 >= header.offsets.len() {
        return Err(MonadChainDataError::Decode("log ordinal out of range"));
    }

    let start = header.offsets[ordinal] as usize;
    let end = header.offsets[ordinal + 1] as usize;
    if start > end || end > blob.len() {
        return Err(MonadChainDataError::Decode("invalid log range"));
    }

    LogEntry::decode(&blob[start..end])
}
