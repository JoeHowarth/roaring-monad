use crate::core::state::BlockRecord;
use crate::error::{MonadChainDataError, Result};
use crate::family::FinalizedBlock;
use super::{LogBlockHeader, LogEntry};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogIngestPlan {
    pub block_record: BlockRecord,
    pub block_log_header: LogBlockHeader,
    pub block_log_blob: Vec<u8>,
    pub written_logs: usize,
}

pub fn plan_log_ingest(block: &FinalizedBlock) -> Result<LogIngestPlan> {
    // First pass writes only per-block payload/header artifacts plus the shared block record.
    // Later commits add global log IDs, directory fragments, and bitmap index artifacts.
    let logs = flatten_logs(block)?;

    let (block_log_header, block_log_blob) = encode_block_logs(&logs)?;
    let block_record = BlockRecord {
        block_number: block.block_number,
        block_hash: block.block_hash,
        parent_hash: block.parent_hash,
        log_count: u32::try_from(logs.len())
            .map_err(|_| MonadChainDataError::Decode("log count overflow"))?,
    };

    Ok(LogIngestPlan {
        block_record,
        block_log_header,
        block_log_blob,
        written_logs: logs.len(),
    })
}

fn flatten_logs(block: &FinalizedBlock) -> Result<Vec<LogEntry>> {
    let total_logs = block
        .logs_by_tx
        .iter()
        .try_fold(0usize, |count, tx_logs| count.checked_add(tx_logs.len()))
        .ok_or(MonadChainDataError::Decode("log count overflow"))?;
    let mut logs = Vec::with_capacity(total_logs);

    for (tx_index, tx_logs) in block.logs_by_tx.iter().enumerate() {
        let tx_index = u32::try_from(tx_index)
            .map_err(|_| MonadChainDataError::Decode("tx index overflow"))?;

        for log in tx_logs {
            let log_index = u32::try_from(logs.len())
                .map_err(|_| MonadChainDataError::Decode("log index overflow"))?;
            logs.push(LogEntry {
                block_number: block.block_number,
                block_hash: block.block_hash,
                tx_index,
                log_index,
                address: log.address,
                topics: log.data.topics().to_vec(),
                data: log.data.data.clone(),
            });
        }
    }

    Ok(logs)
}

fn encode_block_logs(logs: &[LogEntry]) -> Result<(LogBlockHeader, Vec<u8>)> {
    let mut offsets = Vec::with_capacity(logs.len() + 1);
    let mut blob = Vec::new();

    for log in logs {
        offsets.push(
            u32::try_from(blob.len())
                .map_err(|_| MonadChainDataError::Decode("block log blob too large"))?,
        );
        blob.extend_from_slice(&log.encode()?);
    }

    offsets.push(
        u32::try_from(blob.len())
            .map_err(|_| MonadChainDataError::Decode("block log blob too large"))?,
    );

    Ok((LogBlockHeader { offsets }, blob))
}
