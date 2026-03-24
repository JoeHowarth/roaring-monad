use crate::config::Config;
use crate::core::state::{BlockRecord, PrimaryWindowRecord};
use crate::error::{Error, Result};
use crate::logs::family::LogsFamily;
use crate::logs::types::{Log, LogSequencingState};
use crate::runtime::Runtime;
use crate::store::traits::{BlobStore, MetaStore};
use crate::traces::{TraceSequencingState, TracesFamily};
use crate::txs::{Tx, TxFamilyState, TxsFamily};

pub type Hash32 = [u8; 32];

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct FinalizedBlock {
    pub block_num: u64,
    pub block_hash: Hash32,
    pub parent_hash: Hash32,
    pub logs: Vec<Log>,
    pub txs: Vec<Tx>,
    pub trace_rlp: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct FamilyStates {
    pub logs: LogSequencingState,
    pub txs: TxFamilyState,
    pub traces: TraceSequencingState,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FamilyBlockWrites {
    pub logs: usize,
    pub txs: usize,
    pub traces: usize,
}

impl core::ops::AddAssign for FamilyBlockWrites {
    fn add_assign(&mut self, rhs: Self) {
        self.logs = self.logs.saturating_add(rhs.logs);
        self.txs = self.txs.saturating_add(rhs.txs);
        self.traces = self.traces.saturating_add(rhs.traces);
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Families {
    pub logs: LogsFamily,
    pub txs: TxsFamily,
    pub traces: TracesFamily,
}

impl Families {
    pub async fn load_state_from_head<M, B>(
        &self,
        runtime: &Runtime<M, B>,
        indexed_finalized_head: u64,
    ) -> Result<FamilyStates>
    where
        M: MetaStore,
        B: BlobStore,
    {
        let head_record = load_head_block_record(runtime, indexed_finalized_head).await?;
        Ok(FamilyStates {
            logs: self
                .logs
                .load_state_from_head_record(head_record.as_ref())?,
            txs: self.txs.load_state_from_head_record(head_record.as_ref())?,
            traces: self
                .traces
                .load_state_from_head_record(head_record.as_ref())?,
        })
    }

    pub async fn ingest_block<M, B>(
        &self,
        config: &Config,
        runtime: &Runtime<M, B>,
        states: &mut FamilyStates,
        block: &FinalizedBlock,
    ) -> Result<FamilyBlockWrites>
    where
        M: MetaStore,
        B: BlobStore,
    {
        let first_log_id = states.logs.next_log_id.get();
        let first_trace_id = states.traces.next_trace_id.get();

        runtime
            .tables
            .block_hash_index
            .put(&block.block_hash, block.block_num)
            .await?;

        let writes = FamilyBlockWrites {
            logs: self
                .logs
                .ingest_block(config, runtime, &mut states.logs, block)
                .await?,
            txs: self
                .txs
                .ingest_block(config, runtime, &mut states.txs, block)
                .await?,
            traces: self
                .traces
                .ingest_block(config, runtime, &mut states.traces, block)
                .await?,
        };

        runtime
            .tables
            .block_records
            .put(
                block.block_num,
                &BlockRecord {
                    block_hash: block.block_hash,
                    parent_hash: block.parent_hash,
                    logs: Some(PrimaryWindowRecord {
                        first_primary_id: first_log_id,
                        count: writes.logs as u32,
                    }),
                    traces: Some(PrimaryWindowRecord {
                        first_primary_id: first_trace_id,
                        count: writes.traces as u32,
                    }),
                },
            )
            .await?;

        Ok(writes)
    }
}

pub(crate) async fn load_head_block_record<M, B>(
    runtime: &Runtime<M, B>,
    indexed_finalized_head: u64,
) -> Result<Option<BlockRecord>>
where
    M: MetaStore,
    B: BlobStore,
{
    if indexed_finalized_head == 0 {
        return Ok(None);
    }
    runtime
        .tables
        .block_records
        .get(indexed_finalized_head)
        .await?
        .ok_or(Error::NotFound)
        .map(Some)
}
