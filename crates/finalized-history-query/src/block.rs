use crate::logs::types::Log;
use crate::primitives::Hash32;
use crate::traces::types::Trace;
use crate::txs::types::Tx;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct FinalizedBlock {
    pub block_num: u64,
    pub block_hash: Hash32,
    pub parent_hash: Hash32,
    pub logs: Vec<Log>,
    pub txs: Vec<Tx>,
    pub traces: Vec<Trace>,
}
