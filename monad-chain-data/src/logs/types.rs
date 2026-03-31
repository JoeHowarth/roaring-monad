use crate::core::types::{Address, Topic};
use crate::family::Hash32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub block_number: u64,
    pub block_hash: Hash32,
    pub tx_index: u32,
    pub log_index: u32,
    pub address: Address,
    pub topics: Vec<Topic>,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogBlockHeader {
    pub offsets: Vec<u32>,
}
