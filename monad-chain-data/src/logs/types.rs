use alloy_rlp::{RlpDecodable, RlpEncodable};
use alloy_primitives::{Address, B256, Bytes};

use crate::error::{MonadChainDataError, Result};
use crate::family::Hash32;

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct LogEntry {
    pub block_number: u64,
    pub block_hash: Hash32,
    pub tx_index: u32,
    pub log_index: u32,
    pub address: Address,
    pub topics: Vec<B256>,
    pub data: Bytes,
}

impl LogEntry {
    pub fn encode(&self) -> Result<Vec<u8>> {
        Ok(alloy_rlp::encode(self))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        alloy_rlp::decode_exact(bytes)
            .map_err(|_| MonadChainDataError::Decode("invalid log entry rlp"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct LogBlockHeader {
    pub offsets: Vec<u32>,
}

impl LogBlockHeader {
    pub fn log_count(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        Ok(alloy_rlp::encode(self))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        alloy_rlp::decode_exact(bytes)
            .map_err(|_| MonadChainDataError::Decode("invalid log header rlp"))
    }
}
