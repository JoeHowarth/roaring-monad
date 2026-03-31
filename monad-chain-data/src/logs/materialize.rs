use crate::core::page::QueryOrder;
use crate::core::refs::BlockRef;
use crate::core::types::{Address, Topic};

use super::LogEntry;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LogFilter {
    pub address: Option<Address>,
    pub topic0: Option<Topic>,
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
