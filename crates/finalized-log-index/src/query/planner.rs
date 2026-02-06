use crate::domain::filter::{LogFilter, QueryOptions};

#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub filter: LogFilter,
    pub options: QueryOptions,
    pub clipped_from_block: u64,
    pub clipped_to_block: u64,
    pub from_log_id: u64,
    pub to_log_id_inclusive: u64,
}
