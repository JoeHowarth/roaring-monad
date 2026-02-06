use crate::domain::filter::{LogFilter, QueryOptions};

#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub filter: LogFilter,
    pub options: QueryOptions,
    pub clipped_from: u64,
    pub clipped_to: u64,
}
