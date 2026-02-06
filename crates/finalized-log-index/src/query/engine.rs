use crate::domain::filter::{LogFilter, QueryOptions};
use crate::domain::types::Log;
use crate::error::{Error, Result};
use crate::query::executor::execute_plan;
use crate::query::planner::QueryPlan;

#[derive(Debug, Clone)]
pub struct QueryEngine {
    pub max_or_terms: usize,
}

impl QueryEngine {
    pub fn new(max_or_terms: usize) -> Self {
        Self { max_or_terms }
    }

    pub async fn query_finalized(
        &self,
        filter: LogFilter,
        options: QueryOptions,
        finalized_head: u64,
    ) -> Result<Vec<Log>> {
        let max_terms = filter.max_or_terms();
        if max_terms > self.max_or_terms {
            return Err(Error::QueryTooBroad {
                actual: max_terms,
                max: self.max_or_terms,
            });
        }

        let from = filter.from_block.unwrap_or(0);
        let to = filter.to_block.unwrap_or(finalized_head);
        if from > finalized_head {
            return Ok(Vec::new());
        }
        let plan = QueryPlan {
            filter,
            options,
            clipped_from: from,
            clipped_to: to.min(finalized_head),
        };
        execute_plan(plan).await
    }
}

impl Default for QueryEngine {
    fn default() -> Self {
        Self { max_or_terms: 128 }
    }
}
