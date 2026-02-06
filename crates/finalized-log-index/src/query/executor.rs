use crate::domain::types::Log;
use crate::error::Result;
use crate::query::planner::QueryPlan;

pub async fn execute_plan(_plan: QueryPlan) -> Result<Vec<Log>> {
    Ok(Vec::new())
}
