use crate::api::ExecutionBudget;
use crate::core::ids::{FamilyIdRange, FamilyIdValue};
use crate::core::range::ResolvedBlockRange;
use crate::error::{Error, Result};

#[derive(Debug, Clone)]
pub(crate) struct PlannedQuery<I> {
    pub block_range: ResolvedBlockRange,
    pub id_range: FamilyIdRange<I>,
    pub effective_limit: usize,
    pub take: usize,
}

pub(crate) fn effective_limit(limit: usize, budget: ExecutionBudget) -> Result<usize> {
    if limit == 0 {
        return Err(Error::InvalidParams("limit must be at least 1"));
    }

    match budget.max_results {
        Some(0) => Err(Error::InvalidParams(
            "budget.max_results must be at least 1 when set",
        )),
        Some(max_results) => Ok(limit.min(max_results)),
        None => Ok(limit),
    }
}

pub(crate) fn plan_page<I: FamilyIdValue + Copy + Ord>(
    block_range: &ResolvedBlockRange,
    mut id_range: FamilyIdRange<I>,
    resume_after: Option<I>,
    effective_limit: usize,
    resume_outside_message: &'static str,
) -> Result<Option<PlannedQuery<I>>> {
    if let Some(resume_id) = resume_after {
        if !id_range.contains(resume_id) {
            return Err(Error::InvalidParams(resume_outside_message));
        }
        let Some(resumed_range) = id_range.resume_strictly_after(resume_id) else {
            return Ok(None);
        };
        id_range = resumed_range;
    }

    Ok(Some(PlannedQuery {
        block_range: block_range.clone(),
        id_range,
        effective_limit,
        take: effective_limit.saturating_add(1),
    }))
}
