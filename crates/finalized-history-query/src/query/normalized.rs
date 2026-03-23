use crate::api::ExecutionBudget;
use crate::core::range::ResolvedBlockRange;
use crate::error::{Error, Result};

use super::types::PrimaryRange;

#[derive(Debug, Clone)]
pub(crate) struct NormalizedQuery<R: PrimaryRange> {
    pub block_range: ResolvedBlockRange,
    pub id_range: R,
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

pub(crate) fn normalize_query<R: PrimaryRange>(
    block_range: &ResolvedBlockRange,
    mut id_range: R,
    resume_after: Option<R::Id>,
    effective_limit: usize,
    resume_outside_message: &'static str,
) -> Result<Option<NormalizedQuery<R>>> {
    if let Some(resume_id) = resume_after {
        if !id_range.contains(resume_id) {
            return Err(Error::InvalidParams(resume_outside_message));
        }
        let Some(resumed_range) = id_range.resume_strictly_after(resume_id) else {
            return Ok(None);
        };
        id_range = resumed_range;
    }

    Ok(Some(NormalizedQuery {
        block_range: block_range.clone(),
        id_range,
        effective_limit,
        take: effective_limit.saturating_add(1),
    }))
}
