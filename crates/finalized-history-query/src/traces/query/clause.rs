use crate::core::clause::Clause;
use crate::query::planner::{IndexedClause, clause_values, indexed_clause, single_selector_clause};
use crate::traces::filter::TraceFilter;

pub(in crate::traces) type IndexedClauseSpec = IndexedClause;

pub(in crate::traces) fn build_clause_specs(filter: &TraceFilter) -> Vec<IndexedClauseSpec> {
    let mut clauses = Vec::new();

    if let Some(clause) = &filter.from
        && let Some(clause) = indexed_clause("from", clause_values_20(clause))
    {
        clauses.push(clause);
    }

    if let Some(clause) = &filter.to
        && let Some(clause) = indexed_clause("to", clause_values_20(clause))
    {
        clauses.push(clause);
    }

    if let Some(clause) = &filter.selector
        && let Some(clause) = indexed_clause("selector", clause_values_4(clause))
    {
        clauses.push(clause);
    }

    if filter.has_value == Some(true) {
        clauses.push(single_selector_clause("has_value", vec![1]));
    }

    clauses
}

fn clause_values_20(clause: &Clause<[u8; 20]>) -> Vec<Vec<u8>> {
    clause_values(clause)
}

fn clause_values_4(clause: &Clause<[u8; 4]>) -> Vec<Vec<u8>> {
    clause_values(clause)
}
