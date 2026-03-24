use crate::core::clause::Clause;
use crate::logs::filter::LogFilter;
use crate::query::planner::{IndexedClause, clause_values, indexed_clause};

pub(in crate::logs) type IndexedClauseSpec = IndexedClause;

pub(in crate::logs) fn build_clause_specs(filter: &LogFilter) -> Vec<IndexedClauseSpec> {
    let mut clauses = Vec::new();

    if let Some(clause) = &filter.address
        && let Some(clause) = indexed_clause("addr", clause_values_20(clause))
    {
        clauses.push(clause);
    }

    if let Some(clause) = &filter.topic1
        && let Some(clause) = indexed_clause("topic1", clause_values_32(clause))
    {
        clauses.push(clause);
    }

    if let Some(clause) = &filter.topic2
        && let Some(clause) = indexed_clause("topic2", clause_values_32(clause))
    {
        clauses.push(clause);
    }

    if let Some(clause) = &filter.topic3
        && let Some(clause) = indexed_clause("topic3", clause_values_32(clause))
    {
        clauses.push(clause);
    }

    if let Some(clause) = &filter.topic0
        && let Some(clause) = indexed_clause("topic0", clause_values_32(clause))
    {
        clauses.push(clause);
    }

    clauses
}

fn clause_values_20(clause: &Clause<[u8; 20]>) -> Vec<Vec<u8>> {
    clause_values(clause)
}

fn clause_values_32(clause: &Clause<[u8; 32]>) -> Vec<Vec<u8>> {
    clause_values(clause)
}
