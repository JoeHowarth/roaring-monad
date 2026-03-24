use crate::core::clause::{Clause, clause_matches, has_indexed_value, optional_clause_matches};
use crate::family::Hash32;
use crate::query::engine::IndexedFilter;
use crate::query::planner::{IndexedClause, build_indexed_clause};
use crate::txs::types::{Address20, Selector4};
use crate::txs::view::Tx;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TxFilter {
    pub from: Option<Clause<Address20>>,
    pub to: Option<Clause<Address20>>,
    pub selector: Option<Clause<Selector4>>,
    pub tx_hash: Option<Clause<Hash32>>,
}

impl IndexedFilter for TxFilter {
    fn max_or_terms(&self) -> usize {
        let mut max_terms = 0usize;
        if let Some(clause) = &self.from {
            max_terms = max_terms.max(clause.or_terms());
        }
        if let Some(clause) = &self.to {
            max_terms = max_terms.max(clause.or_terms());
        }
        if let Some(clause) = &self.selector {
            max_terms = max_terms.max(clause.or_terms());
        }
        if let Some(clause) = &self.tx_hash {
            max_terms = max_terms.max(clause.or_terms());
        }
        max_terms
    }

    fn has_indexed_clause(&self) -> bool {
        has_indexed_value(&self.from)
            || has_indexed_value(&self.to)
            || has_indexed_value(&self.selector)
            || has_indexed_value(&self.tx_hash)
    }

    fn indexed_clauses(&self) -> Vec<IndexedClause> {
        let mut clauses = Vec::new();

        if let Some(clause) = build_indexed_clause("tx_hash", &self.tx_hash) {
            clauses.push(clause);
        }
        if let Some(clause) = build_indexed_clause("from", &self.from) {
            clauses.push(clause);
        }
        if let Some(clause) = build_indexed_clause("to", &self.to) {
            clauses.push(clause);
        }
        if let Some(clause) = build_indexed_clause("selector", &self.selector) {
            clauses.push(clause);
        }

        clauses
    }
}

pub fn exact_match(tx: &Tx, filter: &TxFilter) -> bool {
    let tx_hash = match tx.tx_hash() {
        Ok(tx_hash) => tx_hash,
        Err(_) => return false,
    };
    if !clause_matches(tx_hash, &filter.tx_hash) {
        return false;
    }

    let sender = match tx.sender() {
        Ok(sender) => sender,
        Err(_) => return false,
    };
    if !clause_matches(sender, &filter.from) {
        return false;
    }

    let signed_tx = match tx.signed_tx() {
        Ok(signed_tx) => signed_tx,
        Err(_) => {
            return filter.to.is_none() && filter.selector.is_none();
        }
    };

    let to = match signed_tx.to_addr() {
        Ok(to) => to,
        Err(_) => return false,
    };
    if !optional_clause_matches(to, &filter.to) {
        return false;
    }

    let selector = match signed_tx.selector() {
        Ok(selector) => selector,
        Err(_) => return false,
    };
    if !optional_clause_matches(selector, &filter.selector) {
        return false;
    }

    true
}
