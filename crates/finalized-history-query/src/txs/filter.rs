use crate::core::clause::Clause;
use crate::family::Hash32;
use crate::query::engine::IndexedFilter;
use crate::query::planner::IndexedClause;
use crate::txs::types::{Address20, Selector4};
use crate::txs::view::TxView;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TxFilter {
    pub from: Option<Clause<Address20>>,
    pub to: Option<Clause<Address20>>,
    pub selector: Option<Clause<Selector4>>,
    pub tx_hash: Option<Clause<Hash32>>,
}

impl IndexedFilter for TxFilter {
    fn max_or_terms(&self) -> usize {
        todo!("tx indexed OR-term planning is not implemented")
    }

    fn has_indexed_clause(&self) -> bool {
        todo!("tx indexed-clause detection is not implemented")
    }

    fn indexed_clauses(&self) -> Vec<IndexedClause> {
        todo!("tx indexed-clause planning is not implemented")
    }
}

pub fn exact_match(_tx: &TxView<'_>, _filter: &TxFilter) -> bool {
    todo!("tx exact-match filtering is not implemented")
}
