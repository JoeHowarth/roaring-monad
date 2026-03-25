use crate::core::clause::{Clause, clause_matches, has_indexed_value, optional_clause_matches};
use crate::query::engine::IndexedFilter;
use crate::query::planner::{IndexedClause, build_indexed_clause};
use crate::txs::types::{Address20, Selector4};
use crate::txs::view::TxRef;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TxFilter {
    pub from: Option<Clause<Address20>>,
    pub to: Option<Clause<Address20>>,
    pub selector: Option<Clause<Selector4>>,
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
        max_terms
    }

    fn has_indexed_clause(&self) -> bool {
        has_indexed_value(&self.from)
            || has_indexed_value(&self.to)
            || has_indexed_value(&self.selector)
    }

    fn indexed_clauses(&self) -> Vec<IndexedClause> {
        let mut clauses = Vec::new();

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

pub fn exact_match(tx: &TxRef, filter: &TxFilter) -> bool {
    let sender = match tx.sender() {
        Ok(sender) => sender,
        Err(_) => return false,
    };
    if !clause_matches(sender, &filter.from) {
        return false;
    }

    let to = match tx.to_addr() {
        Ok(to) => to,
        Err(_) => {
            return filter.to.is_none() && filter.selector.is_none();
        }
    };
    if !optional_clause_matches(to, &filter.to) {
        return false;
    }

    let selector = match tx.selector() {
        Ok(selector) => selector,
        Err(_) => return false,
    };
    if !optional_clause_matches(selector, &filter.selector) {
        return false;
    }

    true
}

#[cfg(test)]
mod tests {
    use alloy_rlp::{Encodable, Header};
    use bytes::Bytes;

    use crate::core::clause::Clause;

    use super::{TxFilter, exact_match};
    use crate::txs::view::TxRef;

    fn encode_field<T: Encodable>(value: T) -> Vec<u8> {
        let mut out = Vec::new();
        value.encode(&mut out);
        out
    }

    fn encode_bytes(value: &[u8]) -> Vec<u8> {
        encode_field(value)
    }

    fn encode_tx_list(fields: Vec<Vec<u8>>) -> Vec<u8> {
        let mut out = Vec::new();
        Header {
            list: true,
            payload_length: fields.iter().map(Vec::len).sum(),
        }
        .encode(&mut out);
        for field in fields {
            out.extend_from_slice(&field);
        }
        out
    }

    fn encode_envelope(sender: [u8; 20], signed_tx_bytes: Vec<u8>) -> TxRef {
        let envelope = encode_tx_list(vec![
            encode_bytes(&[1u8; 32]),
            encode_bytes(&sender),
            encode_bytes(&signed_tx_bytes),
        ]);
        TxRef::new(7, [9u8; 32], 0, Bytes::from(envelope)).expect("valid tx ref")
    }

    fn encode_legacy_tx(to: Option<[u8; 20]>, input: &[u8]) -> Vec<u8> {
        encode_tx_list(vec![
            encode_field(1u64),
            encode_field(2u64),
            encode_field(21_000u64),
            encode_bytes(to.as_ref().map(<[u8; 20]>::as_slice).unwrap_or(&[])),
            encode_field(3u64),
            encode_bytes(input),
            encode_field(27u8),
            encode_field(1u8),
            encode_field(2u8),
        ])
    }

    #[test]
    fn exact_match_accepts_create_tx_only_when_to_and_selector_filters_are_absent() {
        let tx = encode_envelope([2u8; 20], encode_legacy_tx(None, &[0xaa, 0xbb, 0xcc, 0xdd]));

        assert!(exact_match(
            &tx,
            &TxFilter {
                from: Some(Clause::One([2u8; 20])),
                ..TxFilter::default()
            }
        ));
        assert!(!exact_match(
            &tx,
            &TxFilter {
                to: Some(Clause::One([3u8; 20])),
                ..TxFilter::default()
            }
        ));
        assert!(!exact_match(
            &tx,
            &TxFilter {
                selector: Some(Clause::One([0xaa, 0xbb, 0xcc, 0xdd])),
                ..TxFilter::default()
            }
        ));
    }

    #[test]
    fn exact_match_rejects_tx_without_selector_when_selector_is_required() {
        let tx = encode_envelope(
            [2u8; 20],
            encode_legacy_tx(Some([3u8; 20]), &[0xaa, 0xbb, 0xcc]),
        );

        assert!(!exact_match(
            &tx,
            &TxFilter {
                selector: Some(Clause::One([0xaa, 0xbb, 0xcc, 0xdd])),
                ..TxFilter::default()
            }
        ));
    }
}
