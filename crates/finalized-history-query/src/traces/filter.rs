use crate::core::clause::{Clause, clause_matches, has_indexed_value, optional_clause_matches};
use crate::query::engine::IndexedFilter;
use crate::query::planner::{IndexedClause, indexed_clause, single_selector_clause};
use crate::traces::types::{Address20, Selector4};
use crate::traces::view::CallFrameView;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TraceFilter {
    pub from: Option<Clause<Address20>>,
    pub to: Option<Clause<Address20>>,
    pub selector: Option<Clause<Selector4>>,
    pub is_top_level: Option<bool>,
    pub has_value: Option<bool>,
}

impl IndexedFilter for TraceFilter {
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
            || self.has_value == Some(true)
    }
}

impl TraceFilter {
    pub(crate) fn indexed_clauses(&self) -> Vec<IndexedClause> {
        let mut clauses = Vec::new();

        if let Some(clause) = &self.from
            && let Some(clause) = indexed_clause("from", clause.indexed_values())
        {
            clauses.push(clause);
        }

        if let Some(clause) = &self.to
            && let Some(clause) = indexed_clause("to", clause.indexed_values())
        {
            clauses.push(clause);
        }

        if let Some(clause) = &self.selector
            && let Some(clause) = indexed_clause("selector", clause.indexed_values())
        {
            clauses.push(clause);
        }

        if self.has_value == Some(true) {
            clauses.push(single_selector_clause("has_value", vec![1]));
        }

        clauses
    }

    pub fn matches_trace(&self, item: &crate::traces::types::Trace) -> bool {
        if let Some(expected) = self.is_top_level
            && (item.depth == 0) != expected
        {
            return false;
        }
        if let Some(expected) = self.has_value {
            let has_value = item.value.iter().any(|byte| *byte != 0);
            if has_value != expected {
                return false;
            }
        }
        if !clause_matches(&item.from, &self.from) {
            return false;
        }
        if !optional_clause_matches(item.to, &self.to) {
            return false;
        }
        let selector = (item.input.len() >= 4)
            .then(|| <[u8; 4]>::try_from(&item.input[..4]).expect("4-byte selector slice"));
        if !optional_clause_matches(selector, &self.selector) {
            return false;
        }
        true
    }
}

pub fn exact_match(trace: &CallFrameView<'_>, filter: &TraceFilter) -> bool {
    let from = match trace.from_addr() {
        Ok(from) => from,
        Err(_) => return false,
    };
    if !clause_matches(from, &filter.from) {
        return false;
    }

    let to = match trace.to_addr() {
        Ok(to) => to,
        Err(_) => return false,
    };
    if !optional_clause_matches(to.copied(), &filter.to) {
        return false;
    }

    let selector = match trace.selector() {
        Ok(selector) => selector,
        Err(_) => return false,
    };
    if !optional_clause_matches(selector.copied(), &filter.selector) {
        return false;
    }

    if let Some(expected) = filter.is_top_level {
        match trace.depth() {
            Ok(depth) if (depth == 0) == expected => {}
            _ => return false,
        }
    }

    if let Some(expected) = filter.has_value {
        match trace.has_value() {
            Ok(has_value) if has_value == expected => {}
            _ => return false,
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::engine::IndexedFilter;
    use crate::traces::view::BlockTraceIter;
    use alloy_rlp::Encodable;

    fn encode_field<T: alloy_rlp::Encodable>(value: T) -> Vec<u8> {
        let mut out = Vec::new();
        value.encode(&mut out);
        out
    }

    fn encode_bytes(value: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        value.encode(&mut out);
        out
    }

    fn encode_frame(input: &[u8], depth: u64, value: &[u8]) -> Vec<u8> {
        let fields = vec![
            encode_field(0u8),
            encode_field(0u64),
            encode_bytes(&[1u8; 20]),
            encode_bytes(&[2u8; 20]),
            encode_bytes(value),
            encode_field(10u64),
            encode_field(9u64),
            encode_bytes(input),
            encode_bytes(&[]),
            encode_field(1u8),
            encode_field(depth),
        ];
        let mut out = Vec::new();
        alloy_rlp::Header {
            list: true,
            payload_length: fields.iter().map(Vec::len).sum(),
        }
        .encode(&mut out);
        for field in fields {
            out.extend_from_slice(&field);
        }
        out
    }

    fn frame_view() -> CallFrameView<'static> {
        let block = {
            let frame = encode_frame(&[1, 2, 3, 4, 5], 0, &[0, 7]);
            let mut tx = Vec::new();
            alloy_rlp::Header {
                list: true,
                payload_length: frame.len(),
            }
            .encode(&mut tx);
            tx.extend_from_slice(&frame);
            let mut out = Vec::new();
            alloy_rlp::Header {
                list: true,
                payload_length: tx.len(),
            }
            .encode(&mut out);
            out.extend_from_slice(&tx);
            Box::leak(out.into_boxed_slice()) as &'static [u8]
        };
        BlockTraceIter::new(block)
            .expect("iter")
            .next()
            .expect("frame")
            .expect("iterated frame")
            .view
    }

    #[test]
    fn exact_match_accepts_matching_indexed_and_post_filters() {
        let trace = frame_view();
        let filter = TraceFilter {
            from: Some(Clause::One([1; 20])),
            to: Some(Clause::One([2; 20])),
            selector: Some(Clause::One([1, 2, 3, 4])),
            is_top_level: Some(true),
            has_value: Some(true),
        };
        assert!(exact_match(&trace, &filter));
    }

    #[test]
    fn has_indexed_clause_ignores_is_top_level_only() {
        let filter = TraceFilter {
            is_top_level: Some(true),
            ..Default::default()
        };
        assert!(!filter.has_indexed_clause());
    }
}
