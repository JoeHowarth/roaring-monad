use crate::core::clause::{Clause, clause_matches, has_indexed_value, optional_clause_matches};
use crate::query::engine::IndexedFilter;
use crate::query::planner::{IndexedClause, build_indexed_clause, single_selector_clause};
use crate::traces::types::{Address20, Selector4, Trace};
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

    fn indexed_clauses(&self) -> Vec<IndexedClause> {
        let mut clauses = Vec::new();

        if let Some(clause) = build_indexed_clause("from", &self.from) {
            clauses.push(clause)
        }
        if let Some(clause) = build_indexed_clause("to", &self.to) {
            clauses.push(clause)
        }
        if let Some(clause) = build_indexed_clause("selector", &self.selector) {
            clauses.push(clause)
        }
        if self.has_value == Some(true) {
            clauses.push(single_selector_clause("has_value", vec![1]));
        }

        clauses
    }
}

impl TraceFilter {
    pub fn matches_trace(&self, item: &Trace) -> bool {
        matches_trace_fields(self, &trace_fields_for_owned_trace(item))
    }
}

#[derive(Clone, Copy)]
struct TraceMatchFields {
    from: Address20,
    to: Option<Address20>,
    selector: Option<Selector4>,
    is_top_level: bool,
    has_value: bool,
}

fn selector_for_owned_trace(item: &Trace) -> Option<Selector4> {
    if !is_call_type(item.typ, item.flags) || item.input.len() < 4 {
        return None;
    }
    Some(<[u8; 4]>::try_from(&item.input[..4]).expect("4-byte selector slice"))
}

fn is_call_type(typ: u8, flags: u64) -> bool {
    matches!((typ, flags), (0, 0 | 1) | (1, _) | (2, _))
}

fn trace_fields_for_owned_trace(item: &Trace) -> TraceMatchFields {
    TraceMatchFields {
        from: item.from,
        to: item.to,
        selector: selector_for_owned_trace(item),
        is_top_level: item.depth == 0,
        has_value: item.value.iter().any(|byte| *byte != 0),
    }
}

fn trace_fields_for_view(trace: &CallFrameView<'_>) -> Option<TraceMatchFields> {
    Some(TraceMatchFields {
        from: *trace.from_addr().ok()?,
        to: trace.to_addr().ok()?.copied(),
        selector: trace.selector().ok()?.copied(),
        is_top_level: trace.depth().ok()? == 0,
        has_value: trace.has_value().ok()?,
    })
}

fn matches_trace_fields(filter: &TraceFilter, fields: &TraceMatchFields) -> bool {
    if let Some(expected) = filter.is_top_level
        && fields.is_top_level != expected
    {
        return false;
    }
    if let Some(expected) = filter.has_value
        && fields.has_value != expected
    {
        return false;
    }
    clause_matches(&fields.from, &filter.from)
        && optional_clause_matches(fields.to, &filter.to)
        && optional_clause_matches(fields.selector, &filter.selector)
}

pub fn exact_match_frame(trace: &CallFrameView<'_>, filter: &TraceFilter) -> bool {
    trace_fields_for_view(trace)
        .map(|fields| matches_trace_fields(filter, &fields))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::engine::IndexedFilter;
    use crate::traces::ingest_iter::BlockTraceIter;
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

    #[derive(Clone, Copy, Default)]
    struct TraceFrameParts<'a> {
        typ: u8,
        flags: u64,
        input: &'a [u8],
        depth: u64,
        value: &'a [u8],
    }

    fn encode_frame(parts: TraceFrameParts<'_>) -> Vec<u8> {
        let fields = vec![
            encode_field(parts.typ),
            encode_field(parts.flags),
            encode_bytes(&[1u8; 20]),
            encode_bytes(&[2u8; 20]),
            encode_bytes(parts.value),
            encode_field(10u64),
            encode_field(9u64),
            encode_bytes(parts.input),
            encode_bytes(&[]),
            encode_field(1u8),
            encode_field(parts.depth),
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
        frame_view_with_parts(TraceFrameParts {
            input: &[1, 2, 3, 4, 5],
            value: &[0, 7],
            ..Default::default()
        })
    }

    fn frame_view_with_parts(parts: TraceFrameParts<'_>) -> CallFrameView<'static> {
        let block = {
            let frame = encode_frame(parts);
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
        assert!(exact_match_frame(&trace, &filter));
    }

    #[test]
    fn has_indexed_clause_ignores_is_top_level_only() {
        let filter = TraceFilter {
            is_top_level: Some(true),
            ..Default::default()
        };
        assert!(!filter.has_indexed_clause());
    }

    #[test]
    fn selector_policy_matches_between_owned_and_view_for_non_call_frames() {
        let filter = TraceFilter {
            selector: Some(Clause::One([1, 2, 3, 4])),
            ..Default::default()
        };
        let owned_trace = crate::traces::types::Trace {
            block_num: 7,
            block_hash: [9u8; 32],
            tx_idx: 0,
            trace_idx: 0,
            typ: 3,
            flags: 0,
            from: [1u8; 20],
            to: Some([2u8; 20]),
            value: vec![0, 7],
            gas: 10,
            gas_used: 9,
            input: vec![1, 2, 3, 4, 5],
            output: Vec::new(),
            status: 1,
            depth: 0,
        };
        let view = frame_view_with_parts(TraceFrameParts {
            typ: 3,
            input: &[1, 2, 3, 4, 5],
            value: &[0, 7],
            ..Default::default()
        });

        assert_eq!(
            filter.matches_trace(&owned_trace),
            exact_match_frame(&view, &filter)
        );
        assert!(!exact_match_frame(&view, &filter));
    }
}
