use crate::core::clause::Clause;
use crate::logs::types::{Address20, Topic32};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LogFilter {
    pub address: Option<Clause<Address20>>,
    pub topic0: Option<Clause<Topic32>>,
    pub topic1: Option<Clause<Topic32>>,
    pub topic2: Option<Clause<Topic32>>,
    pub topic3: Option<Clause<Topic32>>,
}

impl LogFilter {
    pub fn max_or_terms(&self) -> usize {
        let mut max_terms = 0usize;
        if let Some(clause) = &self.address {
            max_terms = max_terms.max(clause.or_terms());
        }
        if let Some(clause) = &self.topic0 {
            max_terms = max_terms.max(clause.or_terms());
        }
        if let Some(clause) = &self.topic1 {
            max_terms = max_terms.max(clause.or_terms());
        }
        if let Some(clause) = &self.topic2 {
            max_terms = max_terms.max(clause.or_terms());
        }
        if let Some(clause) = &self.topic3 {
            max_terms = max_terms.max(clause.or_terms());
        }
        max_terms
    }

    pub fn has_indexed_clause(&self) -> bool {
        has_indexed_value_20(&self.address)
            || has_indexed_value_32(&self.topic0)
            || has_indexed_value_32(&self.topic1)
            || has_indexed_value_32(&self.topic2)
            || has_indexed_value_32(&self.topic3)
    }
}

pub fn exact_match(log: &crate::logs::types::Log, filter: &LogFilter) -> bool {
    if !match_address(&log.address, &filter.address) {
        return false;
    }

    let topics = &log.topics;
    if !match_topic(topics.first().copied(), &filter.topic0) {
        return false;
    }
    if !match_topic(topics.get(1).copied(), &filter.topic1) {
        return false;
    }
    if !match_topic(topics.get(2).copied(), &filter.topic2) {
        return false;
    }
    if !match_topic(topics.get(3).copied(), &filter.topic3) {
        return false;
    }
    true
}

fn match_address(address: &[u8; 20], clause: &Option<Clause<[u8; 20]>>) -> bool {
    match clause {
        None | Some(Clause::Any) => true,
        Some(Clause::One(value)) => value == address,
        Some(Clause::Or(values)) => values.iter().any(|value| value == address),
    }
}

fn match_topic(topic: Option<Topic32>, clause: &Option<Clause<Topic32>>) -> bool {
    match clause {
        None | Some(Clause::Any) => true,
        Some(Clause::One(value)) => topic.as_ref() == Some(value),
        Some(Clause::Or(values)) => topic
            .as_ref()
            .map(|actual| values.iter().any(|value| value == actual))
            .unwrap_or(false),
    }
}

fn has_indexed_value_20(clause: &Option<Clause<[u8; 20]>>) -> bool {
    matches!(clause, Some(Clause::One(_) | Clause::Or(_)))
}

fn has_indexed_value_32(clause: &Option<Clause<Topic32>>) -> bool {
    matches!(clause, Some(Clause::One(_) | Clause::Or(_)))
}
