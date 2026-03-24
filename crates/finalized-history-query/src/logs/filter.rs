use crate::core::clause::{Clause, clause_matches, has_indexed_value, optional_clause_matches};
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
        has_indexed_value(&self.address)
            || has_indexed_value(&self.topic0)
            || has_indexed_value(&self.topic1)
            || has_indexed_value(&self.topic2)
            || has_indexed_value(&self.topic3)
    }
}

pub fn exact_match(log: &impl crate::logs::log_ref::LogView, filter: &LogFilter) -> bool {
    if !clause_matches(log.address(), &filter.address) {
        return false;
    }

    let tc = log.topic_count();
    if !optional_clause_matches(
        if tc > 0 { Some(*log.topic(0)) } else { None },
        &filter.topic0,
    ) {
        return false;
    }
    if !optional_clause_matches(
        if tc > 1 { Some(*log.topic(1)) } else { None },
        &filter.topic1,
    ) {
        return false;
    }
    if !optional_clause_matches(
        if tc > 2 { Some(*log.topic(2)) } else { None },
        &filter.topic2,
    ) {
        return false;
    }
    if !optional_clause_matches(
        if tc > 3 { Some(*log.topic(3)) } else { None },
        &filter.topic3,
    ) {
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logs::types::Log;

    fn log_with_topics(address: u8, topics: &[u8]) -> Log {
        Log {
            address: [address; 20],
            topics: topics.iter().map(|t| [*t; 32]).collect(),
            data: vec![],
            block_num: 1,
            tx_idx: 0,
            log_idx: 0,
            block_hash: [0; 32],
        }
    }

    // --- exact_match: address clause variants ---

    #[test]
    fn exact_match_address_one_matches() {
        let log = log_with_topics(5, &[10]);
        let filter = LogFilter {
            address: Some(Clause::One([5; 20])),
            ..Default::default()
        };
        assert!(exact_match(&log, &filter));
    }

    #[test]
    fn exact_match_address_one_rejects() {
        let log = log_with_topics(5, &[10]);
        let filter = LogFilter {
            address: Some(Clause::One([9; 20])),
            ..Default::default()
        };
        assert!(!exact_match(&log, &filter));
    }

    #[test]
    fn exact_match_address_or_matches_second() {
        let log = log_with_topics(5, &[10]);
        let filter = LogFilter {
            address: Some(Clause::Or(vec![[9; 20], [5; 20]])),
            ..Default::default()
        };
        assert!(exact_match(&log, &filter));
    }

    #[test]
    fn exact_match_address_or_rejects_all() {
        let log = log_with_topics(5, &[10]);
        let filter = LogFilter {
            address: Some(Clause::Or(vec![[9; 20], [8; 20]])),
            ..Default::default()
        };
        assert!(!exact_match(&log, &filter));
    }

    #[test]
    fn exact_match_address_any_passes() {
        let log = log_with_topics(5, &[10]);
        let filter = LogFilter {
            address: Some(Clause::Any),
            ..Default::default()
        };
        assert!(exact_match(&log, &filter));
    }

    #[test]
    fn exact_match_address_none_passes() {
        let log = log_with_topics(5, &[10]);
        let filter = LogFilter::default();
        assert!(exact_match(&log, &filter));
    }

    // --- exact_match: topic clause variants ---

    #[test]
    fn exact_match_topic0_one_matches() {
        let log = log_with_topics(1, &[10, 20]);
        let filter = LogFilter {
            topic0: Some(Clause::One([10; 32])),
            ..Default::default()
        };
        assert!(exact_match(&log, &filter));
    }

    #[test]
    fn exact_match_topic0_one_rejects() {
        let log = log_with_topics(1, &[10, 20]);
        let filter = LogFilter {
            topic0: Some(Clause::One([99; 32])),
            ..Default::default()
        };
        assert!(!exact_match(&log, &filter));
    }

    #[test]
    fn exact_match_topic1_or_matches() {
        let log = log_with_topics(1, &[10, 20]);
        let filter = LogFilter {
            topic1: Some(Clause::Or(vec![[99; 32], [20; 32]])),
            ..Default::default()
        };
        assert!(exact_match(&log, &filter));
    }

    #[test]
    fn exact_match_topic2_filter_on_log_with_fewer_topics() {
        let log = log_with_topics(1, &[10]);
        let filter = LogFilter {
            topic2: Some(Clause::One([10; 32])),
            ..Default::default()
        };
        assert!(!exact_match(&log, &filter));
    }

    #[test]
    fn exact_match_topic3_filter_on_log_with_fewer_topics() {
        let log = log_with_topics(1, &[10, 20]);
        let filter = LogFilter {
            topic3: Some(Clause::One([10; 32])),
            ..Default::default()
        };
        assert!(!exact_match(&log, &filter));
    }

    #[test]
    fn exact_match_zero_topics_passes_no_topic_filter() {
        let log = log_with_topics(1, &[]);
        let filter = LogFilter {
            address: Some(Clause::One([1; 20])),
            ..Default::default()
        };
        assert!(exact_match(&log, &filter));
    }

    #[test]
    fn exact_match_zero_topics_fails_topic0_filter() {
        let log = log_with_topics(1, &[]);
        let filter = LogFilter {
            topic0: Some(Clause::One([10; 32])),
            ..Default::default()
        };
        assert!(!exact_match(&log, &filter));
    }

    #[test]
    fn exact_match_combined_address_and_topic() {
        let log = log_with_topics(5, &[10, 20]);
        let matching = LogFilter {
            address: Some(Clause::One([5; 20])),
            topic0: Some(Clause::One([10; 32])),
            topic1: Some(Clause::One([20; 32])),
            ..Default::default()
        };
        assert!(exact_match(&log, &matching));

        let wrong_topic = LogFilter {
            address: Some(Clause::One([5; 20])),
            topic0: Some(Clause::One([10; 32])),
            topic1: Some(Clause::One([99; 32])),
            ..Default::default()
        };
        assert!(!exact_match(&log, &wrong_topic));
    }

    // --- has_indexed_clause ---

    #[test]
    fn has_indexed_clause_all_none() {
        assert!(!LogFilter::default().has_indexed_clause());
    }

    #[test]
    fn has_indexed_clause_all_any() {
        let filter = LogFilter {
            address: Some(Clause::Any),
            topic0: Some(Clause::Any),
            ..Default::default()
        };
        assert!(!filter.has_indexed_clause());
    }

    #[test]
    fn has_indexed_clause_one_address() {
        let filter = LogFilter {
            address: Some(Clause::One([1; 20])),
            ..Default::default()
        };
        assert!(filter.has_indexed_clause());
    }

    #[test]
    fn has_indexed_clause_or_topic() {
        let filter = LogFilter {
            topic2: Some(Clause::Or(vec![[1; 32], [2; 32]])),
            ..Default::default()
        };
        assert!(filter.has_indexed_clause());
    }

    // --- max_or_terms ---

    #[test]
    fn max_or_terms_picks_largest() {
        let filter = LogFilter {
            address: Some(Clause::One([1; 20])),
            topic0: Some(Clause::Or(vec![[1; 32], [2; 32], [3; 32]])),
            topic1: Some(Clause::Any),
            ..Default::default()
        };
        assert_eq!(filter.max_or_terms(), 3);
    }

    #[test]
    fn max_or_terms_empty_filter() {
        assert_eq!(LogFilter::default().max_or_terms(), 0);
    }
}
