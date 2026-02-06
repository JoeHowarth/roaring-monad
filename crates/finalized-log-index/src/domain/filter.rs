use crate::domain::types::{Address20, Hash32, Topic32};

#[derive(Debug, Clone)]
pub enum Clause<T> {
    Any,
    One(T),
    Or(Vec<T>),
}

impl<T> Clause<T> {
    pub fn or_terms(&self) -> usize {
        match self {
            Self::Any => 0,
            Self::One(_) => 1,
            Self::Or(v) => v.len(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct QueryOptions {
    pub max_results: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct LogFilter {
    pub from_block: Option<u64>,
    pub to_block: Option<u64>,
    pub block_hash: Option<Hash32>,
    pub address: Option<Clause<Address20>>,
    pub topic0: Option<Clause<Topic32>>,
    pub topic1: Option<Clause<Topic32>>,
    pub topic2: Option<Clause<Topic32>>,
    pub topic3: Option<Clause<Topic32>>,
}

impl LogFilter {
    pub fn is_block_hash_mode(&self) -> bool {
        self.block_hash.is_some()
    }

    pub fn max_or_terms(&self) -> usize {
        let mut max_terms = 0usize;
        if let Some(c) = &self.address {
            max_terms = max_terms.max(c.or_terms());
        }
        if let Some(c) = &self.topic0 {
            max_terms = max_terms.max(c.or_terms());
        }
        if let Some(c) = &self.topic1 {
            max_terms = max_terms.max(c.or_terms());
        }
        if let Some(c) = &self.topic2 {
            max_terms = max_terms.max(c.or_terms());
        }
        if let Some(c) = &self.topic3 {
            max_terms = max_terms.max(c.or_terms());
        }
        max_terms
    }
}
