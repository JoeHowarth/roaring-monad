use crate::types::LogEntry;
use std::collections::HashMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PairType {
    AddressTopic0,
    Topic0Topic1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CooccurrenceRow {
    pub pair_type: PairType,
    pub left_key: Vec<u8>,
    pub right_key: Vec<u8>,
    pub count_total: u64,
    pub first_block: u64,
    pub last_block: u64,
}

type PairKey = (PairType, Vec<u8>, Vec<u8>);
type PairAgg = (u64, u64, u64);

pub struct CooccurrenceAccumulator {
    top_k_per_type: usize,
    by_pair: HashMap<PairKey, PairAgg>,
}

impl CooccurrenceAccumulator {
    pub fn new(top_k_per_type: u64) -> Self {
        Self {
            top_k_per_type: top_k_per_type as usize,
            by_pair: HashMap::new(),
        }
    }

    pub fn observe_log(&mut self, block_number: u64, log: &LogEntry) {
        if let Some(topic0) = log.topics.first() {
            self.observe_pair(
                PairType::AddressTopic0,
                log.address.to_vec(),
                topic0.to_vec(),
                block_number,
            );
        }
        if log.topics.len() >= 2 {
            self.observe_pair(
                PairType::Topic0Topic1,
                log.topics[0].to_vec(),
                log.topics[1].to_vec(),
                block_number,
            );
        }
    }

    pub fn finalize(self) -> Vec<CooccurrenceRow> {
        let mut grouped: HashMap<PairType, Vec<CooccurrenceRow>> = HashMap::new();
        for ((pair_type, left_key, right_key), (count_total, first_block, last_block)) in
            self.by_pair
        {
            grouped.entry(pair_type).or_default().push(CooccurrenceRow {
                pair_type,
                left_key,
                right_key,
                count_total,
                first_block,
                last_block,
            });
        }

        let mut out = Vec::new();
        for pair_type in [PairType::AddressTopic0, PairType::Topic0Topic1] {
            if let Some(mut rows) = grouped.remove(&pair_type) {
                rows.sort_by(|a, b| {
                    b.count_total
                        .cmp(&a.count_total)
                        .then_with(|| a.left_key.cmp(&b.left_key))
                        .then_with(|| a.right_key.cmp(&b.right_key))
                });
                out.extend(rows.into_iter().take(self.top_k_per_type));
            }
        }
        out
    }

    fn observe_pair(
        &mut self,
        pair_type: PairType,
        left_key: Vec<u8>,
        right_key: Vec<u8>,
        block: u64,
    ) {
        let entry = self
            .by_pair
            .entry((pair_type, left_key, right_key))
            .or_insert((0, block, block));
        entry.0 += 1;
        entry.1 = entry.1.min(block);
        entry.2 = entry.2.max(block);
    }
}
