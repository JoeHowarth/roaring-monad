use crate::types::LogEntry;
use std::collections::{HashMap, HashSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum KeyType {
    Address,
    Topic0,
    Topic1,
    Topic2,
    Topic3,
    AddressTopic0,
}

#[derive(Clone, Debug, PartialEq)]
pub struct KeyStatsRow {
    pub key_type: KeyType,
    pub key_value: Vec<u8>,
    pub count_total: u64,
    pub first_block: u64,
    pub last_block: u64,
    pub active_block_count: u64,
    pub distinct_partner_estimate: Option<f64>,
}

#[derive(Default)]
struct KeyAgg {
    count_total: u64,
    first_block: u64,
    last_block: u64,
    active_blocks: HashSet<u64>,
}

impl KeyAgg {
    fn observe(&mut self, block_number: u64) {
        self.count_total += 1;
        if self.count_total == 1 {
            self.first_block = block_number;
            self.last_block = block_number;
        } else {
            self.first_block = self.first_block.min(block_number);
            self.last_block = self.last_block.max(block_number);
        }
        self.active_blocks.insert(block_number);
    }
}

pub struct KeyStatsAccumulator {
    by_key: HashMap<(KeyType, Vec<u8>), KeyAgg>,
    partner_topic0_by_address: HashMap<Vec<u8>, HashSet<Vec<u8>>>,
    partner_address_by_topic0: HashMap<Vec<u8>, HashSet<Vec<u8>>>,
}

impl KeyStatsAccumulator {
    pub fn new() -> Self {
        Self {
            by_key: HashMap::new(),
            partner_topic0_by_address: HashMap::new(),
            partner_address_by_topic0: HashMap::new(),
        }
    }

    pub fn observe_log(&mut self, block_number: u64, log: &LogEntry) {
        let address = log.address.to_vec();
        self.observe_key(block_number, KeyType::Address, address.clone());

        for (idx, topic) in log.topics.iter().take(4).enumerate() {
            let topic_vec = topic.to_vec();
            match idx {
                0 => {
                    self.observe_key(block_number, KeyType::Topic0, topic_vec.clone());
                    self.observe_key(
                        block_number,
                        KeyType::AddressTopic0,
                        [address.clone(), topic_vec.clone()].concat(),
                    );
                    self.partner_topic0_by_address
                        .entry(address.clone())
                        .or_default()
                        .insert(topic_vec.clone());
                    self.partner_address_by_topic0
                        .entry(topic_vec)
                        .or_default()
                        .insert(address.clone());
                }
                1 => self.observe_key(block_number, KeyType::Topic1, topic_vec),
                2 => self.observe_key(block_number, KeyType::Topic2, topic_vec),
                3 => self.observe_key(block_number, KeyType::Topic3, topic_vec),
                _ => unreachable!(),
            }
        }
    }

    pub fn finalize(self) -> Vec<KeyStatsRow> {
        let mut out = Vec::with_capacity(self.by_key.len());
        for ((key_type, key_value), agg) in self.by_key {
            let distinct_partner_estimate = match key_type {
                KeyType::Address => self
                    .partner_topic0_by_address
                    .get(&key_value)
                    .map(|s| s.len() as f64),
                KeyType::Topic0 => self
                    .partner_address_by_topic0
                    .get(&key_value)
                    .map(|s| s.len() as f64),
                _ => None,
            };

            out.push(KeyStatsRow {
                key_type,
                key_value,
                count_total: agg.count_total,
                first_block: agg.first_block,
                last_block: agg.last_block,
                active_block_count: agg.active_blocks.len() as u64,
                distinct_partner_estimate,
            });
        }

        out.sort_by(|a, b| (a.key_type, &a.key_value).cmp(&(b.key_type, &b.key_value)));
        out
    }

    fn observe_key(&mut self, block_number: u64, key_type: KeyType, key_value: Vec<u8>) {
        self.by_key
            .entry((key_type, key_value))
            .or_default()
            .observe(block_number);
    }
}

impl Default for KeyStatsAccumulator {
    fn default() -> Self {
        Self::new()
    }
}
