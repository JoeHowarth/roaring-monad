use crate::types::LogEntry;
use std::collections::{BTreeMap, HashMap, HashSet};

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

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RangeMetric {
    InterarrivalSeconds,
    LogsPerBlock,
    LogsPerWindow,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangeStatsRow {
    pub metric: RangeMetric,
    pub bucket_lower: u64,
    pub bucket_upper: u64,
    pub count: u64,
    pub window_size_blocks: Option<u64>,
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

pub struct RangeStatsAccumulator {
    window_size_blocks: u64,
    logs_per_block_hist: BTreeMap<(u64, u64), u64>,
    interarrival_hist: BTreeMap<(u64, u64), u64>,
    block_logs: HashMap<u64, u64>,
    prev_timestamp: Option<u64>,
}

impl RangeStatsAccumulator {
    pub fn new(window_size_blocks: u64) -> Self {
        Self {
            window_size_blocks,
            logs_per_block_hist: BTreeMap::new(),
            interarrival_hist: BTreeMap::new(),
            block_logs: HashMap::new(),
            prev_timestamp: None,
        }
    }

    pub fn observe_block(&mut self, block_number: u64, log_count: u64, timestamp: u64) {
        self.block_logs.insert(block_number, log_count);
        inc_bucket(&mut self.logs_per_block_hist, log_count);

        if timestamp > 0 {
            if let Some(prev) = self.prev_timestamp
                && timestamp >= prev
            {
                inc_bucket(&mut self.interarrival_hist, timestamp - prev);
            }
            self.prev_timestamp = Some(timestamp);
        }
    }

    pub fn finalize(self, start_block: u64, end_block: u64) -> Vec<RangeStatsRow> {
        let mut out = Vec::new();

        for ((lower, upper), count) in self.logs_per_block_hist {
            out.push(RangeStatsRow {
                metric: RangeMetric::LogsPerBlock,
                bucket_lower: lower,
                bucket_upper: upper,
                count,
                window_size_blocks: None,
            });
        }

        let mut logs_per_window_hist: BTreeMap<(u64, u64), u64> = BTreeMap::new();
        let mut window_start = start_block;
        while window_start <= end_block {
            let window_end = (window_start + self.window_size_blocks - 1).min(end_block);
            let mut total = 0u64;
            for b in window_start..=window_end {
                total += self.block_logs.get(&b).copied().unwrap_or(0);
            }
            inc_bucket(&mut logs_per_window_hist, total);
            if window_end == u64::MAX {
                break;
            }
            window_start = window_end + 1;
        }

        for ((lower, upper), count) in logs_per_window_hist {
            out.push(RangeStatsRow {
                metric: RangeMetric::LogsPerWindow,
                bucket_lower: lower,
                bucket_upper: upper,
                count,
                window_size_blocks: Some(self.window_size_blocks),
            });
        }

        for ((lower, upper), count) in self.interarrival_hist {
            out.push(RangeStatsRow {
                metric: RangeMetric::InterarrivalSeconds,
                bucket_lower: lower,
                bucket_upper: upper,
                count,
                window_size_blocks: None,
            });
        }

        out.sort_by(|a, b| {
            (a.metric, a.bucket_lower, a.bucket_upper).cmp(&(
                b.metric,
                b.bucket_lower,
                b.bucket_upper,
            ))
        });
        out
    }
}

fn inc_bucket(hist: &mut BTreeMap<(u64, u64), u64>, value: u64) {
    let (lower, upper) = log2_bucket(value);
    *hist.entry((lower, upper)).or_insert(0) += 1;
}

fn log2_bucket(v: u64) -> (u64, u64) {
    if v == 0 {
        return (0, 1);
    }
    let exp = 63 - v.leading_zeros() as u64;
    let lower = 1u64 << exp;
    let upper = lower.saturating_mul(2);
    (lower, upper)
}
