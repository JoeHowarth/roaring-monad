use std::collections::{BTreeMap, HashMap};

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
