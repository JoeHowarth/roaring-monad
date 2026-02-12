#[derive(Default)]
pub(crate) struct GapTracker {
    gap_count: u64,
    missing_block_ranges: Vec<[u64; 2]>,
}

impl GapTracker {
    pub(crate) fn observe_new_block(&mut self, prev_end: Option<u64>, current: u64) {
        if let Some(last_block) = prev_end
            && current > last_block.saturating_add(1)
        {
            self.gap_count += 1;
            self.missing_block_ranges
                .push([last_block + 1, current - 1]);
        }
    }

    pub(crate) fn gap_count(&self) -> u64 {
        self.gap_count
    }

    pub(crate) fn missing_block_ranges(&self) -> Option<Vec<[u64; 2]>> {
        if self.missing_block_ranges.is_empty() {
            None
        } else {
            Some(
                self.missing_block_ranges
                    .iter()
                    .take(100)
                    .copied()
                    .collect(),
            )
        }
    }
}
