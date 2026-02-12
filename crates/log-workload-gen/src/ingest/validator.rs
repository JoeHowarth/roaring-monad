use crate::types::{ChainEvent, DatasetSummary};
use std::collections::HashMap;

pub(crate) enum AcceptOutcome {
    New,
    Duplicate,
}

pub(crate) struct Validator {
    chain_id: Option<u64>,
    start_block: Option<u64>,
    end_block: Option<u64>,
    blocks_observed: u64,
    event_count: u64,
    log_count: u64,
    gap_count: u64,
    missing_block_ranges: Vec<[u64; 2]>,
    seen_blocks: HashMap<u64, [u8; 32]>,
}

impl Validator {
    pub(crate) fn new() -> Self {
        Self {
            chain_id: None,
            start_block: None,
            end_block: None,
            blocks_observed: 0,
            event_count: 0,
            log_count: 0,
            gap_count: 0,
            missing_block_ranges: Vec::new(),
            seen_blocks: HashMap::new(),
        }
    }

    pub(crate) fn accept(&mut self, event: &ChainEvent) -> Result<AcceptOutcome, String> {
        if let Some(existing_chain_id) = self.chain_id {
            if existing_chain_id != event.chain_id {
                return Err("chain_id_mismatch".to_string());
            }
        } else {
            self.chain_id = Some(event.chain_id);
        }

        if let Some(existing_hash) = self.seen_blocks.get(&event.block_number) {
            if existing_hash == &event.block_hash {
                return Ok(AcceptOutcome::Duplicate);
            }
            return Err("block_hash_mismatch".to_string());
        }

        if let Some(last_block) = self.end_block {
            if event.block_number < last_block {
                return Err("non_monotonic_block_number".to_string());
            }
            if event.block_number > last_block.saturating_add(1) {
                self.gap_count += 1;
                self.missing_block_ranges
                    .push([last_block + 1, event.block_number - 1]);
            }
        }

        self.start_block.get_or_insert(event.block_number);
        self.end_block = Some(event.block_number);
        self.blocks_observed += 1;
        self.event_count += 1;
        self.log_count += event.logs.len() as u64;
        self.seen_blocks
            .insert(event.block_number, event.block_hash);
        Ok(AcceptOutcome::New)
    }

    pub(crate) fn summary_with_validity(
        &self,
        valid: bool,
        invalid_reason: Option<String>,
    ) -> DatasetSummary {
        let missing_block_ranges = if self.missing_block_ranges.is_empty() {
            None
        } else {
            Some(
                self.missing_block_ranges
                    .iter()
                    .take(100)
                    .copied()
                    .collect(),
            )
        };

        DatasetSummary {
            valid,
            invalid_reason,
            start_block: self.start_block,
            end_block: self.end_block,
            blocks_observed: self.blocks_observed,
            gap_count: self.gap_count,
            missing_block_ranges,
            event_count: self.event_count,
            log_count: self.log_count,
        }
    }

    pub(crate) fn end_block(&self) -> Option<u64> {
        self.end_block
    }
}
