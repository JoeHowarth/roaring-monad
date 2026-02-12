use crate::ingest::block_sequence::{AcceptKind, BlockSequenceValidator};
use crate::ingest::gap_tracker::GapTracker;
use crate::types::{ChainEvent, DatasetSummary};

pub(crate) enum AcceptOutcome {
    New,
    Duplicate,
}

pub(crate) struct Validator {
    sequence: BlockSequenceValidator,
    gap_tracker: GapTracker,
    start_block: Option<u64>,
    blocks_observed: u64,
    event_count: u64,
    log_count: u64,
}

impl Validator {
    pub(crate) fn new() -> Self {
        Self {
            sequence: BlockSequenceValidator::new(),
            gap_tracker: GapTracker::default(),
            start_block: None,
            blocks_observed: 0,
            event_count: 0,
            log_count: 0,
        }
    }

    pub(crate) fn accept(&mut self, event: &ChainEvent) -> Result<AcceptOutcome, String> {
        let kind = self.sequence.validate(event)?;
        let prev_end = match kind {
            AcceptKind::Duplicate => return Ok(AcceptOutcome::Duplicate),
            AcceptKind::New { prev_end_block } => prev_end_block,
        };

        self.start_block.get_or_insert(event.block_number);
        self.gap_tracker
            .observe_new_block(prev_end, event.block_number);
        self.blocks_observed += 1;
        self.event_count += 1;
        self.log_count += event.logs.len() as u64;
        Ok(AcceptOutcome::New)
    }

    pub(crate) fn summary_with_validity(
        &self,
        valid: bool,
        invalid_reason: Option<String>,
    ) -> DatasetSummary {
        DatasetSummary {
            valid,
            invalid_reason,
            start_block: self.start_block,
            end_block: self.sequence.end_block(),
            blocks_observed: self.blocks_observed,
            gap_count: self.gap_tracker.gap_count(),
            missing_block_ranges: self.gap_tracker.missing_block_ranges(),
            event_count: self.event_count,
            log_count: self.log_count,
        }
    }

    pub(crate) fn end_block(&self) -> Option<u64> {
        self.sequence.end_block()
    }
}
