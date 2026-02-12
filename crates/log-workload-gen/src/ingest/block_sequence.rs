use crate::types::ChainEvent;
use std::collections::HashMap;

pub(crate) struct BlockSequenceValidator {
    chain_id: Option<u64>,
    end_block: Option<u64>,
    seen_blocks: HashMap<u64, [u8; 32]>,
}

impl BlockSequenceValidator {
    pub(crate) fn new() -> Self {
        Self {
            chain_id: None,
            end_block: None,
            seen_blocks: HashMap::new(),
        }
    }

    pub(crate) fn validate(&mut self, event: &ChainEvent) -> Result<AcceptKind, String> {
        if let Some(existing_chain_id) = self.chain_id {
            if existing_chain_id != event.chain_id {
                return Err("chain_id_mismatch".to_string());
            }
        } else {
            self.chain_id = Some(event.chain_id);
        }

        if let Some(existing_hash) = self.seen_blocks.get(&event.block_number) {
            if existing_hash == &event.block_hash {
                return Ok(AcceptKind::Duplicate);
            }
            return Err("block_hash_mismatch".to_string());
        }

        if let Some(last_block) = self.end_block
            && event.block_number < last_block
        {
            return Err("non_monotonic_block_number".to_string());
        }

        let prev_end = self.end_block;
        self.end_block = Some(event.block_number);
        self.seen_blocks
            .insert(event.block_number, event.block_hash);

        Ok(AcceptKind::New {
            prev_end_block: prev_end,
        })
    }

    pub(crate) fn end_block(&self) -> Option<u64> {
        self.end_block
    }
}

pub(crate) enum AcceptKind {
    Duplicate,
    New { prev_end_block: Option<u64> },
}
