use crate::block::FinalizedBlock;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StreamAppendStats {
    pub addressed: usize,
    pub topics: usize,
}

pub fn estimate_stream_appends(block: &FinalizedBlock) -> StreamAppendStats {
    let addressed = block.logs.len();
    let topics = block
        .logs
        .iter()
        .map(|l| l.topics.len().saturating_sub(1).min(3))
        .sum();
    StreamAppendStats { addressed, topics }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::FinalizedBlock;
    use crate::logs::types::Log;

    #[test]
    fn estimate_counts_address_and_topics() {
        let b = FinalizedBlock {
            block_num: 1,
            block_hash: [1; 32],
            parent_hash: [0; 32],
            logs: vec![
                Log {
                    address: [1; 20],
                    topics: vec![[0; 32], [1; 32], [2; 32]],
                    data: vec![],
                    block_num: 1,
                    tx_idx: 0,
                    log_idx: 0,
                    block_hash: [1; 32],
                },
                Log {
                    address: [2; 20],
                    topics: vec![[0; 32]],
                    data: vec![],
                    block_num: 1,
                    tx_idx: 0,
                    log_idx: 1,
                    block_hash: [1; 32],
                },
            ],
            txs: Vec::new(),
            traces: Vec::new(),
        };
        let stats = estimate_stream_appends(&b);
        assert_eq!(stats.addressed, 2);
        assert_eq!(stats.topics, 2);
    }
}
