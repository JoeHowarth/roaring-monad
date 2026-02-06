#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BroadQueryPolicy {
    Error,
    BlockScan,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub target_entries_per_chunk: u32,
    pub maintenance_seal_seconds: u64,
    pub tail_flush_seconds: u64,
    pub planner_max_or_terms: usize,
    pub planner_broad_query_policy: BroadQueryPolicy,
    pub max_orphan_chunk_bytes: u64,
    pub max_orphan_manifest_segments: u64,
    pub max_stale_tail_keys: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            target_entries_per_chunk: 1950,
            maintenance_seal_seconds: 600,
            tail_flush_seconds: 5,
            planner_max_or_terms: 128,
            planner_broad_query_policy: BroadQueryPolicy::Error,
            max_orphan_chunk_bytes: 32 * 1024 * 1024 * 1024,
            max_orphan_manifest_segments: 500_000,
            max_stale_tail_keys: 1_000_000,
        }
    }
}
