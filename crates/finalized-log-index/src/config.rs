#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BroadQueryPolicy {
    Error,
    BlockScan,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuardrailAction {
    Throttle,
    FailClosed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestMode {
    StrictCas,
    SingleWriterFast,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub target_entries_per_chunk: u32,
    pub target_chunk_bytes: usize,
    pub maintenance_seal_seconds: u64,
    pub tail_flush_seconds: u64,
    pub planner_max_or_terms: usize,
    pub planner_broad_query_policy: BroadQueryPolicy,
    pub gc_guardrail_action: GuardrailAction,
    pub max_orphan_chunk_bytes: u64,
    pub max_orphan_manifest_segments: u64,
    pub max_stale_tail_keys: u64,
    pub backend_error_throttle_after: u64,
    pub backend_error_degraded_after: u64,
    pub ingest_mode: IngestMode,
    pub topic0_stats_flush_interval_blocks: u64,
    pub assume_empty_streams: bool,
    pub log_locator_write_concurrency: usize,
    pub stream_append_concurrency: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            target_entries_per_chunk: 1950,
            target_chunk_bytes: 32 * 1024,
            maintenance_seal_seconds: 600,
            tail_flush_seconds: 5,
            planner_max_or_terms: 128,
            planner_broad_query_policy: BroadQueryPolicy::Error,
            gc_guardrail_action: GuardrailAction::FailClosed,
            max_orphan_chunk_bytes: 32 * 1024 * 1024 * 1024,
            max_orphan_manifest_segments: 500_000,
            max_stale_tail_keys: 1_000_000,
            backend_error_throttle_after: 3,
            backend_error_degraded_after: 10,
            ingest_mode: IngestMode::StrictCas,
            topic0_stats_flush_interval_blocks: 1,
            assume_empty_streams: false,
            log_locator_write_concurrency: 256,
            stream_append_concurrency: 64,
        }
    }
}
