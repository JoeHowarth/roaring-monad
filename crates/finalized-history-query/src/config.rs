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
    pub now_ms: fn() -> u64,
    pub publication_lease_duration_ms: u64,
    pub publication_lease_renew_skew_ms: u64,
    pub target_entries_per_chunk: u32,
    pub target_chunk_bytes: usize,
    pub maintenance_seal_seconds: u64,
    pub tail_flush_seconds: u64,
    pub planner_max_or_terms: usize,
    pub gc_guardrail_action: GuardrailAction,
    pub max_orphan_chunk_bytes: u64,
    pub max_orphan_manifest_segments: u64,
    pub max_stale_tail_keys: u64,
    pub backend_error_throttle_after: u64,
    pub backend_error_degraded_after: u64,
    pub ingest_mode: IngestMode,
    pub assume_empty_streams: bool,
    pub stream_append_concurrency: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            now_ms: crate::time::current_time_ms,
            publication_lease_duration_ms: 30_000,
            publication_lease_renew_skew_ms: 5_000,
            target_entries_per_chunk: 1950,
            target_chunk_bytes: 32 * 1024,
            maintenance_seal_seconds: 600,
            tail_flush_seconds: 5,
            planner_max_or_terms: 128,
            gc_guardrail_action: GuardrailAction::FailClosed,
            max_orphan_chunk_bytes: 32 * 1024 * 1024 * 1024,
            max_orphan_manifest_segments: 500_000,
            max_stale_tail_keys: 1_000_000,
            backend_error_throttle_after: 3,
            backend_error_degraded_after: 10,
            ingest_mode: IngestMode::StrictCas,
            assume_empty_streams: false,
            stream_append_concurrency: 96,
        }
    }
}
