use std::fmt;
use std::sync::Arc;

use crate::cache::BytesCacheConfig;

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

#[derive(Clone)]
pub struct Config {
    pub observe_upstream_finalized_block: Arc<dyn Fn() -> Option<u64> + Send + Sync>,
    pub publication_lease_blocks: u64,
    pub publication_lease_renew_threshold_blocks: u64,
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
    pub bytes_cache: BytesCacheConfig,
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("observe_upstream_finalized_block", &"<callback>")
            .field("publication_lease_blocks", &self.publication_lease_blocks)
            .field(
                "publication_lease_renew_threshold_blocks",
                &self.publication_lease_renew_threshold_blocks,
            )
            .field("planner_max_or_terms", &self.planner_max_or_terms)
            .field("gc_guardrail_action", &self.gc_guardrail_action)
            .field("max_orphan_chunk_bytes", &self.max_orphan_chunk_bytes)
            .field(
                "max_orphan_manifest_segments",
                &self.max_orphan_manifest_segments,
            )
            .field("max_stale_tail_keys", &self.max_stale_tail_keys)
            .field(
                "backend_error_throttle_after",
                &self.backend_error_throttle_after,
            )
            .field(
                "backend_error_degraded_after",
                &self.backend_error_degraded_after,
            )
            .field("ingest_mode", &self.ingest_mode)
            .field("assume_empty_streams", &self.assume_empty_streams)
            .field("stream_append_concurrency", &self.stream_append_concurrency)
            .field("bytes_cache", &self.bytes_cache)
            .finish()
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            observe_upstream_finalized_block: Arc::new(|| None),
            publication_lease_blocks: 10,
            publication_lease_renew_threshold_blocks: 2,
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
            bytes_cache: BytesCacheConfig::default(),
        }
    }
}
