#[derive(Debug, Default)]
pub struct GcStats {
    pub orphan_chunk_bytes: u64,
    pub orphan_manifest_segments: u64,
    pub stale_tail_keys: u64,
}

#[derive(Debug, Default)]
pub struct GcWorker;

impl GcWorker {
    pub async fn run_once(&self) -> GcStats {
        GcStats::default()
    }
}
