use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct Counters {
    pub ingest_blocks: AtomicU64,
    pub query_requests: AtomicU64,
    pub cas_conflicts: AtomicU64,
}

impl Counters {
    pub fn inc_ingest(&self) {
        let _ = self.ingest_blocks.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_query(&self) {
        let _ = self.query_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_cas_conflict(&self) {
        let _ = self.cas_conflicts.fetch_add(1, Ordering::Relaxed);
    }
}
