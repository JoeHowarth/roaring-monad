use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::config::{Config, GuardrailAction};
use crate::domain::filter::{LogFilter, QueryOptions};
use crate::domain::types::{Block, HealthReport, IngestOutcome, Log};
use crate::error::{Error, Result};
use crate::gc::worker::{GcStats, GcWorker};
use crate::ingest::engine::{IngestEngine, MaintenanceStats};
use crate::query::engine::QueryEngine;
use crate::store::traits::{BlobStore, MetaStore};

#[async_trait::async_trait]
pub trait FinalizedLogIndex: Send + Sync {
    async fn ingest_finalized_block(&self, block: Block) -> Result<IngestOutcome>;
    async fn query_finalized(&self, filter: LogFilter, options: QueryOptions) -> Result<Vec<Log>>;
    async fn indexed_finalized_head(&self) -> Result<u64>;
    async fn health(&self) -> HealthReport;
}

#[derive(Debug, Default)]
struct RuntimeState {
    degraded: AtomicBool,
    throttled: AtomicBool,
    reason: Mutex<String>,
}

impl RuntimeState {
    fn set_degraded(&self, reason: impl Into<String>) {
        self.degraded.store(true, Ordering::Relaxed);
        self.throttled.store(false, Ordering::Relaxed);
        if let Ok(mut r) = self.reason.lock() {
            *r = reason.into();
        }
    }

    fn set_throttled(&self, reason: impl Into<String>) {
        self.throttled.store(true, Ordering::Relaxed);
        if let Ok(mut r) = self.reason.lock() {
            *r = reason.into();
        }
    }

    fn clear_throttle(&self) {
        self.throttled.store(false, Ordering::Relaxed);
        if !self.degraded.load(Ordering::Relaxed)
            && let Ok(mut r) = self.reason.lock()
        {
            r.clear();
        }
    }

    fn reason(&self) -> String {
        self.reason
            .lock()
            .map(|g| g.clone())
            .unwrap_or_else(|_| "lock poisoned".to_string())
    }
}

pub struct FinalizedIndexService<M: MetaStore, B: BlobStore> {
    pub ingest: IngestEngine<M, B>,
    pub query: QueryEngine,
    pub writer_epoch: u64,
    pub config: Config,
    state: Arc<RuntimeState>,
}

impl<M: MetaStore, B: BlobStore> FinalizedIndexService<M, B> {
    pub fn new(config: Config, meta_store: M, blob_store: B, writer_epoch: u64) -> Self {
        let query = QueryEngine::from_config(&config);
        let ingest = IngestEngine::new(config.clone(), meta_store, blob_store);
        Self {
            ingest,
            query,
            writer_epoch,
            config,
            state: Arc::new(RuntimeState::default()),
        }
    }

    pub async fn run_maintenance(&self) -> Result<MaintenanceStats> {
        if self.state.degraded.load(Ordering::Relaxed) {
            return Err(Error::Degraded(self.state.reason()));
        }
        self.ingest
            .run_periodic_maintenance(self.writer_epoch)
            .await
    }

    pub async fn run_gc_once(&self) -> Result<GcStats> {
        if self.state.degraded.load(Ordering::Relaxed) {
            return Err(Error::Degraded(self.state.reason()));
        }

        let worker = GcWorker::new(
            &self.ingest.meta_store,
            &self.ingest.blob_store,
            &self.config,
        );
        let stats = worker.run_once().await?;

        if stats.exceeded_guardrail {
            match self.config.gc_guardrail_action {
                GuardrailAction::FailClosed => {
                    self.state
                        .set_degraded("gc guardrail exceeded; fail-closed".to_string());
                }
                GuardrailAction::Throttle => {
                    self.state
                        .set_throttled("gc guardrail exceeded; throttled".to_string());
                }
            }
        } else {
            self.state.clear_throttle();
        }

        Ok(stats)
    }

    pub async fn prune_block_hash_index_below(&self, min_block_num: u64) -> Result<u64> {
        if self.state.degraded.load(Ordering::Relaxed) {
            return Err(Error::Degraded(self.state.reason()));
        }
        let worker = GcWorker::new(
            &self.ingest.meta_store,
            &self.ingest.blob_store,
            &self.config,
        );
        worker
            .prune_block_hash_index_below(min_block_num, self.writer_epoch)
            .await
    }
}

#[async_trait::async_trait]
impl<M: MetaStore, B: BlobStore> FinalizedLogIndex for FinalizedIndexService<M, B> {
    async fn ingest_finalized_block(&self, block: Block) -> Result<IngestOutcome> {
        if self.state.degraded.load(Ordering::Relaxed) {
            return Err(Error::Degraded(self.state.reason()));
        }
        if self.state.throttled.load(Ordering::Relaxed) {
            return Err(Error::Throttled(self.state.reason()));
        }

        let res = self
            .ingest
            .ingest_finalized_block(&block, self.writer_epoch)
            .await;
        if let Err(Error::InvalidParent | Error::FinalityViolation) = &res {
            self.state
                .set_degraded("finality violation or parent mismatch".to_string());
        }
        res
    }

    async fn query_finalized(&self, filter: LogFilter, options: QueryOptions) -> Result<Vec<Log>> {
        if self.state.degraded.load(Ordering::Relaxed) {
            return Err(Error::Degraded(self.state.reason()));
        }
        self.query
            .query_finalized(
                &self.ingest.meta_store,
                &self.ingest.blob_store,
                filter,
                options,
            )
            .await
    }

    async fn indexed_finalized_head(&self) -> Result<u64> {
        use crate::codec::log::decode_meta_state;
        use crate::domain::keys::META_STATE_KEY;

        let state = self.ingest.meta_store.get(META_STATE_KEY).await?;
        match state {
            Some(r) => Ok(decode_meta_state(&r.value)?.indexed_finalized_head),
            None => Ok(0),
        }
    }

    async fn health(&self) -> HealthReport {
        let degraded = self.state.degraded.load(Ordering::Relaxed);
        let throttled = self.state.throttled.load(Ordering::Relaxed);
        let msg = self.state.reason();
        HealthReport {
            healthy: !degraded && !throttled,
            degraded,
            message: if throttled {
                format!("throttled: {msg}")
            } else if degraded {
                format!("degraded: {msg}")
            } else {
                "ok".to_string()
            },
        }
    }
}
