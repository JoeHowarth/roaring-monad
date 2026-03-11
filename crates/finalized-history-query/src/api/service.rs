use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::api::query_logs::{ExecutionBudget, FinalizedLogQueries, QueryLogsRequest};
use crate::api::write::FinalizedHistoryWriter;
use crate::config::{Config, GuardrailAction};
use crate::core::runtime::RuntimeState;
use crate::core::state::load_finalized_head_state;
use crate::domain::types::SessionId;
use crate::error::{Error, Result};
use crate::gc::worker::{GcStats, GcWorker};
use crate::ingest::engine::{IngestEngine, MaintenanceStats};
use crate::ingest::publication::{PublicationLease, new_session_id};
use crate::logs::query::LogsQueryEngine;
use crate::logs::types::{Block, HealthReport, IngestOutcome, Log};
use crate::recovery::startup::{RecoveryPlan, startup_with_writer};
use crate::store::publication::{FenceStore, PublicationStore};
use crate::store::traits::{BlobStore, MetaStore};

pub struct FinalizedHistoryService<M: MetaStore + PublicationStore + FenceStore, B: BlobStore> {
    pub ingest: IngestEngine<M, B>,
    query: LogsQueryEngine,
    writer_id: u64,
    session_id: SessionId,
    config: Config,
    state: Arc<RuntimeState>,
    startup_state: Arc<futures::lock::Mutex<Option<PublicationLease>>>,
}

impl<M: MetaStore + PublicationStore + FenceStore, B: BlobStore> FinalizedHistoryService<M, B> {
    pub fn new(config: Config, meta_store: M, blob_store: B, writer_id: u64) -> Self {
        let query = LogsQueryEngine::from_config(&config);
        let ingest = IngestEngine::new(config.clone(), meta_store, blob_store);
        Self {
            ingest,
            query,
            writer_id,
            session_id: new_session_id(writer_id),
            config,
            state: Arc::new(RuntimeState::default()),
            startup_state: Arc::new(futures::lock::Mutex::new(None)),
        }
    }

    pub async fn query_logs(
        &self,
        request: QueryLogsRequest,
        budget: ExecutionBudget,
    ) -> Result<crate::core::page::QueryPage<Log>> {
        <Self as FinalizedLogQueries>::query_logs(self, request, budget).await
    }

    pub async fn ingest_finalized_block(&self, block: Block) -> Result<IngestOutcome> {
        <Self as FinalizedHistoryWriter>::ingest_finalized_block(self, block).await
    }

    pub async fn ingest_finalized_blocks(&self, blocks: Vec<Block>) -> Result<IngestOutcome> {
        <Self as FinalizedHistoryWriter>::ingest_finalized_blocks(self, blocks).await
    }

    pub async fn startup(&self) -> Result<RecoveryPlan> {
        let mut startup_state = self.startup_state.lock().await;
        if let Some(lease) = *startup_state {
            drop(startup_state);
            return self.recovery_plan_for_lease(lease).await;
        }

        let result = startup_with_writer(
            &self.config,
            &self.ingest.meta_store,
            &self.ingest.blob_store,
            0,
            self.writer_id,
            self.session_id,
        )
        .await;
        match &result {
            Ok(_) => self.state.on_backend_success(),
            Err(Error::Backend(message)) => self.state.on_backend_error(
                message.clone(),
                self.config.backend_error_throttle_after,
                self.config.backend_error_degraded_after,
            ),
            _ => {}
        }
        let (plan, lease) = result?;
        *startup_state = Some(lease);
        Ok(plan)
    }

    pub async fn indexed_finalized_head(&self) -> Result<u64> {
        let result = load_finalized_head_state(&self.ingest.meta_store)
            .await
            .map(|state| state.indexed_finalized_head);
        self.update_backend_state(&result);
        result
    }

    pub async fn health(&self) -> HealthReport {
        let degraded = self.state.degraded.load(Ordering::Relaxed);
        let throttled = self.state.throttled.load(Ordering::Relaxed);
        let message = self.state.reason();
        HealthReport {
            healthy: !degraded && !throttled,
            degraded,
            message: if throttled {
                format!("throttled: {message}")
            } else if degraded {
                format!("degraded: {message}")
            } else {
                "ok".to_string()
            },
        }
    }

    pub async fn run_maintenance(&self) -> Result<MaintenanceStats> {
        if self.state.degraded.load(Ordering::Relaxed) {
            return Err(Error::Degraded(self.state.reason()));
        }

        let result = self.ingest.run_periodic_maintenance(self.writer_id).await;
        self.update_backend_state(&result);
        result
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
        let stats = match worker.run_once().await {
            Ok(value) => value,
            Err(error) => {
                if let Error::Backend(message) = &error {
                    self.state.on_backend_error(
                        message.clone(),
                        self.config.backend_error_throttle_after,
                        self.config.backend_error_degraded_after,
                    );
                }
                return Err(error);
            }
        };

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
        let result = worker
            .prune_block_hash_index_below(min_block_num, self.writer_id)
            .await;
        self.update_backend_state(&result);
        result
    }

    fn update_backend_state<T>(&self, result: &Result<T>) {
        match result {
            Ok(_) => self.state.on_backend_success(),
            Err(Error::Backend(message)) => self.state.on_backend_error(
                message.clone(),
                self.config.backend_error_throttle_after,
                self.config.backend_error_degraded_after,
            ),
            _ => {}
        }
    }
}

impl<M: MetaStore + PublicationStore + FenceStore, B: BlobStore> FinalizedHistoryWriter
    for FinalizedHistoryService<M, B>
{
    async fn ingest_finalized_block(&self, block: Block) -> Result<IngestOutcome> {
        self.ingest_finalized_blocks(vec![block]).await
    }

    async fn ingest_finalized_blocks(&self, blocks: Vec<Block>) -> Result<IngestOutcome> {
        if self.state.degraded.load(Ordering::Relaxed) {
            return Err(Error::Degraded(self.state.reason()));
        }
        if self.state.throttled.load(Ordering::Relaxed) {
            return Err(Error::Throttled(self.state.reason()));
        }

        let result = self.ingest_blocks_with_startup(blocks).await;
        self.update_backend_state(&result);
        if let Err(Error::InvalidParent | Error::FinalityViolation) = &result {
            self.state
                .set_degraded("finality violation or parent mismatch".to_string());
        }
        result
    }
}

impl<M: MetaStore + PublicationStore + FenceStore, B: BlobStore> FinalizedLogQueries
    for FinalizedHistoryService<M, B>
{
    async fn query_logs(
        &self,
        request: QueryLogsRequest,
        budget: ExecutionBudget,
    ) -> Result<crate::core::page::QueryPage<Log>> {
        if self.state.degraded.load(Ordering::Relaxed) {
            return Err(Error::Degraded(self.state.reason()));
        }

        let result = self
            .query
            .query_logs(
                &self.ingest.meta_store,
                &self.ingest.blob_store,
                request,
                budget,
            )
            .await;
        self.update_backend_state(&result);
        result
    }
}

impl<M: MetaStore + PublicationStore + FenceStore, B: BlobStore> FinalizedHistoryService<M, B> {
    async fn ingest_blocks_with_startup(&self, blocks: Vec<Block>) -> Result<IngestOutcome> {
        let startup_needed = {
            let startup_state = self.startup_state.lock().await;
            startup_state.is_none()
        };
        if startup_needed {
            let _ = self.startup().await?;
        }

        let lease = {
            let startup_state = self.startup_state.lock().await;
            (*startup_state).ok_or(Error::PublicationConflict)?
        };

        match self.ingest.ingest_finalized_blocks(&blocks, lease).await {
            Ok((outcome, next_lease)) => {
                *self.startup_state.lock().await = Some(next_lease);
                Ok(outcome)
            }
            Err(Error::LeaseLost) => {
                *self.startup_state.lock().await = None;
                Err(Error::LeaseLost)
            }
            Err(error) => Err(error),
        }
    }

    async fn recovery_plan_for_lease(&self, lease: PublicationLease) -> Result<RecoveryPlan> {
        let next_log_id = crate::core::state::derive_next_log_id(
            &self.ingest.meta_store,
            lease.indexed_finalized_head,
        )
        .await?;
        Ok(RecoveryPlan {
            head_state: crate::core::state::FinalizedHeadState {
                indexed_finalized_head: lease.indexed_finalized_head,
                publication_epoch: lease.epoch,
            },
            log_state: crate::logs::types::LogSequencingState {
                next_log_id: crate::core::ids::LogId::new(next_log_id),
            },
            warm_streams: 0,
        })
    }
}
