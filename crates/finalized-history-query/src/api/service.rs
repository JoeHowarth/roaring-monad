use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::api::query_logs::{ExecutionBudget, FinalizedLogQueries, QueryLogsRequest};
use crate::api::write::FinalizedHistoryWriter;
use crate::cache::{BytesCacheMetrics, HashMapBytesCache};
use crate::config::{Config, GuardrailAction};
use crate::core::runtime::RuntimeState;
use crate::core::state::{derive_next_log_id, load_finalized_head_state};
use crate::error::{Error, Result};
use crate::gc::worker::{GcStats, GcWorker};
use crate::ingest::authority::{
    LeaseAuthority, ReadOnlyAuthority, SingleWriterAuthority, WriteAuthority, WriteToken,
};
use crate::ingest::engine::{IngestEngine, MaintenanceStats};
use crate::ingest::open_pages::repair_open_stream_page_markers;
use crate::ingest::recovery::cleanup_unpublished_suffix;
use crate::logs::query::LogsQueryEngine;
use crate::logs::types::{Block, HealthReport, IngestOutcome, Log};
use crate::recovery::startup::{RecoveryPlan, build_recovery_plan, startup_plan};
use crate::store::publication::{FenceStore, PublicationStore};
use crate::store::traits::{BlobStore, MetaStore};

pub struct FinalizedHistoryService<A: WriteAuthority, M: MetaStore, B: BlobStore> {
    pub ingest: IngestEngine<A, M, B>,
    query: LogsQueryEngine,
    cache: HashMapBytesCache,
    config: Config,
    state: Arc<RuntimeState>,
    role: ServiceRole,
    writer: Arc<futures::lock::Mutex<Option<WriteToken>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ServiceRole {
    ReaderOnly,
    ReaderWriter,
    SingleWriter,
}

impl ServiceRole {
    fn allows_writes(self) -> bool {
        !matches!(self, Self::ReaderOnly)
    }
}

impl<A: WriteAuthority, M: MetaStore + PublicationStore, B: BlobStore>
    FinalizedHistoryService<A, M, B>
{
    pub(crate) fn with_authority(
        config: Config,
        meta_store: M,
        blob_store: B,
        authority: A,
        role: ServiceRole,
    ) -> Self {
        let query = LogsQueryEngine::from_config(&config);
        let cache = HashMapBytesCache::new(config.bytes_cache);
        let ingest = IngestEngine::new(config.clone(), authority, meta_store, blob_store);
        Self {
            ingest,
            query,
            cache,
            config,
            state: Arc::new(RuntimeState::default()),
            role,
            writer: Arc::new(futures::lock::Mutex::new(None)),
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
        if !self.role.allows_writes() {
            let result = startup_plan(&self.ingest.meta_store, &self.ingest.blob_store, 0).await;
            self.update_backend_state(&result);
            return result;
        }

        let mut writer = self.writer.lock().await;
        let result = self.startup_locked(&mut writer).await;
        self.update_backend_state(&result);
        result
    }

    pub async fn indexed_finalized_head(&self) -> Result<u64>
    where
        M: PublicationStore,
    {
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

    pub fn cache_metrics(&self) -> BytesCacheMetrics {
        self.cache.metrics_snapshot()
    }

    pub async fn run_maintenance(&self) -> Result<MaintenanceStats> {
        if self.state.degraded.load(Ordering::Relaxed) {
            return Err(Error::Degraded(self.state.reason()));
        }

        let result = self.run_maintenance_with_writer().await;
        self.update_backend_state(&result);
        result
    }

    pub async fn run_gc_once(&self) -> Result<GcStats> {
        if self.state.degraded.load(Ordering::Relaxed) {
            return Err(Error::Degraded(self.state.reason()));
        }

        let stats = match self.run_gc_once_with_writer().await {
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

        let result = self
            .prune_block_hash_index_below_with_writer(min_block_num)
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

    async fn startup_locked(&self, writer: &mut Option<WriteToken>) -> Result<RecoveryPlan>
    where
        M: PublicationStore,
    {
        debug_assert!(self.role.allows_writes());
        if let Some(token) = *writer {
            let result = self
                .ingest
                .authority
                .authorize(
                    &token,
                    self.config.observe_upstream_finalized_block.as_ref()(),
                )
                .await;
            let token = match result {
                Ok(token) => token,
                Err(error) => {
                    if should_clear_writer(&error) {
                        *writer = None;
                    }
                    return Err(error);
                }
            };
            *writer = Some(token);
            return self.recovery_plan_for_token(token).await;
        }

        let token = self
            .ingest
            .authority
            .acquire(self.config.observe_upstream_finalized_block.as_ref()())
            .await?;
        let fence = self.ingest.authority.fence(&token);
        let _cleaned = cleanup_unpublished_suffix(
            &self.ingest.meta_store,
            &self.ingest.blob_store,
            token.indexed_finalized_head,
            fence,
        )
        .await?;
        let next_log_id =
            derive_next_log_id(&self.ingest.meta_store, token.indexed_finalized_head).await?;
        repair_open_stream_page_markers(
            &self.ingest.meta_store,
            &self.ingest.blob_store,
            next_log_id,
            fence,
        )
        .await?;
        *writer = Some(token);
        Ok(build_recovery_plan(
            token.indexed_finalized_head,
            token.epoch,
            next_log_id,
            0,
        ))
    }

    async fn ingest_blocks_with_startup(&self, blocks: Vec<Block>) -> Result<IngestOutcome>
    where
        M: PublicationStore,
    {
        if !self.role.allows_writes() {
            return Err(reader_only_mode_error());
        }

        let mut writer = self.writer.lock().await;
        if writer.is_none() {
            let _ = self.startup_locked(&mut writer).await?;
        }

        let token = (*writer).ok_or(Error::PublicationConflict)?;
        match self.ingest.ingest_finalized_blocks(&blocks, token).await {
            Ok((outcome, next_token)) => {
                *writer = Some(next_token);
                Ok(outcome)
            }
            Err(error) => {
                if should_clear_writer(&error) {
                    *writer = None;
                }
                Err(error)
            }
        }
    }

    async fn run_maintenance_with_writer(&self) -> Result<MaintenanceStats>
    where
        M: PublicationStore,
    {
        if !self.role.allows_writes() {
            return Err(reader_only_mode_error());
        }

        let mut writer = self.writer.lock().await;
        if writer.is_none() {
            let _ = self.startup_locked(&mut writer).await?;
        }

        let token = (*writer).ok_or(Error::PublicationConflict)?;
        let result = self
            .ingest
            .authority
            .authorize(
                &token,
                self.config.observe_upstream_finalized_block.as_ref()(),
            )
            .await;
        let token = match result {
            Ok(token) => token,
            Err(error) => {
                if should_clear_writer(&error) {
                    *writer = None;
                }
                return Err(error);
            }
        };
        *writer = Some(token);
        let result = self
            .ingest
            .run_periodic_maintenance(self.ingest.authority.fence(&token))
            .await;
        if let Err(error) = &result
            && should_clear_writer(error)
        {
            *writer = None;
        }
        result
    }

    async fn prune_block_hash_index_below_with_writer(&self, min_block_num: u64) -> Result<u64>
    where
        M: PublicationStore,
    {
        if !self.role.allows_writes() {
            return Err(reader_only_mode_error());
        }

        let mut writer = self.writer.lock().await;
        if writer.is_none() {
            let _ = self.startup_locked(&mut writer).await?;
        }

        let token = (*writer).ok_or(Error::PublicationConflict)?;
        let result = self
            .ingest
            .authority
            .authorize(
                &token,
                self.config.observe_upstream_finalized_block.as_ref()(),
            )
            .await;
        let token = match result {
            Ok(token) => token,
            Err(error) => {
                if should_clear_writer(&error) {
                    *writer = None;
                }
                return Err(error);
            }
        };
        *writer = Some(token);

        let worker = GcWorker::new(&self.ingest.meta_store, &self.config);
        let result = worker
            .prune_block_hash_index_below(min_block_num, self.ingest.authority.fence(&token))
            .await;
        if let Err(error) = &result
            && should_clear_writer(error)
        {
            *writer = None;
        }
        result
    }

    async fn run_gc_once_with_writer(&self) -> Result<GcStats>
    where
        M: PublicationStore,
    {
        if !self.role.allows_writes() {
            return Err(reader_only_mode_error());
        }

        let mut writer = self.writer.lock().await;
        if writer.is_none() {
            let _ = self.startup_locked(&mut writer).await?;
        }

        let token = (*writer).ok_or(Error::PublicationConflict)?;
        let result = self
            .ingest
            .authority
            .authorize(
                &token,
                self.config.observe_upstream_finalized_block.as_ref()(),
            )
            .await;
        let token = match result {
            Ok(token) => token,
            Err(error) => {
                if should_clear_writer(&error) {
                    *writer = None;
                }
                return Err(error);
            }
        };
        *writer = Some(token);

        let worker = GcWorker::new(&self.ingest.meta_store, &self.config);
        let result = worker
            .run_once_with_fence(self.ingest.authority.fence(&token))
            .await;
        if let Err(error) = &result
            && should_clear_writer(error)
        {
            *writer = None;
        }
        result
    }

    async fn recovery_plan_for_token(&self, token: WriteToken) -> Result<RecoveryPlan> {
        let next_log_id =
            derive_next_log_id(&self.ingest.meta_store, token.indexed_finalized_head).await?;
        Ok(build_recovery_plan(
            token.indexed_finalized_head,
            token.epoch,
            next_log_id,
            0,
        ))
    }
}

fn should_clear_writer(error: &Error) -> bool {
    matches!(
        error,
        Error::LeaseLost
            | Error::LeaseObservationUnavailable
            | Error::PublicationConflict
            | Error::ModeConflict(_)
    )
}

fn reader_only_mode_error() -> Error {
    Error::ModeConflict("reader-only service cannot acquire write authority")
}

impl<M, B> FinalizedHistoryService<LeaseAuthority<M>, M, B>
where
    M: MetaStore + PublicationStore + FenceStore + Clone,
    B: BlobStore,
{
    pub fn new_reader_writer(config: Config, meta_store: M, blob_store: B, owner_id: u64) -> Self {
        let authority = LeaseAuthority::new(
            meta_store.clone(),
            owner_id,
            config.publication_lease_blocks,
            config.publication_lease_renew_threshold_blocks,
        );
        Self::with_authority(
            config,
            meta_store,
            blob_store,
            authority,
            ServiceRole::ReaderWriter,
        )
    }
}

impl<M, B> FinalizedHistoryService<ReadOnlyAuthority, M, B>
where
    M: MetaStore + PublicationStore + Clone,
    B: BlobStore,
{
    pub fn new_reader_only(config: Config, meta_store: M, blob_store: B) -> Self {
        Self::with_authority(
            config,
            meta_store,
            blob_store,
            ReadOnlyAuthority,
            ServiceRole::ReaderOnly,
        )
    }
}

impl<M, B> FinalizedHistoryService<SingleWriterAuthority<M>, M, B>
where
    M: MetaStore + PublicationStore + FenceStore + Clone,
    B: BlobStore,
{
    pub fn new_single_writer(config: Config, meta_store: M, blob_store: B) -> Self {
        let authority = SingleWriterAuthority::new(meta_store.clone());
        Self::with_authority(
            config,
            meta_store,
            blob_store,
            authority,
            ServiceRole::SingleWriter,
        )
    }
}

impl<A, M, B> FinalizedHistoryWriter for FinalizedHistoryService<A, M, B>
where
    A: WriteAuthority,
    M: MetaStore + PublicationStore,
    B: BlobStore,
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

impl<A, M, B> FinalizedLogQueries for FinalizedHistoryService<A, M, B>
where
    A: WriteAuthority,
    M: MetaStore + PublicationStore,
    B: BlobStore,
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
            .query_logs_with_cache(
                &self.ingest.meta_store,
                &self.ingest.blob_store,
                &self.cache,
                request,
                budget,
            )
            .await;
        self.update_backend_state(&result);
        result
    }
}
