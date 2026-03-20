mod handlers;
mod startup;

use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::cache::{BytesCacheMetrics, HashMapBytesCache};
use crate::config::Config;
use crate::core::runtime::RuntimeState;
use crate::error::{Error, Result};
use crate::ingest::authority::{LeaseAuthority, ReadOnlyAuthority, WriteAuthority, WriteToken};
use crate::ingest::engine::IngestEngine;
use crate::logs::query::LogsQueryEngine;
use crate::logs::types::HealthReport;
use crate::store::publication::PublicationStore;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;

pub struct FinalizedHistoryService<A: WriteAuthority, M: MetaStore, B: BlobStore> {
    pub ingest: IngestEngine<A, M, B>,
    query: LogsQueryEngine,
    cache: HashMapBytesCache,
    config: Config,
    state: Arc<RuntimeState>,
    allows_writes: bool,
    writer: Arc<futures::lock::Mutex<Option<WriteToken>>>,
}

impl<A: WriteAuthority, M: MetaStore + PublicationStore, B: BlobStore>
    FinalizedHistoryService<A, M, B>
{
    pub(crate) fn with_authority(
        config: Config,
        meta_store: M,
        blob_store: B,
        authority: A,
        allows_writes: bool,
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
            allows_writes,
            writer: Arc::new(futures::lock::Mutex::new(None)),
        }
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

    pub(crate) fn tables(&self) -> Tables<'_, M, B> {
        Tables::new(
            &self.ingest.meta_store,
            &self.ingest.blob_store,
            &self.cache,
        )
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

fn should_clear_writer(error: &Error) -> bool {
    matches!(
        error,
        Error::LeaseLost
            | Error::LeaseObservationUnavailable
            | Error::PublicationConflict
            | Error::ReadOnlyMode(_)
    )
}

fn reader_only_mode_error() -> Error {
    Error::ReadOnlyMode("reader-only service cannot acquire write authority")
}

impl<M, B> FinalizedHistoryService<LeaseAuthority<M>, M, B>
where
    M: MetaStore + PublicationStore + Clone,
    B: BlobStore,
{
    pub fn new_reader_writer(config: Config, meta_store: M, blob_store: B, owner_id: u64) -> Self {
        let authority = LeaseAuthority::new(
            meta_store.clone(),
            owner_id,
            config.publication_lease_blocks,
            config.publication_lease_renew_threshold_blocks,
        );
        Self::with_authority(config, meta_store, blob_store, authority, true)
    }
}

impl<M, B> FinalizedHistoryService<ReadOnlyAuthority, M, B>
where
    M: MetaStore + PublicationStore + Clone,
    B: BlobStore,
{
    pub fn new_reader_only(config: Config, meta_store: M, blob_store: B) -> Self {
        Self::with_authority(config, meta_store, blob_store, ReadOnlyAuthority, false)
    }
}
