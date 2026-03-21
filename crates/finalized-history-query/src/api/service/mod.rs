mod handlers;
mod startup;

use std::sync::Arc;

use crate::config::Config;
use crate::error::Error;
use crate::ingest::authority::{LeaseAuthority, ReadOnlyAuthority, WriteAuthority};
use crate::ingest::engine::IngestEngine;
use crate::logs::query::LogsQueryEngine;
use crate::logs::types::HealthReport;
use crate::store::publication::MetaPublicationStore;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::{BytesCacheMetrics, Tables};

pub struct FinalizedHistoryService<A: WriteAuthority, M: MetaStore, B: BlobStore> {
    pub ingest: IngestEngine<A, Arc<M>, Arc<B>>,
    publication_store: MetaPublicationStore<M>,
    query: LogsQueryEngine,
    tables: Tables<M, B>,
    allows_writes: bool,
}

impl<A: WriteAuthority, M: MetaStore, B: BlobStore> FinalizedHistoryService<A, M, B> {
    pub(crate) fn with_authority(
        config: Config,
        meta_store: M,
        blob_store: B,
        authority: A,
        allows_writes: bool,
    ) -> Self {
        let query = LogsQueryEngine::from_config(&config);
        let meta_store = Arc::new(meta_store);
        let blob_store = Arc::new(blob_store);
        let publication_store = MetaPublicationStore::new(Arc::clone(&meta_store));
        let bytes_cache = config.bytes_cache;
        let tables = Tables::new(
            Arc::clone(&meta_store),
            Arc::clone(&blob_store),
            bytes_cache,
        );
        let ingest = IngestEngine::new(config, authority, meta_store, blob_store);
        Self {
            ingest,
            publication_store,
            query,
            tables,
            allows_writes,
        }
    }

    pub async fn health(&self) -> HealthReport {
        HealthReport {
            healthy: true,
            message: "ok".to_string(),
        }
    }

    pub fn cache_metrics(&self) -> BytesCacheMetrics {
        self.tables.metrics_snapshot()
    }

    pub(crate) fn tables(&self) -> &Tables<M, B> {
        &self.tables
    }
}

fn reader_only_mode_error() -> Error {
    Error::ReadOnlyMode("reader-only service cannot acquire write authority")
}

impl<M, B> FinalizedHistoryService<LeaseAuthority<MetaPublicationStore<M>>, M, B>
where
    M: MetaStore + Clone,
    B: BlobStore,
{
    pub fn new_reader_writer(config: Config, meta_store: M, blob_store: B, owner_id: u64) -> Self {
        let authority = LeaseAuthority::new(
            MetaPublicationStore::new(Arc::new(meta_store.clone())),
            owner_id,
            config.publication_lease_blocks,
            config.publication_lease_renew_threshold_blocks,
        );
        Self::with_authority(config, meta_store, blob_store, authority, true)
    }
}

impl<M, B> FinalizedHistoryService<ReadOnlyAuthority, M, B>
where
    M: MetaStore + Clone,
    B: BlobStore,
{
    pub fn new_reader_only(config: Config, meta_store: M, blob_store: B) -> Self {
        Self::with_authority(config, meta_store, blob_store, ReadOnlyAuthority, false)
    }
}
