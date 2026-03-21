use crate::core::state::derive_next_log_id;
use crate::error::Result;
use crate::ingest::authority::{WriteAuthority, WriteSession};
use crate::startup::{StartupPlan, build_startup_plan, startup_plan};
use crate::store::publication::PublicationStore;
use crate::store::traits::{BlobStore, MetaStore};

use super::FinalizedHistoryService;

impl<A: WriteAuthority, M: MetaStore, B: BlobStore> FinalizedHistoryService<A, M, B> {
    pub async fn startup(&self) -> Result<StartupPlan> {
        if !self.allows_writes {
            return startup_plan(self.tables(), &self.publication_store, 0).await;
        }

        self.startup_locked().await
    }

    pub async fn indexed_finalized_head(&self) -> Result<u64> {
        self.publication_store
            .load_finalized_head_state()
            .await
            .map(|state| state.indexed_finalized_head)
    }

    pub(super) async fn startup_locked(&self) -> Result<StartupPlan> {
        debug_assert!(self.allows_writes);
        let session = self
            .ingest
            .authority
            .begin_write(self.ingest.config.observe_upstream_finalized_block.as_ref()())
            .await?;
        self.recover_and_plan(session.state().indexed_finalized_head)
            .await
    }

    async fn recover_and_plan(&self, indexed_finalized_head: u64) -> Result<StartupPlan> {
        let next_log_id = derive_next_log_id(self.tables(), indexed_finalized_head).await?;
        Ok(build_startup_plan(indexed_finalized_head, next_log_id, 0))
    }
}
