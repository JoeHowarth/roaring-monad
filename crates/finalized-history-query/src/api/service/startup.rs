use crate::core::state::derive_next_log_id;
use crate::error::Result;
use crate::ingest::authority::WriteAuthority;
use crate::startup::{StartupPlan, build_startup_plan, startup_plan};
use crate::store::publication::PublicationStore;
use crate::store::traits::{BlobStore, MetaStore};

use super::{FinalizedHistoryService, should_clear_writer};

impl<A: WriteAuthority, M: MetaStore + PublicationStore, B: BlobStore>
    FinalizedHistoryService<A, M, B>
{
    pub async fn startup(&self) -> Result<StartupPlan> {
        if !self.allows_writes {
            let result = startup_plan(self.tables(), 0).await;
            self.update_backend_state(&result);
            return result;
        }

        let _write_guard = self.write_guard.lock().await;
        let result = self.startup_locked().await;
        self.update_backend_state(&result);
        result
    }

    pub async fn indexed_finalized_head(&self) -> Result<u64>
    where
        M: PublicationStore,
    {
        let result = self
            .ingest
            .meta_store
            .load_finalized_head_state()
            .await
            .map(|state| state.indexed_finalized_head);
        self.update_backend_state(&result);
        result
    }

    pub(super) async fn startup_locked(&self) -> Result<StartupPlan>
    where
        M: PublicationStore,
    {
        debug_assert!(self.allows_writes);
        let state = match self
            .ingest
            .authority
            .ensure_writer(self.config.observe_upstream_finalized_block.as_ref()())
            .await
        {
            Ok(state) => state,
            Err(error) => {
                if should_clear_writer(&error) {
                    self.ingest.authority.clear().await;
                }
                return Err(error);
            }
        };

        match self.recover_and_plan(state.indexed_finalized_head).await {
            Ok(plan) => Ok(plan),
            Err(error) => {
                if should_clear_writer(&error) {
                    self.ingest.authority.clear().await;
                }
                Err(error)
            }
        }
    }

    async fn recover_and_plan(&self, indexed_finalized_head: u64) -> Result<StartupPlan> {
        let next_log_id = derive_next_log_id(self.tables(), indexed_finalized_head).await?;
        Ok(build_startup_plan(indexed_finalized_head, next_log_id, 0))
    }
}
