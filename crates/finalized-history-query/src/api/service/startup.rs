use crate::core::state::{derive_next_log_id, load_finalized_head_state};
use crate::error::Result;
use crate::ingest::authority::{WriteAuthority, WriteToken};
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

    pub(super) async fn startup_locked(
        &self,
        writer: &mut Option<WriteToken>,
    ) -> Result<StartupPlan>
    where
        M: PublicationStore,
    {
        debug_assert!(self.allows_writes);
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
            match self.recover_and_plan(token).await {
                Ok(plan) => return Ok(plan),
                Err(error) => {
                    if should_clear_writer(&error) {
                        *writer = None;
                    }
                    return Err(error);
                }
            }
        }

        let token = self
            .ingest
            .authority
            .acquire(self.config.observe_upstream_finalized_block.as_ref()())
            .await?;
        let plan = self.recover_and_plan(token).await?;
        *writer = Some(token);
        Ok(plan)
    }

    async fn recover_and_plan(&self, token: WriteToken) -> Result<StartupPlan> {
        let next_log_id = derive_next_log_id(self.tables(), token.indexed_finalized_head).await?;
        Ok(build_startup_plan(
            token.indexed_finalized_head,
            token.epoch,
            next_log_id,
            0,
        ))
    }
}
