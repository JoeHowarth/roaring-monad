use crate::domain::types::PublicationState;
use crate::error::Result;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CasOutcome<T> {
    Applied(T),
    Failed { current: Option<T> },
}

#[allow(async_fn_in_trait)]
pub trait PublicationStore: Send + Sync {
    async fn load(&self) -> Result<Option<PublicationState>>;

    async fn create_if_absent(
        &self,
        initial: &PublicationState,
    ) -> Result<CasOutcome<PublicationState>>;

    async fn compare_and_set(
        &self,
        expected: &PublicationState,
        next: &PublicationState,
    ) -> Result<CasOutcome<PublicationState>>;
}

#[allow(async_fn_in_trait)]
pub trait FenceStore: Send + Sync {
    async fn advance_fence(&self, min_epoch: u64) -> Result<()>;

    async fn current_fence(&self) -> Result<u64>;
}
