use std::sync::Arc;

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

impl<T: PublicationStore> PublicationStore for Arc<T> {
    async fn load(&self) -> Result<Option<PublicationState>> {
        self.as_ref().load().await
    }

    async fn create_if_absent(
        &self,
        initial: &PublicationState,
    ) -> Result<CasOutcome<PublicationState>> {
        self.as_ref().create_if_absent(initial).await
    }

    async fn compare_and_set(
        &self,
        expected: &PublicationState,
        next: &PublicationState,
    ) -> Result<CasOutcome<PublicationState>> {
        self.as_ref().compare_and_set(expected, next).await
    }
}
