use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use bytes::Bytes;

use crate::codec::finalized_state::{decode_publication_state, encode_publication_state};
use crate::domain::keys::PUBLICATION_STATE_KEY;
use crate::domain::types::PublicationState;
use crate::error::{Error, Result};
use crate::store::publication::{CasOutcome, FenceStore, PublicationStore};
use crate::store::traits::{DelCond, FenceToken, MetaStore, Page, PutCond, PutResult, Record};

#[derive(Clone)]
pub struct InMemoryMetaStore {
    inner: Arc<RwLock<BTreeMap<Vec<u8>, Record>>>,
    min_epoch: Arc<AtomicU64>,
}

impl InMemoryMetaStore {
    pub fn with_min_epoch(min_epoch: u64) -> Self {
        Self {
            inner: Arc::new(RwLock::new(BTreeMap::new())),
            min_epoch: Arc::new(AtomicU64::new(min_epoch)),
        }
    }

    pub fn set_min_epoch(&self, min_epoch: u64) {
        self.min_epoch.store(min_epoch, Ordering::Relaxed);
    }

    fn validate_fence(&self, fence: FenceToken) -> Result<()> {
        let required = self.min_epoch.load(Ordering::Relaxed);
        if fence.0 < required {
            return Err(Error::LeaseLost);
        }
        Ok(())
    }
}

impl Default for InMemoryMetaStore {
    fn default() -> Self {
        Self::with_min_epoch(0)
    }
}

impl MetaStore for InMemoryMetaStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Record>> {
        let guard = self
            .inner
            .read()
            .map_err(|_| Error::Backend("poisoned lock".to_string()))?;
        Ok(guard.get(key).cloned())
    }

    async fn put(
        &self,
        key: &[u8],
        value: Bytes,
        cond: PutCond,
        fence: FenceToken,
    ) -> Result<PutResult> {
        self.validate_fence(fence)?;
        let mut guard = self
            .inner
            .write()
            .map_err(|_| Error::Backend("poisoned lock".to_string()))?;

        let current = guard.get(key).cloned();
        let allowed = match (cond, current.as_ref()) {
            (PutCond::Any, _) => true,
            (PutCond::IfAbsent, None) => true,
            (PutCond::IfAbsent, Some(_)) => false,
            (PutCond::IfVersion(v), Some(r)) => r.version == v,
            (PutCond::IfVersion(_), None) => false,
        };

        if !allowed {
            return Ok(PutResult {
                applied: false,
                version: current.map(|r| r.version),
            });
        }

        let next_version = current.map_or(1, |r| r.version + 1);
        guard.insert(
            key.to_vec(),
            Record {
                value,
                version: next_version,
            },
        );
        Ok(PutResult {
            applied: true,
            version: Some(next_version),
        })
    }

    async fn delete(&self, key: &[u8], cond: DelCond, fence: FenceToken) -> Result<()> {
        self.validate_fence(fence)?;
        let mut guard = self
            .inner
            .write()
            .map_err(|_| Error::Backend("poisoned lock".to_string()))?;

        let should_delete = match (cond, guard.get(key)) {
            (DelCond::Any, Some(_)) => true,
            (DelCond::Any, None) => false,
            (DelCond::IfVersion(v), Some(r)) => r.version == v,
            (DelCond::IfVersion(_), None) => false,
        };

        if should_delete {
            guard.remove(key);
        }
        Ok(())
    }

    async fn list_prefix(
        &self,
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page> {
        let guard = self
            .inner
            .read()
            .map_err(|_| Error::Backend("poisoned lock".to_string()))?;

        let mut keys = Vec::new();
        let mut next_cursor = None;
        let has_cursor = cursor.is_some();
        let start = cursor.unwrap_or_default();

        for k in guard.keys() {
            if has_cursor && k <= &start {
                continue;
            }
            if !has_cursor && k < &start {
                continue;
            }
            if !k.starts_with(prefix) {
                continue;
            }
            keys.push(k.clone());
            if keys.len() == limit {
                next_cursor = keys.last().cloned();
                break;
            }
        }

        Ok(Page { keys, next_cursor })
    }
}

impl PublicationStore for InMemoryMetaStore {
    async fn load(&self) -> Result<Option<PublicationState>> {
        let guard = self
            .inner
            .read()
            .map_err(|_| Error::Backend("poisoned lock".to_string()))?;
        let Some(record) = guard.get(PUBLICATION_STATE_KEY) else {
            return Ok(None);
        };
        Ok(Some(decode_publication_state(&record.value)?))
    }

    async fn create_if_absent(
        &self,
        initial: &PublicationState,
    ) -> Result<CasOutcome<PublicationState>> {
        let mut guard = self
            .inner
            .write()
            .map_err(|_| Error::Backend("poisoned lock".to_string()))?;
        if let Some(record) = guard.get(PUBLICATION_STATE_KEY) {
            return Ok(CasOutcome::Failed {
                current: Some(decode_publication_state(&record.value)?),
            });
        }

        guard.insert(
            PUBLICATION_STATE_KEY.to_vec(),
            Record {
                value: encode_publication_state(initial),
                version: 1,
            },
        );
        Ok(CasOutcome::Applied(initial.clone()))
    }

    async fn compare_and_set(
        &self,
        expected: &PublicationState,
        next: &PublicationState,
    ) -> Result<CasOutcome<PublicationState>> {
        let mut guard = self
            .inner
            .write()
            .map_err(|_| Error::Backend("poisoned lock".to_string()))?;
        let Some(record) = guard.get(PUBLICATION_STATE_KEY).cloned() else {
            return Ok(CasOutcome::Failed { current: None });
        };
        let current = decode_publication_state(&record.value)?;
        if current != *expected {
            return Ok(CasOutcome::Failed {
                current: Some(current),
            });
        }

        guard.insert(
            PUBLICATION_STATE_KEY.to_vec(),
            Record {
                value: encode_publication_state(next),
                version: record.version.saturating_add(1),
            },
        );
        Ok(CasOutcome::Applied(next.clone()))
    }
}

impl FenceStore for InMemoryMetaStore {
    async fn advance_fence(&self, min_epoch: u64) -> Result<()> {
        let _ = self
            .min_epoch
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.max(min_epoch))
            });
        Ok(())
    }

    async fn current_fence(&self) -> Result<u64> {
        Ok(self.min_epoch.load(Ordering::Relaxed))
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures::executor::block_on;

    use super::InMemoryMetaStore;
    use crate::store::traits::{FenceToken, MetaStore, PutCond};

    #[test]
    fn list_prefix_pagination_does_not_repeat_cursor_entry() {
        block_on(async {
            let store = InMemoryMetaStore::default();
            for index in 0..1_025u64 {
                let key = format!("list-prefix/{index:04}").into_bytes();
                store
                    .put(&key, Bytes::from_static(b"v"), PutCond::Any, FenceToken(0))
                    .await
                    .expect("seed key");
            }

            let mut cursor = None;
            let mut seen = Vec::new();
            loop {
                let page = store
                    .list_prefix(b"list-prefix/", cursor.take(), 1_024)
                    .await
                    .expect("list prefix");
                seen.extend(page.keys.iter().cloned());
                if page.next_cursor.is_none() {
                    break;
                }
                cursor = page.next_cursor;
            }

            let unique = seen.iter().collect::<std::collections::BTreeSet<_>>();
            assert_eq!(seen.len(), 1_025);
            assert_eq!(unique.len(), 1_025);
        });
    }
}
