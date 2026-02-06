use std::collections::BTreeMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::store::traits::{DelCond, FenceToken, MetaStore, Page, PutCond, PutResult, Record};

#[derive(Default)]
pub struct InMemoryMetaStore {
    inner: RwLock<BTreeMap<Vec<u8>, Record>>,
    min_epoch: AtomicU64,
}

impl InMemoryMetaStore {
    pub fn with_min_epoch(min_epoch: u64) -> Self {
        Self {
            inner: RwLock::new(BTreeMap::new()),
            min_epoch: AtomicU64::new(min_epoch),
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

#[async_trait::async_trait]
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
        let start = cursor.unwrap_or_default();

        for k in guard.keys() {
            if k < &start {
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
