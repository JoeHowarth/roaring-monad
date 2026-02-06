use std::collections::BTreeMap;
use std::sync::RwLock;

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::store::traits::{DelCond, FenceToken, MetaStore, Page, PutCond, PutResult, Record};

#[derive(Default)]
pub struct InMemoryMetaStore {
    inner: RwLock<BTreeMap<Vec<u8>, Record>>,
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

    async fn put(&self, key: &[u8], value: Bytes, cond: PutCond, _fence: FenceToken) -> Result<PutResult> {
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

    async fn delete(&self, key: &[u8], cond: DelCond, _fence: FenceToken) -> Result<()> {
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

    async fn list_prefix(&self, prefix: &[u8], cursor: Option<Vec<u8>>, limit: usize) -> Result<Page> {
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
