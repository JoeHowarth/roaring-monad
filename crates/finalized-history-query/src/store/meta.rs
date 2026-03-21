use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::store::traits::{
    DelCond, MetaStore, Page, PutCond, PutResult, Record, ScannableTableId, TableId,
};

#[derive(Clone)]
pub struct InMemoryMetaStore {
    inner: Arc<RwLock<BTreeMap<(TableId, Vec<u8>), Record>>>,
    scan_inner: Arc<RwLock<BTreeMap<(ScannableTableId, Vec<u8>, Vec<u8>), Record>>>,
}

impl InMemoryMetaStore {
    pub fn with_min_epoch(_min_epoch: u64) -> Self {
        Self {
            inner: Arc::new(RwLock::new(BTreeMap::new())),
            scan_inner: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

impl Default for InMemoryMetaStore {
    fn default() -> Self {
        Self::with_min_epoch(0)
    }
}

impl MetaStore for InMemoryMetaStore {
    async fn get(&self, table: TableId, key: &[u8]) -> Result<Option<Record>> {
        let guard = self
            .inner
            .read()
            .map_err(|_| Error::Backend("poisoned lock".to_string()))?;
        Ok(guard.get(&(table, key.to_vec())).cloned())
    }

    async fn put(
        &self,
        table: TableId,
        key: &[u8],
        value: Bytes,
        cond: PutCond,
    ) -> Result<PutResult> {
        let mut guard = self
            .inner
            .write()
            .map_err(|_| Error::Backend("poisoned lock".to_string()))?;

        let entry_key = (table, key.to_vec());
        let current = guard.get(&entry_key).cloned();
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
            entry_key,
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

    async fn delete(&self, table: TableId, key: &[u8], cond: DelCond) -> Result<()> {
        let mut guard = self
            .inner
            .write()
            .map_err(|_| Error::Backend("poisoned lock".to_string()))?;

        let entry_key = (table, key.to_vec());
        let should_delete = match (cond, guard.get(&entry_key)) {
            (DelCond::Any, Some(_)) => true,
            (DelCond::Any, None) => false,
            (DelCond::IfVersion(v), Some(r)) => r.version == v,
            (DelCond::IfVersion(_), None) => false,
        };

        if should_delete {
            guard.remove(&entry_key);
        }
        Ok(())
    }

    async fn scan_get(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
    ) -> Result<Option<Record>> {
        let guard = self
            .scan_inner
            .read()
            .map_err(|_| Error::Backend("poisoned lock".to_string()))?;
        Ok(guard
            .get(&(table, partition.to_vec(), clustering.to_vec()))
            .cloned())
    }

    async fn scan_put(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
        value: Bytes,
        cond: PutCond,
    ) -> Result<PutResult> {
        let mut guard = self
            .scan_inner
            .write()
            .map_err(|_| Error::Backend("poisoned lock".to_string()))?;

        let entry_key = (table, partition.to_vec(), clustering.to_vec());
        let current = guard.get(&entry_key).cloned();
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
            entry_key,
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

    async fn scan_delete(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
        cond: DelCond,
    ) -> Result<()> {
        let mut guard = self
            .scan_inner
            .write()
            .map_err(|_| Error::Backend("poisoned lock".to_string()))?;

        let entry_key = (table, partition.to_vec(), clustering.to_vec());
        let should_delete = match (cond, guard.get(&entry_key)) {
            (DelCond::Any, Some(_)) => true,
            (DelCond::Any, None) => false,
            (DelCond::IfVersion(v), Some(r)) => r.version == v,
            (DelCond::IfVersion(_), None) => false,
        };

        if should_delete {
            guard.remove(&entry_key);
        }
        Ok(())
    }

    async fn scan_list(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page> {
        let guard = self
            .scan_inner
            .read()
            .map_err(|_| Error::Backend("poisoned lock".to_string()))?;

        let mut keys = Vec::new();
        let mut next_cursor = None;
        let has_cursor = cursor.is_some();
        let start = cursor.unwrap_or_default();

        for ((entry_table, entry_partition, key), _) in guard.iter() {
            if *entry_table != table {
                continue;
            }
            if entry_partition.as_slice() != partition {
                continue;
            }
            if !key.starts_with(prefix) {
                continue;
            }
            if has_cursor && key <= &start {
                continue;
            }
            if !has_cursor && key < &start {
                continue;
            }
            keys.push(key.clone());
            if keys.len() == limit {
                next_cursor = keys.last().cloned();
                break;
            }
        }

        Ok(Page { keys, next_cursor })
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures::executor::block_on;

    use super::InMemoryMetaStore;
    use crate::domain::keys::LOG_DIR_BY_BLOCK_TABLE;
    use crate::domain::table_specs::LogDirByBlockSpec;
    use crate::store::traits::MetaStore;
    use crate::store::traits::PutCond;

    #[test]
    fn list_pagination_does_not_repeat_cursor_entry() {
        block_on(async {
            let store = InMemoryMetaStore::default();
            for index in 0..1_025u64 {
                store
                    .scan_put(
                        LOG_DIR_BY_BLOCK_TABLE,
                        &LogDirByBlockSpec::partition(0),
                        &LogDirByBlockSpec::clustering(index),
                        Bytes::from_static(b"v"),
                        PutCond::Any,
                    )
                    .await
                    .expect("seed key");
            }

            let mut cursor = None;
            let mut seen = Vec::new();
            loop {
                let page = store
                    .scan_list(
                        LOG_DIR_BY_BLOCK_TABLE,
                        &LogDirByBlockSpec::partition(0),
                        b"",
                        cursor.take(),
                        1_024,
                    )
                    .await
                    .expect("list");
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
