use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use bytes::Bytes;

use crate::error::Result;
use crate::store::common::Page;
use crate::store::meta::{DelCond, MetaStore, PutResult, Record, ScannableTableId, TableId};

#[derive(Debug, Clone, Default)]
pub struct InMemoryMetaStore {
    kv_records: Arc<RwLock<BTreeMap<(TableId, Vec<u8>), Record>>>,
    scan_records: Arc<RwLock<BTreeMap<(ScannableTableId, Vec<u8>, Vec<u8>), Record>>>,
}

impl InMemoryMetaStore {
    pub fn len(&self) -> usize {
        self.kv_records
            .read()
            .map(|guard| guard.len())
            .unwrap_or_default()
            + self
                .scan_records
                .read()
                .map(|guard| guard.len())
                .unwrap_or_default()
    }
}

impl MetaStore for InMemoryMetaStore {
    async fn get(&self, table: TableId, key: &[u8]) -> Result<Option<Record>> {
        let guard = self
            .kv_records
            .read()
            .map_err(|_| crate::error::MonadChainDataError::Backend("poisoned lock".to_string()))?;
        Ok(guard.get(&(table, key.to_vec())).cloned())
    }

    async fn put(&self, table: TableId, key: &[u8], value: Bytes) -> Result<PutResult> {
        let mut guard = self
            .kv_records
            .write()
            .map_err(|_| crate::error::MonadChainDataError::Backend("poisoned lock".to_string()))?;

        let entry_key = (table, key.to_vec());
        let current = guard.get(&entry_key).cloned();
        let next_version = current.map_or(1, |record| record.version + 1);
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
        let _ = (table, key, cond);
        todo!("delete metadata records from the in-memory meta store")
    }

    async fn scan_get(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
    ) -> Result<Option<Record>> {
        let guard = self
            .scan_records
            .read()
            .map_err(|_| crate::error::MonadChainDataError::Backend("poisoned lock".to_string()))?;
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
    ) -> Result<PutResult> {
        let mut guard = self
            .scan_records
            .write()
            .map_err(|_| crate::error::MonadChainDataError::Backend("poisoned lock".to_string()))?;

        let entry_key = (table, partition.to_vec(), clustering.to_vec());
        let current = guard.get(&entry_key).cloned();
        let next_version = current.map_or(1, |record| record.version + 1);
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
        let _ = (table, partition, clustering, cond);
        todo!("delete scannable metadata records from the in-memory meta store")
    }

    async fn scan_list(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page> {
        let _ = (table, partition, prefix, cursor, limit);
        todo!("list scannable metadata keys from the in-memory meta store")
    }
}
