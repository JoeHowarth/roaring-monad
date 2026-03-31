use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use bytes::Bytes;

use crate::error::Result;
use crate::store::blob::{BlobStore, BlobTableId};
use crate::store::common::Page;

#[derive(Debug, Clone, Default)]
pub struct InMemoryBlobStore {
    blobs: Arc<RwLock<BTreeMap<(BlobTableId, Vec<u8>), Bytes>>>,
}

impl InMemoryBlobStore {
    pub fn len(&self) -> usize {
        self.blobs.read().map(|guard| guard.len()).unwrap_or_default()
    }
}

impl BlobStore for InMemoryBlobStore {
    async fn put_blob(&self, table: BlobTableId, key: &[u8], value: Bytes) -> Result<()> {
        let mut guard = self
            .blobs
            .write()
            .map_err(|_| crate::error::MonadChainDataError::Backend("poisoned lock".to_string()))?;
        guard.insert((table, key.to_vec()), value);
        Ok(())
    }

    async fn get_blob(&self, table: BlobTableId, key: &[u8]) -> Result<Option<Bytes>> {
        let guard = self
            .blobs
            .read()
            .map_err(|_| crate::error::MonadChainDataError::Backend("poisoned lock".to_string()))?;
        Ok(guard.get(&(table, key.to_vec())).cloned())
    }

    async fn delete_blob(&self, table: BlobTableId, key: &[u8]) -> Result<()> {
        let _ = (table, key);
        todo!("delete blob values from the in-memory blob store")
    }

    async fn list_prefix(
        &self,
        table: BlobTableId,
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page> {
        let _ = (table, prefix, cursor, limit);
        todo!("list blob keys by prefix from the in-memory blob store")
    }
}
