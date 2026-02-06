use std::collections::HashMap;
use std::sync::RwLock;

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::store::traits::BlobStore;

#[derive(Default)]
pub struct InMemoryBlobStore {
    inner: RwLock<HashMap<Vec<u8>, Bytes>>,
}

#[async_trait::async_trait]
impl BlobStore for InMemoryBlobStore {
    async fn put_blob(&self, key: &[u8], value: Bytes) -> Result<()> {
        let mut guard = self
            .inner
            .write()
            .map_err(|_| Error::Backend("poisoned lock".to_string()))?;
        guard.insert(key.to_vec(), value);
        Ok(())
    }

    async fn get_blob(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let guard = self
            .inner
            .read()
            .map_err(|_| Error::Backend("poisoned lock".to_string()))?;
        Ok(guard.get(key).cloned())
    }

    async fn delete_blob(&self, key: &[u8]) -> Result<()> {
        let mut guard = self
            .inner
            .write()
            .map_err(|_| Error::Backend("poisoned lock".to_string()))?;
        guard.remove(key);
        Ok(())
    }
}
