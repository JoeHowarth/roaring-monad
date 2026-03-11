use std::collections::HashMap;
use std::sync::RwLock;

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::store::traits::{BlobStore, CreateOutcome, Page};

#[derive(Default)]
pub struct InMemoryBlobStore {
    inner: RwLock<HashMap<Vec<u8>, Bytes>>,
}

impl BlobStore for InMemoryBlobStore {
    async fn put_blob(&self, key: &[u8], value: Bytes) -> Result<()> {
        let mut guard = self
            .inner
            .write()
            .map_err(|_| Error::Backend("poisoned lock".to_string()))?;
        guard.insert(key.to_vec(), value);
        Ok(())
    }

    async fn put_blob_if_absent(&self, key: &[u8], value: Bytes) -> Result<CreateOutcome> {
        let mut guard = self
            .inner
            .write()
            .map_err(|_| Error::Backend("poisoned lock".to_string()))?;
        if guard.contains_key(key) {
            return Ok(CreateOutcome::AlreadyExists);
        }
        guard.insert(key.to_vec(), value);
        Ok(CreateOutcome::Created)
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

        let has_cursor = cursor.is_some();
        let start = cursor.unwrap_or_default();
        let mut keys = Vec::new();
        let mut all_keys: Vec<Vec<u8>> = guard.keys().cloned().collect();
        all_keys.sort();
        let mut next_cursor = None;
        for k in all_keys {
            if has_cursor && k <= start {
                continue;
            }
            if !has_cursor && k < start {
                continue;
            }
            if !k.starts_with(prefix) {
                continue;
            }
            keys.push(k);
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

    use super::InMemoryBlobStore;
    use crate::store::traits::BlobStore;

    #[test]
    fn list_prefix_pagination_does_not_repeat_cursor_entry() {
        block_on(async {
            let store = InMemoryBlobStore::default();
            for index in 0..1_025u64 {
                let key = format!("list-prefix/{index:04}").into_bytes();
                store
                    .put_blob(&key, Bytes::from_static(b"v"))
                    .await
                    .expect("seed blob");
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
