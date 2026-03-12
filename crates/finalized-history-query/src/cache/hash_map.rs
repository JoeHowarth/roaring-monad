use std::collections::HashMap;
use std::sync::Mutex;

use bytes::Bytes;

use super::TableId;

/// Simple HashMap-based BytesCache. No eviction, unbounded.
/// Suitable for per-request or bounded-lifetime usage.
pub struct HashMapBytesCache {
    inner: Mutex<HashMap<(TableId, Vec<u8>), Bytes>>,
}

impl HashMapBytesCache {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }
}

impl Default for HashMapBytesCache {
    fn default() -> Self {
        Self::new()
    }
}

impl super::BytesCache for HashMapBytesCache {
    fn get(&self, table: TableId, key: &[u8]) -> Option<Bytes> {
        let guard = self.inner.lock().unwrap();
        guard.get(&(table, key.to_vec())).cloned()
    }

    fn put(&self, table: TableId, key: &[u8], value: Bytes, _weight: usize) {
        let mut guard = self.inner.lock().unwrap();
        guard.insert((table, key.to_vec()), value);
    }
}
