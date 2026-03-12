mod hash_map;

use bytes::Bytes;

pub use hash_map::HashMapBytesCache;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TableId {
    BlockLogHeaders,
    LogDirectoryBuckets,
    LogDirectorySubBuckets,
    BlockLogBlobs,
}

pub trait BytesCache: Send + Sync {
    /// Returns a cached value if present. Cloning Bytes is an Arc increment.
    fn get(&self, table: TableId, key: &[u8]) -> Option<Bytes>;

    /// Inserts a value with its weight in bytes for capacity accounting.
    fn put(&self, table: TableId, key: &[u8], value: Bytes, weight: usize);
}

/// No-op cache that never stores anything. Useful as default when no cache is configured.
pub struct NoopBytesCache;

impl BytesCache for NoopBytesCache {
    fn get(&self, _table: TableId, _key: &[u8]) -> Option<Bytes> {
        None
    }

    fn put(&self, _table: TableId, _key: &[u8], _value: Bytes, _weight: usize) {}
}
