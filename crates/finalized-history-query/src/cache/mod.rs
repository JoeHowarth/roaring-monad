mod hash_map;

use bytes::Bytes;

pub use hash_map::HashMapBytesCache;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TableId {
    BlockLogHeaders,
    LogDirectoryBuckets,
    LogDirectorySubBuckets,
    BlockLogBlobs,
    StreamPages,
}

impl TableId {
    pub const COUNT: usize = 5;
    pub const ALL: [Self; Self::COUNT] = [
        Self::BlockLogHeaders,
        Self::LogDirectoryBuckets,
        Self::LogDirectorySubBuckets,
        Self::BlockLogBlobs,
        Self::StreamPages,
    ];

    pub const fn as_index(self) -> usize {
        match self {
            Self::BlockLogHeaders => 0,
            Self::LogDirectoryBuckets => 1,
            Self::LogDirectorySubBuckets => 2,
            Self::BlockLogBlobs => 3,
            Self::StreamPages => 4,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableCacheConfig {
    pub max_bytes: u64,
}

impl TableCacheConfig {
    pub const fn disabled() -> Self {
        Self { max_bytes: 0 }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BytesCacheConfig {
    pub block_log_headers: TableCacheConfig,
    pub log_directory_buckets: TableCacheConfig,
    pub log_directory_sub_buckets: TableCacheConfig,
    pub block_log_blobs: TableCacheConfig,
    pub stream_pages: TableCacheConfig,
}

impl BytesCacheConfig {
    pub const fn disabled() -> Self {
        Self {
            block_log_headers: TableCacheConfig::disabled(),
            log_directory_buckets: TableCacheConfig::disabled(),
            log_directory_sub_buckets: TableCacheConfig::disabled(),
            block_log_blobs: TableCacheConfig::disabled(),
            stream_pages: TableCacheConfig::disabled(),
        }
    }

    pub const fn table(self, table: TableId) -> TableCacheConfig {
        match table {
            TableId::BlockLogHeaders => self.block_log_headers,
            TableId::LogDirectoryBuckets => self.log_directory_buckets,
            TableId::LogDirectorySubBuckets => self.log_directory_sub_buckets,
            TableId::BlockLogBlobs => self.block_log_blobs,
            TableId::StreamPages => self.stream_pages,
        }
    }
}

impl Default for BytesCacheConfig {
    fn default() -> Self {
        Self::disabled()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TableCacheMetrics {
    pub hits: u64,
    pub misses: u64,
    pub inserts: u64,
    pub evictions: u64,
    pub bytes_used: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BytesCacheMetrics {
    pub block_log_headers: TableCacheMetrics,
    pub log_directory_buckets: TableCacheMetrics,
    pub log_directory_sub_buckets: TableCacheMetrics,
    pub block_log_blobs: TableCacheMetrics,
    pub stream_pages: TableCacheMetrics,
}

impl BytesCacheMetrics {
    pub const fn table(self, table: TableId) -> TableCacheMetrics {
        match table {
            TableId::BlockLogHeaders => self.block_log_headers,
            TableId::LogDirectoryBuckets => self.log_directory_buckets,
            TableId::LogDirectorySubBuckets => self.log_directory_sub_buckets,
            TableId::BlockLogBlobs => self.block_log_blobs,
            TableId::StreamPages => self.stream_pages,
        }
    }
}

pub trait BytesCache: Send + Sync {
    fn is_enabled(&self, table: TableId) -> bool;

    /// Returns a cached value if present. Cloning Bytes is an Arc increment.
    fn get(&self, table: TableId, key: &[u8]) -> Option<Bytes>;

    /// Inserts a value with its weight in bytes for capacity accounting.
    fn put(&self, table: TableId, key: &[u8], value: Bytes, weight: usize);
}

/// No-op cache that never stores anything. Useful as default when no cache is configured.
pub struct NoopBytesCache;

impl BytesCache for NoopBytesCache {
    fn is_enabled(&self, _table: TableId) -> bool {
        false
    }

    fn get(&self, _table: TableId, _key: &[u8]) -> Option<Bytes> {
        None
    }

    fn put(&self, _table: TableId, _key: &[u8], _value: Bytes, _weight: usize) {}
}
