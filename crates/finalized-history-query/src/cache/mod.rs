mod hash_map;

use bytes::Bytes;

pub use hash_map::HashMapBytesCache;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TableId {
    BlockLogHeaders,
    LogDirectoryBuckets,
    LogDirectorySubBuckets,
    PointLogPayloads,
    StreamPageMeta,
    StreamPageBlobs,
}

impl TableId {
    pub const COUNT: usize = 6;
    pub const ALL: [Self; Self::COUNT] = [
        Self::BlockLogHeaders,
        Self::LogDirectoryBuckets,
        Self::LogDirectorySubBuckets,
        Self::PointLogPayloads,
        Self::StreamPageMeta,
        Self::StreamPageBlobs,
    ];

    pub const fn as_index(self) -> usize {
        match self {
            Self::BlockLogHeaders => 0,
            Self::LogDirectoryBuckets => 1,
            Self::LogDirectorySubBuckets => 2,
            Self::PointLogPayloads => 3,
            Self::StreamPageMeta => 4,
            Self::StreamPageBlobs => 5,
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
    pub point_log_payloads: TableCacheConfig,
    pub stream_page_meta: TableCacheConfig,
    pub stream_page_blobs: TableCacheConfig,
}

impl BytesCacheConfig {
    pub const fn disabled() -> Self {
        Self {
            block_log_headers: TableCacheConfig::disabled(),
            log_directory_buckets: TableCacheConfig::disabled(),
            log_directory_sub_buckets: TableCacheConfig::disabled(),
            point_log_payloads: TableCacheConfig::disabled(),
            stream_page_meta: TableCacheConfig::disabled(),
            stream_page_blobs: TableCacheConfig::disabled(),
        }
    }

    pub const fn table(self, table: TableId) -> TableCacheConfig {
        match table {
            TableId::BlockLogHeaders => self.block_log_headers,
            TableId::LogDirectoryBuckets => self.log_directory_buckets,
            TableId::LogDirectorySubBuckets => self.log_directory_sub_buckets,
            TableId::PointLogPayloads => self.point_log_payloads,
            TableId::StreamPageMeta => self.stream_page_meta,
            TableId::StreamPageBlobs => self.stream_page_blobs,
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
    pub point_log_payloads: TableCacheMetrics,
    pub stream_page_meta: TableCacheMetrics,
    pub stream_page_blobs: TableCacheMetrics,
}

impl BytesCacheMetrics {
    pub const fn table(self, table: TableId) -> TableCacheMetrics {
        match table {
            TableId::BlockLogHeaders => self.block_log_headers,
            TableId::LogDirectoryBuckets => self.log_directory_buckets,
            TableId::LogDirectorySubBuckets => self.log_directory_sub_buckets,
            TableId::PointLogPayloads => self.point_log_payloads,
            TableId::StreamPageMeta => self.stream_page_meta,
            TableId::StreamPageBlobs => self.stream_page_blobs,
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
