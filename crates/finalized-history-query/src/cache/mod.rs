mod hash_map;

pub use hash_map::{HashMapBytesCache, HashMapTableBytesCache};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TableId {
    BlockLogHeaders,
    DirBuckets,
    LogDirSubBuckets,
    PointLogPayloads,
    BitmapPageMeta,
    BitmapPageBlobs,
}

impl TableId {
    pub const COUNT: usize = 6;
    pub const ALL: [Self; Self::COUNT] = [
        Self::BlockLogHeaders,
        Self::DirBuckets,
        Self::LogDirSubBuckets,
        Self::PointLogPayloads,
        Self::BitmapPageMeta,
        Self::BitmapPageBlobs,
    ];

    pub const fn as_index(self) -> usize {
        match self {
            Self::BlockLogHeaders => 0,
            Self::DirBuckets => 1,
            Self::LogDirSubBuckets => 2,
            Self::PointLogPayloads => 3,
            Self::BitmapPageMeta => 4,
            Self::BitmapPageBlobs => 5,
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
    pub block_log_header: TableCacheConfig,
    pub log_dir_buckets: TableCacheConfig,
    pub log_dir_sub_buckets: TableCacheConfig,
    pub point_log_payloads: TableCacheConfig,
    pub bitmap_page_meta: TableCacheConfig,
    pub bitmap_page_blobs: TableCacheConfig,
}

impl BytesCacheConfig {
    pub const fn disabled() -> Self {
        Self {
            block_log_header: TableCacheConfig::disabled(),
            log_dir_buckets: TableCacheConfig::disabled(),
            log_dir_sub_buckets: TableCacheConfig::disabled(),
            point_log_payloads: TableCacheConfig::disabled(),
            bitmap_page_meta: TableCacheConfig::disabled(),
            bitmap_page_blobs: TableCacheConfig::disabled(),
        }
    }

    pub const fn table(self, table: TableId) -> TableCacheConfig {
        match table {
            TableId::BlockLogHeaders => self.block_log_header,
            TableId::DirBuckets => self.log_dir_buckets,
            TableId::LogDirSubBuckets => self.log_dir_sub_buckets,
            TableId::PointLogPayloads => self.point_log_payloads,
            TableId::BitmapPageMeta => self.bitmap_page_meta,
            TableId::BitmapPageBlobs => self.bitmap_page_blobs,
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
    pub block_log_header: TableCacheMetrics,
    pub log_dir_buckets: TableCacheMetrics,
    pub log_dir_sub_buckets: TableCacheMetrics,
    pub point_log_payloads: TableCacheMetrics,
    pub bitmap_page_meta: TableCacheMetrics,
    pub bitmap_page_blobs: TableCacheMetrics,
}

impl BytesCacheMetrics {
    pub const fn table(self, table: TableId) -> TableCacheMetrics {
        match table {
            TableId::BlockLogHeaders => self.block_log_header,
            TableId::DirBuckets => self.log_dir_buckets,
            TableId::LogDirSubBuckets => self.log_dir_sub_buckets,
            TableId::PointLogPayloads => self.point_log_payloads,
            TableId::BitmapPageMeta => self.bitmap_page_meta,
            TableId::BitmapPageBlobs => self.bitmap_page_blobs,
        }
    }
}
