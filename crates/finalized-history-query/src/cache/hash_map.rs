use std::collections::HashMap;
use std::sync::Mutex;

use bytes::Bytes;

use super::{BytesCacheConfig, BytesCacheMetrics, TableCacheMetrics};

#[derive(Debug)]
struct CacheEntry {
    value: Bytes,
    weight: u64,
    last_touch: u64,
}

#[derive(Debug, Default)]
struct TableState {
    clock: u64,
    bytes_used: u64,
    hits: u64,
    misses: u64,
    inserts: u64,
    evictions: u64,
    entries: HashMap<Vec<u8>, CacheEntry>,
}

/// Simple in-process bytes cache for a single immutable artifact family.
#[derive(Debug)]
pub struct HashMapTableBytesCache {
    max_bytes: u64,
    inner: Mutex<TableState>,
}

impl HashMapTableBytesCache {
    pub fn new(max_bytes: u64) -> Self {
        Self {
            max_bytes,
            inner: Mutex::new(TableState::default()),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.max_bytes > 0
    }

    fn next_touch(state: &mut TableState) -> u64 {
        state.clock = state.clock.wrapping_add(1);
        state.clock
    }

    fn evict_lru(table_state: &mut TableState) {
        let Some((lru_key, _)) = table_state
            .entries
            .iter()
            .min_by_key(|(_, entry)| entry.last_touch)
            .map(|(key, entry)| (key.clone(), entry.last_touch))
        else {
            return;
        };
        if let Some(removed) = table_state.entries.remove(&lru_key) {
            table_state.bytes_used = table_state.bytes_used.saturating_sub(removed.weight);
            table_state.evictions = table_state.evictions.saturating_add(1);
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        if !self.is_enabled() {
            return None;
        }

        let mut guard = self.inner.lock().unwrap();
        let touch = Self::next_touch(&mut guard);
        let value = match guard.entries.get_mut(key) {
            Some(entry) => {
                entry.last_touch = touch;
                entry.value.clone()
            }
            None => {
                guard.misses = guard.misses.saturating_add(1);
                return None;
            }
        };
        guard.hits = guard.hits.saturating_add(1);
        Some(value)
    }

    pub fn put(&self, key: &[u8], value: Bytes, weight: usize) {
        if self.max_bytes == 0 {
            return;
        }

        let weight = u64::try_from(weight).unwrap_or(u64::MAX);
        let mut guard = self.inner.lock().unwrap();
        let touch = Self::next_touch(&mut guard);

        if let Some(existing) = guard.entries.remove(key) {
            guard.bytes_used = guard.bytes_used.saturating_sub(existing.weight);
        }

        if weight > self.max_bytes {
            return;
        }

        guard.bytes_used = guard.bytes_used.saturating_add(weight);
        guard.inserts = guard.inserts.saturating_add(1);
        guard.entries.insert(
            key.to_vec(),
            CacheEntry {
                value,
                weight,
                last_touch: touch,
            },
        );

        while guard.bytes_used > self.max_bytes {
            Self::evict_lru(&mut guard);
        }
    }

    pub fn metrics_snapshot(&self) -> TableCacheMetrics {
        let guard = self.inner.lock().unwrap();
        TableCacheMetrics {
            hits: guard.hits,
            misses: guard.misses,
            inserts: guard.inserts,
            evictions: guard.evictions,
            bytes_used: guard.bytes_used,
        }
    }
}

impl Default for HashMapTableBytesCache {
    fn default() -> Self {
        Self::new(0)
    }
}

/// Concrete cache set with one independent cache instance per artifact family.
#[derive(Debug)]
pub struct HashMapBytesCache {
    block_log_headers: HashMapTableBytesCache,
    dir_buckets: HashMapTableBytesCache,
    log_dir_sub_buckets: HashMapTableBytesCache,
    point_log_payloads: HashMapTableBytesCache,
    bitmap_page_meta: HashMapTableBytesCache,
    bitmap_page_blobs: HashMapTableBytesCache,
}

impl HashMapBytesCache {
    pub fn new(config: BytesCacheConfig) -> Self {
        Self {
            block_log_headers: HashMapTableBytesCache::new(config.block_log_header.max_bytes),
            dir_buckets: HashMapTableBytesCache::new(config.log_dir_buckets.max_bytes),
            log_dir_sub_buckets: HashMapTableBytesCache::new(config.log_dir_sub_buckets.max_bytes),
            point_log_payloads: HashMapTableBytesCache::new(config.point_log_payloads.max_bytes),
            bitmap_page_meta: HashMapTableBytesCache::new(config.bitmap_page_meta.max_bytes),
            bitmap_page_blobs: HashMapTableBytesCache::new(config.bitmap_page_blobs.max_bytes),
        }
    }

    pub fn block_log_headers(&self) -> &HashMapTableBytesCache {
        &self.block_log_headers
    }

    pub fn dir_buckets(&self) -> &HashMapTableBytesCache {
        &self.dir_buckets
    }

    pub fn log_dir_sub_buckets(&self) -> &HashMapTableBytesCache {
        &self.log_dir_sub_buckets
    }

    pub fn point_log_payloads(&self) -> &HashMapTableBytesCache {
        &self.point_log_payloads
    }

    pub fn bitmap_page_meta(&self) -> &HashMapTableBytesCache {
        &self.bitmap_page_meta
    }

    pub fn bitmap_page_blobs(&self) -> &HashMapTableBytesCache {
        &self.bitmap_page_blobs
    }

    pub fn metrics_snapshot(&self) -> BytesCacheMetrics {
        BytesCacheMetrics {
            block_log_header: self.block_log_headers.metrics_snapshot(),
            log_dir_buckets: self.dir_buckets.metrics_snapshot(),
            log_dir_sub_buckets: self.log_dir_sub_buckets.metrics_snapshot(),
            point_log_payloads: self.point_log_payloads.metrics_snapshot(),
            bitmap_page_meta: self.bitmap_page_meta.metrics_snapshot(),
            bitmap_page_blobs: self.bitmap_page_blobs.metrics_snapshot(),
        }
    }
}

impl Default for HashMapBytesCache {
    fn default() -> Self {
        Self::new(BytesCacheConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[test]
    fn disabled_tables_are_bypassed() {
        let cache = HashMapTableBytesCache::default();
        cache.put(b"header", Bytes::from_static(b"abc"), 3);

        assert!(!cache.is_enabled());
        assert_eq!(cache.get(b"header"), None);
    }

    #[test]
    fn evicts_least_recently_used_within_budget() {
        let cache = HashMapTableBytesCache::new(5);

        cache.put(b"a", Bytes::from_static(b"aa"), 2);
        cache.put(b"b", Bytes::from_static(b"bb"), 2);
        let _ = cache.get(b"a");
        cache.put(b"c", Bytes::from_static(b"cc"), 2);

        assert_eq!(cache.get(b"a"), Some(Bytes::from_static(b"aa")));
        assert_eq!(cache.get(b"b"), None);
        assert_eq!(cache.get(b"c"), Some(Bytes::from_static(b"cc")));
    }

    #[test]
    fn reports_metrics() {
        let cache = HashMapTableBytesCache::new(3);

        assert_eq!(cache.metrics_snapshot(), TableCacheMetrics::default());

        assert_eq!(cache.get(b"missing"), None);
        cache.put(b"a", Bytes::from_static(b"aa"), 2);
        let _ = cache.get(b"a");
        cache.put(b"b", Bytes::from_static(b"bb"), 2);

        let metrics = cache.metrics_snapshot();
        assert_eq!(metrics.hits, 1);
        assert_eq!(metrics.misses, 1);
        assert_eq!(metrics.inserts, 2);
        assert_eq!(metrics.evictions, 1);
        assert_eq!(metrics.bytes_used, 2);
    }

    #[test]
    fn cache_set_keeps_metrics_isolated_per_table() {
        let cache = HashMapBytesCache::new(BytesCacheConfig {
            block_log_header: super::super::TableCacheConfig { max_bytes: 2 },
            log_dir_buckets: super::super::TableCacheConfig { max_bytes: 2 },
            ..BytesCacheConfig::disabled()
        });

        cache
            .block_log_headers()
            .put(b"a", Bytes::from_static(b"aa"), 2);
        cache.dir_buckets().put(b"a", Bytes::from_static(b"bb"), 2);
        cache
            .block_log_headers()
            .put(b"b", Bytes::from_static(b"cc"), 2);

        assert_eq!(cache.block_log_headers().get(b"a"), None);
        assert_eq!(
            cache.block_log_headers().get(b"b"),
            Some(Bytes::from_static(b"cc"))
        );
        assert_eq!(
            cache.dir_buckets().get(b"a"),
            Some(Bytes::from_static(b"bb"))
        );
    }
}
