use std::array;
use std::collections::HashMap;
use std::sync::Mutex;

use bytes::Bytes;

use super::{BytesCache, BytesCacheConfig, BytesCacheMetrics, TableCacheMetrics, TableId};

#[derive(Debug)]
struct CacheEntry {
    value: Bytes,
    weight: u64,
    last_touch: u64,
}

#[derive(Debug, Default)]
struct TableState {
    bytes_used: u64,
    hits: u64,
    misses: u64,
    inserts: u64,
    evictions: u64,
    entries: HashMap<Vec<u8>, CacheEntry>,
}

#[derive(Debug)]
struct CacheState {
    clock: u64,
    tables: [TableState; TableId::COUNT],
}

impl Default for CacheState {
    fn default() -> Self {
        Self {
            clock: 0,
            tables: array::from_fn(|_| TableState::default()),
        }
    }
}

/// Simple in-process BytesCache with per-table byte budgets and LRU eviction.
pub struct HashMapBytesCache {
    max_bytes: [u64; TableId::COUNT],
    inner: Mutex<CacheState>,
}

impl HashMapBytesCache {
    pub fn new(config: BytesCacheConfig) -> Self {
        Self {
            max_bytes: TableId::ALL.map(|table| config.table(table).max_bytes),
            inner: Mutex::new(CacheState::default()),
        }
    }

    fn max_bytes_for(&self, table: TableId) -> u64 {
        self.max_bytes[table.as_index()]
    }

    fn next_touch(state: &mut CacheState) -> u64 {
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

    pub fn metrics_snapshot(&self) -> BytesCacheMetrics {
        let guard = self.inner.lock().unwrap();
        BytesCacheMetrics {
            block_log_header: table_metrics(&guard.tables[TableId::BlockLogHeaders.as_index()]),
            log_dir_buckets: table_metrics(&guard.tables[TableId::DirBuckets.as_index()]),
            log_dir_sub_buckets: table_metrics(&guard.tables[TableId::LogDirSubBuckets.as_index()]),
            point_log_payloads: table_metrics(&guard.tables[TableId::PointLogPayloads.as_index()]),
            bitmap_page_meta: table_metrics(&guard.tables[TableId::BitmapPageMeta.as_index()]),
            bitmap_page_blobs: table_metrics(&guard.tables[TableId::BitmapPageBlobs.as_index()]),
        }
    }
}

fn table_metrics(state: &TableState) -> TableCacheMetrics {
    TableCacheMetrics {
        hits: state.hits,
        misses: state.misses,
        inserts: state.inserts,
        evictions: state.evictions,
        bytes_used: state.bytes_used,
    }
}

impl Default for HashMapBytesCache {
    fn default() -> Self {
        Self::new(BytesCacheConfig::default())
    }
}

impl BytesCache for HashMapBytesCache {
    fn is_enabled(&self, table: TableId) -> bool {
        self.max_bytes_for(table) > 0
    }

    fn get(&self, table: TableId, key: &[u8]) -> Option<Bytes> {
        if !self.is_enabled(table) {
            return None;
        }

        let mut guard = self.inner.lock().unwrap();
        let touch = Self::next_touch(&mut guard);
        let table_state = &mut guard.tables[table.as_index()];
        let Some(entry) = table_state.entries.get_mut(key) else {
            table_state.misses = table_state.misses.saturating_add(1);
            return None;
        };
        table_state.hits = table_state.hits.saturating_add(1);
        entry.last_touch = touch;
        Some(entry.value.clone())
    }

    fn put(&self, table: TableId, key: &[u8], value: Bytes, weight: usize) {
        let max_bytes = self.max_bytes_for(table);
        if max_bytes == 0 {
            return;
        }

        let weight = u64::try_from(weight).unwrap_or(u64::MAX);
        let mut guard = self.inner.lock().unwrap();
        let touch = Self::next_touch(&mut guard);
        let table_state = &mut guard.tables[table.as_index()];

        if let Some(existing) = table_state.entries.remove(key) {
            table_state.bytes_used = table_state.bytes_used.saturating_sub(existing.weight);
        }

        if weight > max_bytes {
            return;
        }

        table_state.bytes_used = table_state.bytes_used.saturating_add(weight);
        table_state.inserts = table_state.inserts.saturating_add(1);
        table_state.entries.insert(
            key.to_vec(),
            CacheEntry {
                value,
                weight,
                last_touch: touch,
            },
        );

        while table_state.bytes_used > max_bytes {
            Self::evict_lru(table_state);
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[test]
    fn disabled_tables_are_bypassed() {
        let cache = HashMapBytesCache::new(BytesCacheConfig::disabled());
        cache.put(
            TableId::BlockLogHeaders,
            b"header",
            Bytes::from_static(b"abc"),
            3,
        );

        assert!(!cache.is_enabled(TableId::BlockLogHeaders));
        assert_eq!(cache.get(TableId::BlockLogHeaders, b"header"), None);
    }

    #[test]
    fn evicts_least_recently_used_within_a_table_budget() {
        let cache = HashMapBytesCache::new(BytesCacheConfig {
            block_log_header: super::super::TableCacheConfig { max_bytes: 5 },
            ..BytesCacheConfig::disabled()
        });

        cache.put(TableId::BlockLogHeaders, b"a", Bytes::from_static(b"aa"), 2);
        cache.put(TableId::BlockLogHeaders, b"b", Bytes::from_static(b"bb"), 2);
        let _ = cache.get(TableId::BlockLogHeaders, b"a");
        cache.put(TableId::BlockLogHeaders, b"c", Bytes::from_static(b"cc"), 2);

        assert_eq!(
            cache.get(TableId::BlockLogHeaders, b"a"),
            Some(Bytes::from_static(b"aa"))
        );
        assert_eq!(cache.get(TableId::BlockLogHeaders, b"b"), None);
        assert_eq!(
            cache.get(TableId::BlockLogHeaders, b"c"),
            Some(Bytes::from_static(b"cc"))
        );
    }

    #[test]
    fn budgets_are_isolated_per_table() {
        let cache = HashMapBytesCache::new(BytesCacheConfig {
            block_log_header: super::super::TableCacheConfig { max_bytes: 2 },
            log_dir_buckets: super::super::TableCacheConfig { max_bytes: 2 },
            ..BytesCacheConfig::disabled()
        });

        cache.put(TableId::BlockLogHeaders, b"a", Bytes::from_static(b"aa"), 2);
        cache.put(TableId::DirBuckets, b"a", Bytes::from_static(b"bb"), 2);
        cache.put(TableId::BlockLogHeaders, b"b", Bytes::from_static(b"cc"), 2);

        assert_eq!(cache.get(TableId::BlockLogHeaders, b"a"), None);
        assert_eq!(
            cache.get(TableId::BlockLogHeaders, b"b"),
            Some(Bytes::from_static(b"cc"))
        );
        assert_eq!(
            cache.get(TableId::DirBuckets, b"a"),
            Some(Bytes::from_static(b"bb"))
        );
    }

    #[test]
    fn reports_per_table_metrics() {
        let cache = HashMapBytesCache::new(BytesCacheConfig {
            block_log_header: super::super::TableCacheConfig { max_bytes: 3 },
            ..BytesCacheConfig::disabled()
        });

        assert_eq!(
            cache.metrics_snapshot().block_log_header,
            TableCacheMetrics::default()
        );

        assert_eq!(cache.get(TableId::BlockLogHeaders, b"missing"), None);
        cache.put(TableId::BlockLogHeaders, b"a", Bytes::from_static(b"aa"), 2);
        let _ = cache.get(TableId::BlockLogHeaders, b"a");
        cache.put(TableId::BlockLogHeaders, b"b", Bytes::from_static(b"bb"), 2);

        let metrics = cache.metrics_snapshot().block_log_header;
        assert_eq!(metrics.hits, 1);
        assert_eq!(metrics.misses, 1);
        assert_eq!(metrics.inserts, 2);
        assert_eq!(metrics.evictions, 1);
        assert_eq!(metrics.bytes_used, 2);
    }
}
