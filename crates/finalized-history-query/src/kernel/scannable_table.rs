use bytes::Bytes;

use crate::error::Result;
use crate::kernel::cache::{HashMapTableBytesCache, TableCacheMetrics};
use crate::store::traits::{MetaStore, PutCond, ScannableKvTable};

pub struct ScannableFragmentTable<M: MetaStore> {
    table: ScannableKvTable<M>,
    cache: HashMapTableBytesCache,
}

impl<M: MetaStore> ScannableFragmentTable<M> {
    pub fn new(table: ScannableKvTable<M>, cache: HashMapTableBytesCache) -> Self {
        Self { table, cache }
    }

    pub async fn load_partition_values(&self, partition: &[u8]) -> Result<Vec<Bytes>> {
        let mut cursor = None;
        let mut values = Vec::new();

        loop {
            let page = self
                .table
                .list_prefix(partition, b"", cursor.take(), 1_024)
                .await?;
            for clustering in page.keys {
                let cache_key = composite_cache_key(partition, &clustering);
                if let Some(cached) = self.cache.get(&cache_key) {
                    values.push(cached);
                    continue;
                }
                let Some(record) = self.table.get(partition, &clustering).await? else {
                    continue;
                };
                self.cache
                    .put(&cache_key, record.value.clone(), record.value.len());
                values.push(record.value);
            }
            if page.next_cursor.is_none() {
                break;
            }
            cursor = page.next_cursor;
        }

        Ok(values)
    }

    pub async fn put_value(&self, partition: &[u8], clustering: &[u8], value: Bytes) -> Result<()> {
        let cache_key = composite_cache_key(partition, clustering);
        let len = value.len();
        let _ = self
            .table
            .put(partition, clustering, value.clone(), PutCond::Any)
            .await?;
        self.cache.put(&cache_key, value, len);
        Ok(())
    }

    pub fn metrics(&self) -> TableCacheMetrics {
        self.cache.metrics_snapshot()
    }
}

fn composite_cache_key(partition: &[u8], clustering: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(4 + partition.len() + clustering.len());
    key.extend_from_slice(&(partition.len() as u32).to_le_bytes());
    key.extend_from_slice(partition);
    key.extend_from_slice(clustering);
    key
}
