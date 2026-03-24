use crate::kernel::cache::BytesCacheConfig;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;

pub struct Runtime<M: MetaStore, B: BlobStore> {
    pub meta_store: M,
    pub blob_store: B,
    pub tables: Tables<M, B>,
}

impl<M: MetaStore, B: BlobStore> Runtime<M, B> {
    pub fn new(meta_store: M, blob_store: B, bytes_cache: BytesCacheConfig) -> Self {
        let tables = Tables::new(meta_store.clone(), blob_store.clone(), bytes_cache);
        Self {
            meta_store,
            blob_store,
            tables,
        }
    }
}
