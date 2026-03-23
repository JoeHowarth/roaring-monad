use crate::kernel::cache::BytesCacheConfig;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;

pub struct Runtime<M: MetaStore, B: BlobStore> {
    meta_store: M,
    blob_store: B,
    tables: Tables<M, B>,
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

    pub fn meta_store(&self) -> &M {
        &self.meta_store
    }

    pub fn blob_store(&self) -> &B {
        &self.blob_store
    }

    pub fn tables(&self) -> &Tables<M, B> {
        &self.tables
    }
}
