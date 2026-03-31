mod in_memory;

use std::sync::Arc;

use bytes::Bytes;

use crate::error::{MonadChainDataError, Result};
use crate::store::common::Page;

pub use in_memory::InMemoryBlobStore;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlobTableId(&'static str);

impl BlobTableId {
    pub const fn new(name: &'static str) -> Self {
        Self(name)
    }

    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

#[derive(Debug)]
pub struct BlobTable<B> {
    store: B,
    pub table: BlobTableId,
}

impl<B> BlobTable<B> {
    pub fn new(store: B, table: BlobTableId) -> Self {
        Self { store, table }
    }
}

impl<B: Clone> Clone for BlobTable<B> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            table: self.table,
        }
    }
}

impl<B: BlobStore> BlobTable<B> {
    pub async fn put(&self, key: &[u8], value: Bytes) -> Result<()> {
        self.store.put_blob(self.table, key, value).await
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.store.get_blob(self.table, key).await
    }

    pub async fn read_range(
        &self,
        key: &[u8],
        start: u64,
        end_exclusive: u64,
    ) -> Result<Option<Bytes>> {
        self.store
            .read_range(self.table, key, start, end_exclusive)
            .await
    }

    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        self.store.delete_blob(self.table, key).await
    }

    pub async fn list_prefix(
        &self,
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page> {
        self.store
            .list_prefix(self.table, prefix, cursor, limit)
            .await
    }
}

#[derive(Debug, Clone, Copy)]
pub struct BlobTableRef<'a, B> {
    store: &'a B,
    pub table: BlobTableId,
}

impl<'a, B> BlobTableRef<'a, B> {
    pub fn new(store: &'a B, table: BlobTableId) -> Self {
        Self { store, table }
    }
}

impl<B: BlobStore> BlobTableRef<'_, B> {
    pub async fn put(&self, key: &[u8], value: Bytes) -> Result<()> {
        self.store.put_blob(self.table, key, value).await
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.store.get_blob(self.table, key).await
    }

    pub async fn read_range(
        &self,
        key: &[u8],
        start: u64,
        end_exclusive: u64,
    ) -> Result<Option<Bytes>> {
        self.store
            .read_range(self.table, key, start, end_exclusive)
            .await
    }

    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        self.store.delete_blob(self.table, key).await
    }

    pub async fn list_prefix(
        &self,
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page> {
        self.store
            .list_prefix(self.table, prefix, cursor, limit)
            .await
    }
}

#[allow(async_fn_in_trait)]
pub trait BlobStore: Clone + Send + Sync {
    fn table(&self, table: BlobTableId) -> BlobTable<Self>
    where
        Self: Sized,
    {
        BlobTable::new(self.clone(), table)
    }

    fn table_ref(&self, table: BlobTableId) -> BlobTableRef<'_, Self>
    where
        Self: Sized,
    {
        BlobTableRef::new(self, table)
    }

    async fn put_blob(&self, table: BlobTableId, key: &[u8], value: Bytes) -> Result<()>;
    async fn get_blob(&self, table: BlobTableId, key: &[u8]) -> Result<Option<Bytes>>;
    async fn read_range(
        &self,
        table: BlobTableId,
        key: &[u8],
        start: u64,
        end_exclusive: u64,
    ) -> Result<Option<Bytes>> {
        let Some(blob) = self.get_blob(table, key).await? else {
            return Ok(None);
        };
        let start = usize::try_from(start)
            .map_err(|_| MonadChainDataError::Decode("blob range start overflow"))?;
        let end = usize::try_from(end_exclusive)
            .map_err(|_| MonadChainDataError::Decode("blob range end overflow"))?;
        if start > end || end > blob.len() {
            return Err(MonadChainDataError::Decode("invalid blob range"));
        }
        Ok(Some(blob.slice(start..end)))
    }
    async fn delete_blob(&self, table: BlobTableId, key: &[u8]) -> Result<()>;
    async fn list_prefix(
        &self,
        table: BlobTableId,
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page>;
}

impl<T: BlobStore> BlobStore for Arc<T> {
    async fn put_blob(&self, table: BlobTableId, key: &[u8], value: Bytes) -> Result<()> {
        self.as_ref().put_blob(table, key, value).await
    }

    async fn get_blob(&self, table: BlobTableId, key: &[u8]) -> Result<Option<Bytes>> {
        self.as_ref().get_blob(table, key).await
    }

    async fn read_range(
        &self,
        table: BlobTableId,
        key: &[u8],
        start: u64,
        end_exclusive: u64,
    ) -> Result<Option<Bytes>> {
        self.as_ref()
            .read_range(table, key, start, end_exclusive)
            .await
    }

    async fn delete_blob(&self, table: BlobTableId, key: &[u8]) -> Result<()> {
        self.as_ref().delete_blob(table, key).await
    }

    async fn list_prefix(
        &self,
        table: BlobTableId,
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page> {
        self.as_ref()
            .list_prefix(table, prefix, cursor, limit)
            .await
    }
}
