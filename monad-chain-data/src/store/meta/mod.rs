mod in_memory;

use std::sync::Arc;

use bytes::Bytes;

use crate::error::Result;
use crate::store::common::Page;

pub use in_memory::InMemoryMetaStore;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableId(&'static str);

impl TableId {
    pub const fn new(name: &'static str) -> Self {
        Self(name)
    }

    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ScannableTableId(&'static str);

impl ScannableTableId {
    pub const fn new(name: &'static str) -> Self {
        Self(name)
    }

    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

#[derive(Debug, Clone)]
pub struct Record {
    pub value: Bytes,
    pub version: u64,
}

#[derive(Debug, Clone, Copy)]
pub enum DelCond {
    Any,
    IfVersion(u64),
}

#[derive(Debug, Clone)]
pub struct PutResult {
    pub applied: bool,
    pub version: Option<u64>,
}

#[derive(Debug)]
pub struct KvTable<M> {
    store: M,
    pub table: TableId,
}

impl<M> KvTable<M> {
    pub fn new(store: M, table: TableId) -> Self {
        Self { store, table }
    }
}

impl<M: Clone> Clone for KvTable<M> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            table: self.table,
        }
    }
}

impl<M: MetaStore> KvTable<M> {
    pub async fn get(&self, key: &[u8]) -> Result<Option<Record>> {
        self.store.get(self.table, key).await
    }

    pub async fn put(&self, key: &[u8], value: Bytes) -> Result<PutResult> {
        self.store.put(self.table, key, value).await
    }

    pub async fn delete(&self, key: &[u8], cond: DelCond) -> Result<()> {
        self.store.delete(self.table, key, cond).await
    }
}

#[derive(Debug, Clone, Copy)]
pub struct KvTableRef<'a, M> {
    store: &'a M,
    pub table: TableId,
}

impl<'a, M> KvTableRef<'a, M> {
    pub fn new(store: &'a M, table: TableId) -> Self {
        Self { store, table }
    }
}

impl<M: MetaStore> KvTableRef<'_, M> {
    pub async fn get(&self, key: &[u8]) -> Result<Option<Record>> {
        self.store.get(self.table, key).await
    }

    pub async fn put(&self, key: &[u8], value: Bytes) -> Result<PutResult> {
        self.store.put(self.table, key, value).await
    }

    pub async fn delete(&self, key: &[u8], cond: DelCond) -> Result<()> {
        self.store.delete(self.table, key, cond).await
    }
}

#[derive(Debug)]
pub struct ScannableKvTable<M> {
    store: M,
    pub table: ScannableTableId,
}

impl<M> ScannableKvTable<M> {
    pub fn new(store: M, table: ScannableTableId) -> Self {
        Self { store, table }
    }
}

impl<M: Clone> Clone for ScannableKvTable<M> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            table: self.table,
        }
    }
}

impl<M: MetaStore> ScannableKvTable<M> {
    pub async fn get(&self, partition: &[u8], clustering: &[u8]) -> Result<Option<Record>> {
        self.store.scan_get(self.table, partition, clustering).await
    }

    pub async fn put(
        &self,
        partition: &[u8],
        clustering: &[u8],
        value: Bytes,
    ) -> Result<PutResult> {
        self.store
            .scan_put(self.table, partition, clustering, value)
            .await
    }

    pub async fn delete(&self, partition: &[u8], clustering: &[u8], cond: DelCond) -> Result<()> {
        self.store
            .scan_delete(self.table, partition, clustering, cond)
            .await
    }

    pub async fn list_prefix(
        &self,
        partition: &[u8],
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page> {
        self.store
            .scan_list(self.table, partition, prefix, cursor, limit)
            .await
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ScannableKvTableRef<'a, M> {
    store: &'a M,
    pub table: ScannableTableId,
}

impl<'a, M> ScannableKvTableRef<'a, M> {
    pub fn new(store: &'a M, table: ScannableTableId) -> Self {
        Self { store, table }
    }
}

impl<M: MetaStore> ScannableKvTableRef<'_, M> {
    pub async fn get(&self, partition: &[u8], clustering: &[u8]) -> Result<Option<Record>> {
        self.store.scan_get(self.table, partition, clustering).await
    }

    pub async fn put(
        &self,
        partition: &[u8],
        clustering: &[u8],
        value: Bytes,
    ) -> Result<PutResult> {
        self.store
            .scan_put(self.table, partition, clustering, value)
            .await
    }

    pub async fn delete(&self, partition: &[u8], clustering: &[u8], cond: DelCond) -> Result<()> {
        self.store
            .scan_delete(self.table, partition, clustering, cond)
            .await
    }

    pub async fn list_prefix(
        &self,
        partition: &[u8],
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page> {
        self.store
            .scan_list(self.table, partition, prefix, cursor, limit)
            .await
    }
}

#[allow(async_fn_in_trait)]
pub trait MetaStore: Clone + Send + Sync {
    fn table(&self, table: TableId) -> KvTable<Self>
    where
        Self: Sized,
    {
        KvTable::new(self.clone(), table)
    }

    fn table_ref(&self, table: TableId) -> KvTableRef<'_, Self>
    where
        Self: Sized,
    {
        KvTableRef::new(self, table)
    }

    fn scannable_table(&self, table: ScannableTableId) -> ScannableKvTable<Self>
    where
        Self: Sized,
    {
        ScannableKvTable::new(self.clone(), table)
    }

    fn scannable_table_ref(&self, table: ScannableTableId) -> ScannableKvTableRef<'_, Self>
    where
        Self: Sized,
    {
        ScannableKvTableRef::new(self, table)
    }

    async fn get(&self, table: TableId, key: &[u8]) -> Result<Option<Record>>;
    async fn put(&self, table: TableId, key: &[u8], value: Bytes) -> Result<PutResult>;
    async fn delete(&self, table: TableId, key: &[u8], cond: DelCond) -> Result<()>;
    async fn scan_get(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
    ) -> Result<Option<Record>>;
    async fn scan_put(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
        value: Bytes,
    ) -> Result<PutResult>;
    async fn scan_delete(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
        cond: DelCond,
    ) -> Result<()>;
    async fn scan_list(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page>;
}

impl<T: MetaStore> MetaStore for Arc<T> {
    async fn get(&self, table: TableId, key: &[u8]) -> Result<Option<Record>> {
        self.as_ref().get(table, key).await
    }

    async fn put(&self, table: TableId, key: &[u8], value: Bytes) -> Result<PutResult> {
        self.as_ref().put(table, key, value).await
    }

    async fn delete(&self, table: TableId, key: &[u8], cond: DelCond) -> Result<()> {
        self.as_ref().delete(table, key, cond).await
    }

    async fn scan_get(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
    ) -> Result<Option<Record>> {
        self.as_ref().scan_get(table, partition, clustering).await
    }

    async fn scan_put(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
        value: Bytes,
    ) -> Result<PutResult> {
        self.as_ref().scan_put(table, partition, clustering, value).await
    }

    async fn scan_delete(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
        cond: DelCond,
    ) -> Result<()> {
        self.as_ref()
            .scan_delete(table, partition, clustering, cond)
            .await
    }

    async fn scan_list(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page> {
        self.as_ref()
            .scan_list(table, partition, prefix, cursor, limit)
            .await
    }
}
