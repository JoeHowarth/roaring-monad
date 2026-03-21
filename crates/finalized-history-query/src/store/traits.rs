use std::sync::Arc;

use bytes::Bytes;

use crate::error::Result;

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

#[derive(Debug, Clone)]
pub struct Record {
    pub value: Bytes,
    pub version: u64,
}

#[derive(Debug, Clone, Copy)]
pub enum PutCond {
    Any,
    IfAbsent,
    IfVersion(u64),
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

#[derive(Debug, Clone)]
pub struct Page {
    pub keys: Vec<Vec<u8>>,
    // Opaque continuation token that resumes strictly after the returned page.
    pub next_cursor: Option<Vec<u8>>,
}

#[derive(Debug)]
pub struct KvTable<M> {
    store: M,
    table: TableId,
}

impl<M> KvTable<M> {
    pub fn new(store: M, table: TableId) -> Self {
        Self { store, table }
    }

    pub fn table(&self) -> TableId {
        self.table
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

    pub async fn put(&self, key: &[u8], value: Bytes, cond: PutCond) -> Result<PutResult> {
        self.store.put(self.table, key, value, cond).await
    }

    pub async fn delete(&self, key: &[u8], cond: DelCond) -> Result<()> {
        self.store.delete(self.table, key, cond).await
    }
}

#[derive(Debug, Clone, Copy)]
pub struct KvTableRef<'a, M> {
    store: &'a M,
    table: TableId,
}

impl<'a, M> KvTableRef<'a, M> {
    pub fn new(store: &'a M, table: TableId) -> Self {
        Self { store, table }
    }

    pub fn table(&self) -> TableId {
        self.table
    }
}

impl<M: MetaStore> KvTableRef<'_, M> {
    pub async fn get(&self, key: &[u8]) -> Result<Option<Record>> {
        self.store.get(self.table, key).await
    }

    pub async fn put(&self, key: &[u8], value: Bytes, cond: PutCond) -> Result<PutResult> {
        self.store.put(self.table, key, value, cond).await
    }

    pub async fn delete(&self, key: &[u8], cond: DelCond) -> Result<()> {
        self.store.delete(self.table, key, cond).await
    }
}

#[derive(Debug)]
pub struct ScannableKvTable<M> {
    store: M,
    table: ScannableTableId,
}

impl<M> ScannableKvTable<M> {
    pub fn new(store: M, table: ScannableTableId) -> Self {
        Self { store, table }
    }

    pub fn table(&self) -> ScannableTableId {
        self.table
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
        cond: PutCond,
    ) -> Result<PutResult> {
        self.store
            .scan_put(self.table, partition, clustering, value, cond)
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
    table: ScannableTableId,
}

#[derive(Debug)]
pub struct BlobTable<B> {
    store: B,
    table: BlobTableId,
}

impl<B> BlobTable<B> {
    pub fn new(store: B, table: BlobTableId) -> Self {
        Self { store, table }
    }

    pub fn table(&self) -> BlobTableId {
        self.table
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
    table: BlobTableId,
}

impl<'a, B> BlobTableRef<'a, B> {
    pub fn new(store: &'a B, table: BlobTableId) -> Self {
        Self { store, table }
    }

    pub fn table(&self) -> BlobTableId {
        self.table
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

impl<'a, M> ScannableKvTableRef<'a, M> {
    pub fn new(store: &'a M, table: ScannableTableId) -> Self {
        Self { store, table }
    }

    pub fn table(&self) -> ScannableTableId {
        self.table
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
        cond: PutCond,
    ) -> Result<PutResult> {
        self.store
            .scan_put(self.table, partition, clustering, value, cond)
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

/// Metadata store handle.
///
/// Implementations are expected to behave like cheap, cloneable handles to a
/// single logical store instance. Cloning a handle must not create a distinct
/// backing store or fork state; all clones must continue to target the same
/// logical store.
///
/// Rust cannot enforce "cheap clone" mechanically, so this is a semantic
/// contract for implementors of the trait.
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
    async fn put(
        &self,
        table: TableId,
        key: &[u8],
        value: Bytes,
        cond: PutCond,
    ) -> Result<PutResult>;
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
        cond: PutCond,
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

    async fn put(
        &self,
        table: TableId,
        key: &[u8],
        value: Bytes,
        cond: PutCond,
    ) -> Result<PutResult> {
        self.as_ref().put(table, key, value, cond).await
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
        cond: PutCond,
    ) -> Result<PutResult> {
        self.as_ref()
            .scan_put(table, partition, clustering, value, cond)
            .await
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

/// Blob store handle.
///
/// Implementations are expected to behave like cheap, cloneable handles to a
/// single logical store instance. Cloning a handle must not create a distinct
/// backing store or fork state; all clones must continue to target the same
/// logical store.
///
/// Rust cannot enforce "cheap clone" mechanically, so this is a semantic
/// contract for implementors of the trait.
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
            .map_err(|_| crate::error::Error::Decode("blob range start overflow"))?;
        let end = usize::try_from(end_exclusive)
            .map_err(|_| crate::error::Error::Decode("blob range end overflow"))?;
        if start > end || end > blob.len() {
            return Err(crate::error::Error::Decode("invalid blob range"));
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
