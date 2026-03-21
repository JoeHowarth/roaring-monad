use std::sync::Arc;

use bytes::Bytes;

use crate::error::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FamilyId(&'static str);

impl FamilyId {
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
    store: Arc<M>,
    family: FamilyId,
}

impl<M> KvTable<M> {
    pub fn new(store: Arc<M>, family: FamilyId) -> Self {
        Self { store, family }
    }

    pub fn family(&self) -> FamilyId {
        self.family
    }
}

impl<M> Clone for KvTable<M> {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            family: self.family,
        }
    }
}

impl<M: MetaStore> KvTable<M> {
    pub async fn get(&self, key: &[u8]) -> Result<Option<Record>> {
        self.store.get(self.family, key).await
    }

    pub async fn put(&self, key: &[u8], value: Bytes, cond: PutCond) -> Result<PutResult> {
        self.store.put(self.family, key, value, cond).await
    }

    pub async fn delete(&self, key: &[u8], cond: DelCond) -> Result<()> {
        self.store.delete(self.family, key, cond).await
    }

    pub async fn list_prefix(
        &self,
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page> {
        self.store
            .list_prefix(self.family, prefix, cursor, limit)
            .await
    }
}

#[derive(Debug, Clone, Copy)]
pub struct KvTableRef<'a, M> {
    store: &'a M,
    family: FamilyId,
}

impl<'a, M> KvTableRef<'a, M> {
    pub fn new(store: &'a M, family: FamilyId) -> Self {
        Self { store, family }
    }

    pub fn family(&self) -> FamilyId {
        self.family
    }
}

impl<M: MetaStore> KvTableRef<'_, M> {
    pub async fn get(&self, key: &[u8]) -> Result<Option<Record>> {
        self.store.get(self.family, key).await
    }

    pub async fn put(&self, key: &[u8], value: Bytes, cond: PutCond) -> Result<PutResult> {
        self.store.put(self.family, key, value, cond).await
    }

    pub async fn delete(&self, key: &[u8], cond: DelCond) -> Result<()> {
        self.store.delete(self.family, key, cond).await
    }

    pub async fn list_prefix(
        &self,
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page> {
        self.store
            .list_prefix(self.family, prefix, cursor, limit)
            .await
    }
}

#[allow(async_fn_in_trait)]
pub trait MetaStore: Send + Sync {
    fn table(self: Arc<Self>, family: FamilyId) -> KvTable<Self>
    where
        Self: Sized,
    {
        KvTable::new(self, family)
    }

    fn table_ref(&self, family: FamilyId) -> KvTableRef<'_, Self>
    where
        Self: Sized,
    {
        KvTableRef::new(self, family)
    }

    async fn get(&self, family: FamilyId, key: &[u8]) -> Result<Option<Record>>;
    async fn put(
        &self,
        family: FamilyId,
        key: &[u8],
        value: Bytes,
        cond: PutCond,
    ) -> Result<PutResult>;
    async fn delete(&self, family: FamilyId, key: &[u8], cond: DelCond) -> Result<()>;
    async fn list_prefix(
        &self,
        family: FamilyId,
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page>;
}

impl<T: MetaStore> MetaStore for Arc<T> {
    async fn get(&self, family: FamilyId, key: &[u8]) -> Result<Option<Record>> {
        self.as_ref().get(family, key).await
    }

    async fn put(
        &self,
        family: FamilyId,
        key: &[u8],
        value: Bytes,
        cond: PutCond,
    ) -> Result<PutResult> {
        self.as_ref().put(family, key, value, cond).await
    }

    async fn delete(&self, family: FamilyId, key: &[u8], cond: DelCond) -> Result<()> {
        self.as_ref().delete(family, key, cond).await
    }

    async fn list_prefix(
        &self,
        family: FamilyId,
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page> {
        self.as_ref()
            .list_prefix(family, prefix, cursor, limit)
            .await
    }
}

#[allow(async_fn_in_trait)]
pub trait BlobStore: Send + Sync {
    async fn put_blob(&self, key: &[u8], value: Bytes) -> Result<()>;
    async fn get_blob(&self, key: &[u8]) -> Result<Option<Bytes>>;
    async fn read_range(
        &self,
        key: &[u8],
        start: u64,
        end_exclusive: u64,
    ) -> Result<Option<Bytes>> {
        let Some(blob) = self.get_blob(key).await? else {
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
    async fn delete_blob(&self, key: &[u8]) -> Result<()>;
    async fn list_prefix(
        &self,
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page>;
}

impl<T: BlobStore> BlobStore for Arc<T> {
    async fn put_blob(&self, key: &[u8], value: Bytes) -> Result<()> {
        self.as_ref().put_blob(key, value).await
    }

    async fn get_blob(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.as_ref().get_blob(key).await
    }

    async fn read_range(
        &self,
        key: &[u8],
        start: u64,
        end_exclusive: u64,
    ) -> Result<Option<Bytes>> {
        self.as_ref().read_range(key, start, end_exclusive).await
    }

    async fn delete_blob(&self, key: &[u8]) -> Result<()> {
        self.as_ref().delete_blob(key).await
    }

    async fn list_prefix(
        &self,
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page> {
        self.as_ref().list_prefix(prefix, cursor, limit).await
    }
}
