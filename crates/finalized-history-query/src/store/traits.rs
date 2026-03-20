use std::sync::Arc;

use bytes::Bytes;

use crate::error::Result;

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

#[allow(async_fn_in_trait)]
pub trait MetaStore: Send + Sync {
    async fn get(&self, key: &[u8]) -> Result<Option<Record>>;
    async fn put(&self, key: &[u8], value: Bytes, cond: PutCond) -> Result<PutResult>;
    async fn delete(&self, key: &[u8], cond: DelCond) -> Result<()>;
    async fn list_prefix(
        &self,
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page>;
}

impl<T: MetaStore> MetaStore for Arc<T> {
    async fn get(&self, key: &[u8]) -> Result<Option<Record>> {
        self.as_ref().get(key).await
    }

    async fn put(&self, key: &[u8], value: Bytes, cond: PutCond) -> Result<PutResult> {
        self.as_ref().put(key, value, cond).await
    }

    async fn delete(&self, key: &[u8], cond: DelCond) -> Result<()> {
        self.as_ref().delete(key, cond).await
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
