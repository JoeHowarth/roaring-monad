use bytes::Bytes;

use crate::error::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FenceToken(pub u64);

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
    pub next_cursor: Option<Vec<u8>>,
}

#[allow(async_fn_in_trait)]
pub trait MetaStore: Send + Sync {
    async fn get(&self, key: &[u8]) -> Result<Option<Record>>;
    async fn put(
        &self,
        key: &[u8],
        value: Bytes,
        cond: PutCond,
        fence: FenceToken,
    ) -> Result<PutResult>;
    async fn delete(&self, key: &[u8], cond: DelCond, fence: FenceToken) -> Result<()>;
    async fn list_prefix(
        &self,
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page>;
}

#[allow(async_fn_in_trait)]
pub trait BlobStore: Send + Sync {
    async fn put_blob(&self, key: &[u8], value: Bytes) -> Result<()>;
    async fn put_blob_if_absent(&self, key: &[u8], value: Bytes) -> Result<CreateOutcome>;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateOutcome {
    Created,
    AlreadyExists,
}
