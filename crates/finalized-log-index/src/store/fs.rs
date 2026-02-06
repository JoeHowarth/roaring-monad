use std::fs;
use std::path::{Path, PathBuf};

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::store::traits::{
    BlobStore, DelCond, FenceToken, MetaStore, Page, PutCond, PutResult, Record,
};

#[derive(Debug, Clone)]
pub struct FsMetaStore {
    root: PathBuf,
    min_epoch: u64,
}

impl FsMetaStore {
    pub fn new(root: impl AsRef<Path>, min_epoch: u64) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(root.join("meta"))
            .map_err(|e| Error::Backend(format!("create fs meta dir: {e}")))?;
        Ok(Self { root, min_epoch })
    }

    fn key_path(&self, key: &[u8]) -> PathBuf {
        let mut p = self.root.join("meta");
        p.push(hex(key));
        p
    }

    fn version_path(&self, key: &[u8]) -> PathBuf {
        let mut p = self.root.join("meta");
        p.push(format!("{}.ver", hex(key)));
        p
    }

    fn validate_fence(&self, fence: FenceToken) -> Result<()> {
        if fence.0 < self.min_epoch {
            return Err(Error::LeaseLost);
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl MetaStore for FsMetaStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Record>> {
        let kp = self.key_path(key);
        if !kp.exists() {
            return Ok(None);
        }
        let vp = self.version_path(key);
        let value = fs::read(&kp).map_err(|e| Error::Backend(format!("fs meta read: {e}")))?;
        let version = if vp.exists() {
            let b = fs::read(&vp).map_err(|e| Error::Backend(format!("fs ver read: {e}")))?;
            if b.len() != 8 {
                return Err(Error::Decode("invalid fs version bytes"));
            }
            let mut a = [0u8; 8];
            a.copy_from_slice(&b);
            u64::from_be_bytes(a)
        } else {
            0
        };

        Ok(Some(Record {
            value: Bytes::from(value),
            version,
        }))
    }

    async fn put(
        &self,
        key: &[u8],
        value: Bytes,
        cond: PutCond,
        fence: FenceToken,
    ) -> Result<PutResult> {
        self.validate_fence(fence)?;
        let current = self.get(key).await?;

        let allowed = match (cond, current.as_ref()) {
            (PutCond::Any, _) => true,
            (PutCond::IfAbsent, None) => true,
            (PutCond::IfAbsent, Some(_)) => false,
            (PutCond::IfVersion(v), Some(r)) => r.version == v,
            (PutCond::IfVersion(_), None) => false,
        };
        if !allowed {
            return Ok(PutResult {
                applied: false,
                version: current.map(|c| c.version),
            });
        }

        let kp = self.key_path(key);
        let vp = self.version_path(key);
        fs::write(&kp, &value).map_err(|e| Error::Backend(format!("fs meta write: {e}")))?;

        let next_version = current.map_or(1, |c| c.version + 1);
        fs::write(&vp, next_version.to_be_bytes())
            .map_err(|e| Error::Backend(format!("fs ver write: {e}")))?;

        Ok(PutResult {
            applied: true,
            version: Some(next_version),
        })
    }

    async fn delete(&self, key: &[u8], cond: DelCond, fence: FenceToken) -> Result<()> {
        self.validate_fence(fence)?;
        let current = self.get(key).await?;
        let allowed = match (cond, current.as_ref()) {
            (DelCond::Any, Some(_)) => true,
            (DelCond::Any, None) => false,
            (DelCond::IfVersion(v), Some(r)) => r.version == v,
            (DelCond::IfVersion(_), None) => false,
        };
        if allowed {
            let _ = fs::remove_file(self.key_path(key));
            let _ = fs::remove_file(self.version_path(key));
        }
        Ok(())
    }

    async fn list_prefix(
        &self,
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page> {
        let mut all = Vec::<Vec<u8>>::new();
        let dir = self.root.join("meta");
        for entry in fs::read_dir(dir).map_err(|e| Error::Backend(format!("fs read_dir: {e}")))? {
            let entry = entry.map_err(|e| Error::Backend(format!("fs dir entry: {e}")))?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.ends_with(".ver") {
                continue;
            }
            let key = unhex(&name)?;
            if key.starts_with(prefix) {
                all.push(key);
            }
        }
        all.sort();

        let start = cursor.unwrap_or_default();
        let mut keys = Vec::new();
        let mut next_cursor = None;
        for k in all {
            if k < start {
                continue;
            }
            keys.push(k);
            if keys.len() == limit {
                next_cursor = keys.last().cloned();
                break;
            }
        }

        Ok(Page { keys, next_cursor })
    }
}

#[derive(Debug, Clone)]
pub struct FsBlobStore {
    root: PathBuf,
}

impl FsBlobStore {
    pub fn new(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(root.join("blob"))
            .map_err(|e| Error::Backend(format!("create fs blob dir: {e}")))?;
        Ok(Self { root })
    }

    fn key_path(&self, key: &[u8]) -> PathBuf {
        let mut p = self.root.join("blob");
        p.push(hex(key));
        p
    }
}

#[async_trait::async_trait]
impl BlobStore for FsBlobStore {
    async fn put_blob(&self, key: &[u8], value: Bytes) -> Result<()> {
        fs::write(self.key_path(key), &value)
            .map_err(|e| Error::Backend(format!("fs blob write: {e}")))?;
        Ok(())
    }

    async fn get_blob(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let p = self.key_path(key);
        if !p.exists() {
            return Ok(None);
        }
        let b = fs::read(p).map_err(|e| Error::Backend(format!("fs blob read: {e}")))?;
        Ok(Some(Bytes::from(b)))
    }

    async fn delete_blob(&self, key: &[u8]) -> Result<()> {
        let _ = fs::remove_file(self.key_path(key));
        Ok(())
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(nibble((b >> 4) & 0xf));
        out.push(nibble(b & 0xf));
    }
    out
}

fn unhex(s: &str) -> Result<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return Err(Error::Decode("invalid hex length"));
    }
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 2);
    let mut i = 0usize;
    while i < bytes.len() {
        let h = from_nibble(bytes[i])?;
        let l = from_nibble(bytes[i + 1])?;
        out.push((h << 4) | l);
        i += 2;
    }
    Ok(out)
}

fn nibble(v: u8) -> char {
    match v {
        0..=9 => (b'0' + v) as char,
        10..=15 => (b'a' + (v - 10)) as char,
        _ => '0',
    }
}

fn from_nibble(b: u8) -> Result<u8> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(10 + b - b'a'),
        b'A'..=b'F' => Ok(10 + b - b'A'),
        _ => Err(Error::Decode("invalid hex char")),
    }
}
