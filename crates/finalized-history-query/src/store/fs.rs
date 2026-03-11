use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use bytes::Bytes;

use crate::codec::finalized_state::{decode_publication_state, encode_publication_state};
use crate::domain::types::PublicationState;
use crate::error::{Error, Result};
use crate::store::publication::{CasOutcome, PublicationStore};
use crate::store::traits::{
    BlobStore, CreateOutcome, DelCond, FenceToken, MetaStore, Page, PutCond, PutResult, Record,
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
        p.push(extract_group(key));
        p.push(hex(key));
        p
    }

    fn version_path(&self, key: &[u8]) -> PathBuf {
        let mut p = self.root.join("meta");
        p.push(extract_group(key));
        p.push(format!("{}.ver", hex(key)));
        p
    }

    fn publication_state_path(&self) -> PathBuf {
        self.root.join("meta").join("publication_state")
    }

    fn publication_state_version_path(&self) -> PathBuf {
        self.root.join("meta").join("publication_state.ver")
    }

    fn validate_fence(&self, fence: FenceToken) -> Result<()> {
        if fence.0 < self.min_epoch {
            return Err(Error::LeaseLost);
        }
        Ok(())
    }

    fn publication_state_lock(&self) -> Result<Arc<Mutex<()>>> {
        static LOCKS: OnceLock<Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>> = OnceLock::new();
        let locks = LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
        let mut guard = locks
            .lock()
            .map_err(|_| Error::Backend("poisoned publication lock map".to_string()))?;
        Ok(guard
            .entry(self.publication_state_path())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone())
    }
}

impl PublicationStore for FsMetaStore {
    async fn load(&self) -> Result<Option<PublicationState>> {
        load_publication_state_from_path(&self.publication_state_path())
    }

    async fn create_if_absent(
        &self,
        initial: &PublicationState,
    ) -> Result<CasOutcome<PublicationState>> {
        let lock = self.publication_state_lock()?;
        let _guard = lock
            .lock()
            .map_err(|_| Error::Backend("poisoned publication state lock".to_string()))?;
        let path = self.publication_state_path();
        let version_path = self.publication_state_version_path();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| Error::Backend(format!("create fs publication dir: {e}")))?;
        }
        match write_file_bytes_create_new(&path, &encode_publication_state(initial)) {
            Ok(()) => {
                write_file_bytes_atomic(&version_path, &1u64.to_be_bytes())?;
                Ok(CasOutcome::Applied(initial.clone()))
            }
            Err(Error::Backend(message)) if message.contains("exists") => Ok(CasOutcome::Failed {
                current: load_publication_state_from_path(&path)?,
            }),
            Err(error) => Err(error),
        }
    }

    async fn compare_and_set(
        &self,
        expected: &PublicationState,
        next: &PublicationState,
    ) -> Result<CasOutcome<PublicationState>> {
        let lock = self.publication_state_lock()?;
        let _guard = lock
            .lock()
            .map_err(|_| Error::Backend("poisoned publication state lock".to_string()))?;
        let path = self.publication_state_path();
        if !path.exists() {
            return Ok(CasOutcome::Failed { current: None });
        }
        let current = decode_publication_state(&read_file_bytes(&path)?)?;
        if current != *expected {
            return Ok(CasOutcome::Failed {
                current: Some(current),
            });
        }

        write_file_bytes_atomic(&path, &encode_publication_state(next))?;
        let version_path = self.publication_state_version_path();
        let next_version = if version_path.exists() {
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&read_file_bytes(&version_path)?);
            u64::from_be_bytes(bytes).saturating_add(1)
        } else {
            1
        };
        write_file_bytes_atomic(&version_path, &next_version.to_be_bytes())?;
        Ok(CasOutcome::Applied(next.clone()))
    }
}

fn load_publication_state_from_path(path: &Path) -> Result<Option<PublicationState>> {
    if !path.exists() {
        return Ok(None);
    }
    Ok(Some(decode_publication_state(&read_file_bytes(path)?)?))
}

impl MetaStore for FsMetaStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Record>> {
        let kp = self.key_path(key);
        if !kp.exists() {
            return Ok(None);
        }
        let vp = self.version_path(key);
        let value = read_file_bytes(&kp)?;
        let version = if vp.exists() {
            let b = read_file_bytes(&vp)?;
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
        if let Some(parent) = kp.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| Error::Backend(format!("create fs meta group dir: {e}")))?;
        }
        write_file_bytes(&kp, &value)?;

        let next_version = current.map_or(1, |c| c.version + 1);
        write_file_bytes(&vp, &next_version.to_be_bytes())?;

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
        let base = self.root.join("meta");
        if !base.exists() {
            return Ok(Page {
                keys: Vec::new(),
                next_cursor: None,
            });
        }
        if let Some(group) = extract_group_from_prefix(prefix) {
            collect_keys_from_group_dir(&base.join(group), true, prefix, &mut all)?;
        } else {
            for entry in
                fs::read_dir(&base).map_err(|e| Error::Backend(format!("fs read_dir: {e}")))?
            {
                let entry = entry.map_err(|e| Error::Backend(format!("fs dir entry: {e}")))?;
                if !entry.path().is_dir() {
                    continue;
                }
                collect_keys_from_group_dir(&entry.path(), true, prefix, &mut all)?;
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
        p.push(extract_group(key));
        p.push(hex(key));
        p
    }
}

impl BlobStore for FsBlobStore {
    async fn put_blob(&self, key: &[u8], value: Bytes) -> Result<()> {
        let path = self.key_path(key);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| Error::Backend(format!("create fs blob group dir: {e}")))?;
        }
        write_file_bytes(&path, &value)
    }

    async fn put_blob_if_absent(&self, key: &[u8], value: Bytes) -> Result<CreateOutcome> {
        let path = self.key_path(key);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| Error::Backend(format!("create fs blob group dir: {e}")))?;
        }
        match write_file_bytes_create_new(&path, &value) {
            Ok(()) => Ok(CreateOutcome::Created),
            Err(Error::Backend(message)) if message.contains("exists") => {
                Ok(CreateOutcome::AlreadyExists)
            }
            Err(error) => Err(error),
        }
    }

    async fn get_blob(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let p = self.key_path(key);
        if !p.exists() {
            return Ok(None);
        }
        let b = read_file_bytes(&p)?;
        Ok(Some(Bytes::from(b)))
    }

    async fn delete_blob(&self, key: &[u8]) -> Result<()> {
        let _ = fs::remove_file(self.key_path(key));
        Ok(())
    }

    async fn list_prefix(
        &self,
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page> {
        let mut all = Vec::<Vec<u8>>::new();
        let base = self.root.join("blob");
        if !base.exists() {
            return Ok(Page {
                keys: Vec::new(),
                next_cursor: None,
            });
        }
        if let Some(group) = extract_group_from_prefix(prefix) {
            collect_keys_from_group_dir(&base.join(group), false, prefix, &mut all)?;
        } else {
            for entry in
                fs::read_dir(&base).map_err(|e| Error::Backend(format!("fs blob read_dir: {e}")))?
            {
                let entry = entry.map_err(|e| Error::Backend(format!("fs blob dir entry: {e}")))?;
                if !entry.path().is_dir() {
                    continue;
                }
                collect_keys_from_group_dir(&entry.path(), false, prefix, &mut all)?;
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

fn read_file_bytes(path: &Path) -> Result<Vec<u8>> {
    let mut file = OpenOptions::new()
        .read(true)
        .open(path)
        .map_err(|e| Error::Backend(format!("fs read open: {e}")))?;
    set_no_cache(&file)?;
    let mut out = Vec::new();
    file.read_to_end(&mut out)
        .map_err(|e| Error::Backend(format!("fs read: {e}")))?;
    Ok(out)
}

fn write_file_bytes(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)
        .map_err(|e| Error::Backend(format!("fs write open: {e}")))?;
    set_no_cache(&file)?;
    file.write_all(bytes)
        .map_err(|e| Error::Backend(format!("fs write: {e}")))?;
    Ok(())
}

fn write_file_bytes_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let Some(parent) = path.parent() else {
        return Err(Error::Backend("path missing parent".to_string()));
    };
    fs::create_dir_all(parent).map_err(|e| Error::Backend(format!("fs atomic parent: {e}")))?;
    let temp_name = format!(
        ".tmp-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| Error::Backend(format!("fs atomic time: {e}")))?
            .as_nanos()
    );
    let temp_path = parent.join(temp_name);
    write_file_bytes_create_new(&temp_path, bytes)?;
    fs::rename(&temp_path, path).map_err(|e| Error::Backend(format!("fs atomic rename: {e}")))?;
    Ok(())
}

fn write_file_bytes_create_new(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|e| Error::Backend(format!("fs create_new open: {e}")))?;
    set_no_cache(&file)?;
    file.write_all(bytes)
        .map_err(|e| Error::Backend(format!("fs create_new write: {e}")))?;
    Ok(())
}

#[cfg(target_os = "macos")]
fn set_no_cache(file: &File) -> Result<()> {
    use std::os::fd::AsRawFd;

    let rc = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) };
    if rc == -1 {
        return Err(Error::Backend(format!(
            "macos fcntl(F_NOCACHE) failed: {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok(())
}

#[cfg(not(target_os = "macos"))]
fn set_no_cache(_file: &File) -> Result<()> {
    Ok(())
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

fn extract_group(key: &[u8]) -> String {
    let mut out = String::new();
    for &b in key {
        if b == b'/' {
            break;
        }
        if b.is_ascii_alphanumeric() || b == b'_' || b == b'-' {
            out.push(char::from(b));
        } else {
            break;
        }
    }
    if out.is_empty() {
        "misc".to_string()
    } else {
        out
    }
}

fn extract_group_from_prefix(prefix: &[u8]) -> Option<String> {
    if prefix.is_empty() {
        return None;
    }
    Some(extract_group(prefix))
}

fn collect_keys_from_group_dir(
    dir: &Path,
    skip_ver: bool,
    prefix: &[u8],
    out: &mut Vec<Vec<u8>>,
) -> Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(dir).map_err(|e| Error::Backend(format!("fs read_dir: {e}")))? {
        let entry = entry.map_err(|e| Error::Backend(format!("fs dir entry: {e}")))?;
        if !entry.path().is_file() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if skip_ver && name.ends_with(".ver") {
            continue;
        }
        let key = unhex(&name)?;
        if key.starts_with(prefix) {
            out.push(key);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;

    fn unique_temp_root(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "finalized-history-query-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        ))
    }

    #[test]
    fn create_if_absent_allows_only_one_creator() {
        let root = unique_temp_root("fs-create");
        let store = Arc::new(FsMetaStore::new(&root, 0).expect("fs store"));
        let state = PublicationState {
            owner_id: 1,
            epoch: 1,
            indexed_finalized_head: 0,
        };

        let handles = (0..2)
            .map(|_| {
                let store = Arc::clone(&store);
                let state = state.clone();
                std::thread::spawn(move || block_on(store.create_if_absent(&state)))
            })
            .collect::<Vec<_>>();

        let mut applied = 0usize;
        let mut failed = 0usize;
        for handle in handles {
            match handle.join().expect("join").expect("cas result") {
                CasOutcome::Applied(_) => applied += 1,
                CasOutcome::Failed { .. } => failed += 1,
            }
        }

        assert_eq!(applied, 1);
        assert_eq!(failed, 1);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn compare_and_set_is_serialized() {
        let root = unique_temp_root("fs-cas");
        let store = Arc::new(FsMetaStore::new(&root, 0).expect("fs store"));
        let initial = PublicationState {
            owner_id: 1,
            epoch: 1,
            indexed_finalized_head: 0,
        };
        block_on(store.create_if_absent(&initial)).expect("seed publication state");

        let next_a = PublicationState {
            owner_id: 1,
            epoch: 1,
            indexed_finalized_head: 1,
        };
        let next_b = PublicationState {
            owner_id: 2,
            epoch: 2,
            indexed_finalized_head: 0,
        };

        let store_a = Arc::clone(&store);
        let store_b = store;
        let initial_a = initial.clone();
        let initial_b = initial;
        let handles = vec![
            std::thread::spawn(move || block_on(store_a.compare_and_set(&initial_a, &next_a))),
            std::thread::spawn(move || block_on(store_b.compare_and_set(&initial_b, &next_b))),
        ];

        let mut applied = 0usize;
        let mut failed = 0usize;
        for handle in handles {
            match handle.join().expect("join").expect("cas result") {
                CasOutcome::Applied(_) => applied += 1,
                CasOutcome::Failed { .. } => failed += 1,
            }
        }

        assert_eq!(applied, 1);
        assert_eq!(failed, 1);
        let _ = fs::remove_dir_all(root);
    }
}
