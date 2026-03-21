use bytes::Bytes;
use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use crate::error::{Error, Result};
use crate::store::traits::{
    BlobStore, DelCond, FamilyId, MetaStore, Page, PutCond, PutResult, Record,
};

#[derive(Debug, Clone)]
pub struct FsMetaStore {
    root: PathBuf,
}

impl FsMetaStore {
    pub fn new(root: impl AsRef<Path>, min_epoch: u64) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(root.join("meta"))
            .map_err(|e| Error::Backend(format!("create fs meta dir: {e}")))?;
        let _ = min_epoch;
        Ok(Self { root })
    }

    fn family_dir(&self, family: FamilyId) -> PathBuf {
        let mut p = self.root.join("meta");
        p.push(family.as_str());
        p
    }

    fn key_path(&self, family: FamilyId, key: &[u8]) -> PathBuf {
        let mut p = self.family_dir(family);
        p.push(hex(key));
        p
    }

    fn version_path(&self, family: FamilyId, key: &[u8]) -> PathBuf {
        let mut p = self.family_dir(family);
        p.push(format!("{}.ver", hex(key)));
        p
    }

    fn key_lock(&self, family: FamilyId, key: &[u8]) -> Result<Arc<Mutex<()>>> {
        static LOCKS: OnceLock<Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>> = OnceLock::new();
        let locks = LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
        let mut guard = locks
            .lock()
            .map_err(|_| Error::Backend("poisoned fs key lock map".to_string()))?;
        Ok(guard
            .entry(self.key_path(family, key))
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone())
    }
}

impl MetaStore for FsMetaStore {
    async fn get(&self, family: FamilyId, key: &[u8]) -> Result<Option<Record>> {
        let kp = self.key_path(family, key);
        if !kp.exists() {
            return Ok(None);
        }
        let vp = self.version_path(family, key);
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
        family: FamilyId,
        key: &[u8],
        value: Bytes,
        cond: PutCond,
    ) -> Result<PutResult> {
        let lock = if matches!(cond, PutCond::Any) {
            None
        } else {
            Some(self.key_lock(family, key)?)
        };
        let _guard = if let Some(lock) = lock.as_ref() {
            Some(
                lock.lock()
                    .map_err(|_| Error::Backend("poisoned fs key lock".to_string()))?,
            )
        } else {
            None
        };
        let current = self.get(family, key).await?;

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

        let kp = self.key_path(family, key);
        let vp = self.version_path(family, key);
        if let Some(parent) = kp.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| Error::Backend(format!("create fs meta family dir: {e}")))?;
        }
        write_file_bytes(&kp, &value)?;

        let next_version = current.map_or(1, |c| c.version + 1);
        write_file_bytes(&vp, &next_version.to_be_bytes())?;

        Ok(PutResult {
            applied: true,
            version: Some(next_version),
        })
    }

    async fn delete(&self, family: FamilyId, key: &[u8], cond: DelCond) -> Result<()> {
        let lock = if matches!(cond, DelCond::Any) {
            None
        } else {
            Some(self.key_lock(family, key)?)
        };
        let _guard = if let Some(lock) = lock.as_ref() {
            Some(
                lock.lock()
                    .map_err(|_| Error::Backend("poisoned fs key lock".to_string()))?,
            )
        } else {
            None
        };
        let current = self.get(family, key).await?;
        let allowed = match (cond, current.as_ref()) {
            (DelCond::Any, Some(_)) => true,
            (DelCond::Any, None) => false,
            (DelCond::IfVersion(v), Some(r)) => r.version == v,
            (DelCond::IfVersion(_), None) => false,
        };
        if allowed {
            let _ = fs::remove_file(self.key_path(family, key));
            let _ = fs::remove_file(self.version_path(family, key));
        }
        Ok(())
    }

    async fn list_prefix(
        &self,
        family: FamilyId,
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page> {
        let mut all = Vec::<Vec<u8>>::new();
        let family_dir = self.family_dir(family);
        if !family_dir.exists() {
            return Ok(Page {
                keys: Vec::new(),
                next_cursor: None,
            });
        }
        collect_keys_from_family_dir(&family_dir, true, prefix, &mut all)?;
        all.sort();

        let has_cursor = cursor.is_some();
        let start = cursor.unwrap_or_default();
        let mut keys = Vec::new();
        let mut next_cursor = None;
        for k in all {
            if has_cursor && k <= start {
                continue;
            }
            if !has_cursor && k < start {
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

        let has_cursor = cursor.is_some();
        let start = cursor.unwrap_or_default();
        let mut keys = Vec::new();
        let mut next_cursor = None;
        for k in all {
            if has_cursor && k <= start {
                continue;
            }
            if !has_cursor && k < start {
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

fn collect_keys_from_family_dir(
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
    use crate::domain::keys::BLOCK_RECORD_FAMILY;
    use crate::domain::types::PublicationState;
    use crate::store::publication::{CasOutcome, MetaPublicationStore, PublicationStore};
    use crate::store::traits::{BlobStore, MetaStore, PutCond};
    use bytes::Bytes;
    use futures::executor::block_on;
    use std::sync::Arc;

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
        let publication_store = Arc::new(MetaPublicationStore::new(Arc::clone(&store)));
        let state = PublicationState {
            owner_id: 1,
            session_id: [1u8; 16],
            indexed_finalized_head: 0,
            lease_valid_through_block: u64::MAX,
        };

        let handles = (0..2)
            .map(|_| {
                let store = Arc::clone(&publication_store);
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
        let publication_store = Arc::new(MetaPublicationStore::new(Arc::clone(&store)));
        let initial = PublicationState {
            owner_id: 1,
            session_id: [1u8; 16],
            indexed_finalized_head: 0,
            lease_valid_through_block: u64::MAX,
        };
        block_on(publication_store.create_if_absent(&initial)).expect("seed publication state");

        let next_a = PublicationState {
            owner_id: 1,
            session_id: [1u8; 16],
            indexed_finalized_head: 1,
            lease_valid_through_block: u64::MAX,
        };
        let next_b = PublicationState {
            owner_id: 2,
            session_id: [2u8; 16],
            indexed_finalized_head: 0,
            lease_valid_through_block: u64::MAX,
        };

        let store_a = Arc::clone(&publication_store);
        let store_b = publication_store;
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

    #[test]
    fn list_prefix_pagination_does_not_repeat_cursor_entry() {
        let root = unique_temp_root("fs-list-prefix");
        let meta_store = FsMetaStore::new(&root, 0).expect("fs meta store");
        let blob_store = FsBlobStore::new(&root).expect("fs blob store");

        block_on(async {
            for index in 0..1_025u64 {
                let key = format!("{index:04}").into_bytes();
                meta_store
                    .put(
                        BLOCK_RECORD_FAMILY,
                        &key,
                        Bytes::from_static(b"v"),
                        PutCond::Any,
                    )
                    .await
                    .expect("seed meta key");
                let blob_key = format!("list-prefix/{index:04}").into_bytes();
                blob_store
                    .put_blob(&blob_key, Bytes::from_static(b"v"))
                    .await
                    .expect("seed blob key");
            }

            let mut meta_cursor = None;
            let mut meta_seen = Vec::new();
            loop {
                let page = meta_store
                    .list_prefix(BLOCK_RECORD_FAMILY, b"", meta_cursor.take(), 1_024)
                    .await
                    .expect("list meta");
                meta_seen.extend(page.keys.iter().cloned());
                if page.next_cursor.is_none() {
                    break;
                }
                meta_cursor = page.next_cursor;
            }

            let mut blob_cursor = None;
            let mut blob_seen = Vec::new();
            loop {
                let page = blob_store
                    .list_prefix(b"list-prefix/", blob_cursor.take(), 1_024)
                    .await
                    .expect("list blob prefix");
                blob_seen.extend(page.keys.iter().cloned());
                if page.next_cursor.is_none() {
                    break;
                }
                blob_cursor = page.next_cursor;
            }

            let meta_unique = meta_seen.iter().collect::<std::collections::BTreeSet<_>>();
            let blob_unique = blob_seen.iter().collect::<std::collections::BTreeSet<_>>();
            assert_eq!(meta_seen.len(), 1_025);
            assert_eq!(meta_unique.len(), 1_025);
            assert_eq!(blob_seen.len(), 1_025);
            assert_eq!(blob_unique.len(), 1_025);
        });

        let _ = fs::remove_dir_all(root);
    }
}
