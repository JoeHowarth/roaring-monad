use bytes::Bytes;
use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use crate::error::{Error, Result};
use crate::store::traits::{
    BlobStore, BlobTableId, DelCond, MetaStore, Page, PutCond, PutResult, Record, ScannableTableId,
    TableId,
};

/// Cheap clone handle to the same filesystem-backed metadata namespace.
#[derive(Debug, Clone)]
pub struct FsMetaStore {
    root: PathBuf,
}

impl FsMetaStore {
    pub fn new(root: impl AsRef<Path>, min_epoch: u64) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(root.join("meta"))
            .map_err(|e| Error::Backend(format!("create fs meta dir: {e}")))?;
        fs::create_dir_all(root.join("meta_scan"))
            .map_err(|e| Error::Backend(format!("create fs scannable meta dir: {e}")))?;
        let _ = min_epoch;
        Ok(Self { root })
    }

    fn table_dir(&self, table: TableId) -> PathBuf {
        let mut p = self.root.join("meta");
        p.push(table.as_str());
        p
    }

    fn scan_table_dir(&self, table: ScannableTableId) -> PathBuf {
        let mut p = self.root.join("meta_scan");
        p.push(table.as_str());
        p
    }

    fn key_path(&self, table: TableId, key: &[u8]) -> PathBuf {
        let mut p = self.table_dir(table);
        p.push(hex(key));
        p
    }

    fn version_path(&self, table: TableId, key: &[u8]) -> PathBuf {
        let mut p = self.table_dir(table);
        p.push(format!("{}.ver", hex(key)));
        p
    }

    fn scan_partition_dir(&self, table: ScannableTableId, partition: &[u8]) -> PathBuf {
        let mut p = self.scan_table_dir(table);
        p.push(hex(partition));
        p
    }

    fn scan_key_path(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
    ) -> PathBuf {
        let mut p = self.scan_partition_dir(table, partition);
        p.push(hex(clustering));
        p
    }

    fn scan_version_path(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
    ) -> PathBuf {
        let mut p = self.scan_partition_dir(table, partition);
        p.push(format!("{}.ver", hex(clustering)));
        p
    }

    fn key_lock(&self, table: TableId, key: &[u8]) -> Result<Arc<Mutex<()>>> {
        self.path_lock(self.key_path(table, key))
    }

    fn scan_key_lock(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
    ) -> Result<Arc<Mutex<()>>> {
        self.path_lock(self.scan_key_path(table, partition, clustering))
    }

    fn path_lock(&self, path: PathBuf) -> Result<Arc<Mutex<()>>> {
        static LOCKS: OnceLock<Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>> = OnceLock::new();
        let locks = LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
        let mut guard = locks
            .lock()
            .map_err(|_| Error::Backend("poisoned fs key lock map".to_string()))?;
        Ok(guard
            .entry(path)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone())
    }
}

impl MetaStore for FsMetaStore {
    async fn get(&self, table: TableId, key: &[u8]) -> Result<Option<Record>> {
        let kp = self.key_path(table, key);
        if !kp.exists() {
            return Ok(None);
        }
        let vp = self.version_path(table, key);
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
        table: TableId,
        key: &[u8],
        value: Bytes,
        cond: PutCond,
    ) -> Result<PutResult> {
        let lock = if matches!(cond, PutCond::Any) {
            None
        } else {
            Some(self.key_lock(table, key)?)
        };
        let _guard = if let Some(lock) = lock.as_ref() {
            Some(
                lock.lock()
                    .map_err(|_| Error::Backend("poisoned fs key lock".to_string()))?,
            )
        } else {
            None
        };
        let current = self.get(table, key).await?;

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

        let kp = self.key_path(table, key);
        let vp = self.version_path(table, key);
        if let Some(parent) = kp.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| Error::Backend(format!("create fs meta table dir: {e}")))?;
        }
        write_file_bytes(&kp, &value)?;

        let next_version = current.map_or(1, |c| c.version + 1);
        write_file_bytes(&vp, &next_version.to_be_bytes())?;

        Ok(PutResult {
            applied: true,
            version: Some(next_version),
        })
    }

    async fn delete(&self, table: TableId, key: &[u8], cond: DelCond) -> Result<()> {
        let lock = if matches!(cond, DelCond::Any) {
            None
        } else {
            Some(self.key_lock(table, key)?)
        };
        let _guard = if let Some(lock) = lock.as_ref() {
            Some(
                lock.lock()
                    .map_err(|_| Error::Backend("poisoned fs key lock".to_string()))?,
            )
        } else {
            None
        };
        let current = self.get(table, key).await?;
        let allowed = match (cond, current.as_ref()) {
            (DelCond::Any, Some(_)) => true,
            (DelCond::Any, None) => false,
            (DelCond::IfVersion(v), Some(r)) => r.version == v,
            (DelCond::IfVersion(_), None) => false,
        };
        if allowed {
            let _ = fs::remove_file(self.key_path(table, key));
            let _ = fs::remove_file(self.version_path(table, key));
        }
        Ok(())
    }

    async fn scan_get(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
    ) -> Result<Option<Record>> {
        let kp = self.scan_key_path(table, partition, clustering);
        if !kp.exists() {
            return Ok(None);
        }
        let vp = self.scan_version_path(table, partition, clustering);
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

    async fn scan_put(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
        value: Bytes,
        cond: PutCond,
    ) -> Result<PutResult> {
        let lock = if matches!(cond, PutCond::Any) {
            None
        } else {
            Some(self.scan_key_lock(table, partition, clustering)?)
        };
        let _guard = if let Some(lock) = lock.as_ref() {
            Some(
                lock.lock()
                    .map_err(|_| Error::Backend("poisoned fs key lock".to_string()))?,
            )
        } else {
            None
        };
        let current = self.scan_get(table, partition, clustering).await?;

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

        let kp = self.scan_key_path(table, partition, clustering);
        let vp = self.scan_version_path(table, partition, clustering);
        if let Some(parent) = kp.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                Error::Backend(format!("create fs scannable meta partition dir: {e}"))
            })?;
        }
        write_file_bytes(&kp, &value)?;

        let next_version = current.map_or(1, |c| c.version + 1);
        write_file_bytes(&vp, &next_version.to_be_bytes())?;

        Ok(PutResult {
            applied: true,
            version: Some(next_version),
        })
    }

    async fn scan_delete(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
        cond: DelCond,
    ) -> Result<()> {
        let lock = if matches!(cond, DelCond::Any) {
            None
        } else {
            Some(self.scan_key_lock(table, partition, clustering)?)
        };
        let _guard = if let Some(lock) = lock.as_ref() {
            Some(
                lock.lock()
                    .map_err(|_| Error::Backend("poisoned fs key lock".to_string()))?,
            )
        } else {
            None
        };
        let current = self.scan_get(table, partition, clustering).await?;
        let allowed = match (cond, current.as_ref()) {
            (DelCond::Any, Some(_)) => true,
            (DelCond::Any, None) => false,
            (DelCond::IfVersion(v), Some(r)) => r.version == v,
            (DelCond::IfVersion(_), None) => false,
        };
        if allowed {
            let _ = fs::remove_file(self.scan_key_path(table, partition, clustering));
            let _ = fs::remove_file(self.scan_version_path(table, partition, clustering));
        }
        Ok(())
    }

    async fn scan_list(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page> {
        let mut all = Vec::<Vec<u8>>::new();
        let partition_dir = self.scan_partition_dir(table, partition);
        if !partition_dir.exists() {
            return Ok(Page {
                keys: Vec::new(),
                next_cursor: None,
            });
        }
        collect_keys_from_partition_dir(&partition_dir, prefix, &mut all)?;
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

/// Cheap clone handle to the same filesystem-backed blob namespace.
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

    fn table_dir(&self, table: BlobTableId) -> PathBuf {
        let mut p = self.root.join("blob");
        p.push(table.as_str());
        p
    }

    fn key_path(&self, table: BlobTableId, key: &[u8]) -> PathBuf {
        let mut p = self.table_dir(table);
        p.push(hex(key));
        p
    }
}

impl BlobStore for FsBlobStore {
    async fn put_blob(&self, table: BlobTableId, key: &[u8], value: Bytes) -> Result<()> {
        let path = self.key_path(table, key);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| Error::Backend(format!("create fs blob table dir: {e}")))?;
        }
        write_file_bytes(&path, &value)
    }

    async fn get_blob(&self, table: BlobTableId, key: &[u8]) -> Result<Option<Bytes>> {
        let p = self.key_path(table, key);
        if !p.exists() {
            return Ok(None);
        }
        let b = read_file_bytes(&p)?;
        Ok(Some(Bytes::from(b)))
    }

    async fn delete_blob(&self, table: BlobTableId, key: &[u8]) -> Result<()> {
        let _ = fs::remove_file(self.key_path(table, key));
        Ok(())
    }

    async fn list_prefix(
        &self,
        table: BlobTableId,
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page> {
        let mut all = Vec::<Vec<u8>>::new();
        let base = self.table_dir(table);
        if !base.exists() {
            return Ok(Page {
                keys: Vec::new(),
                next_cursor: None,
            });
        }
        collect_keys_from_group_dir(&base, false, prefix, &mut all)?;
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
    let parent = path
        .parent()
        .ok_or_else(|| Error::Backend("fs write path missing parent".to_string()))?;
    let tmp_path = parent.join(format!(
        ".{}.tmp-{}",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("fs-write"),
        std::process::id()
    ));

    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&tmp_path)
        .map_err(|e| Error::Backend(format!("fs write open: {e}")))?;
    set_no_cache(&file)?;
    file.write_all(bytes)
        .map_err(|e| Error::Backend(format!("fs write: {e}")))?;
    file.sync_all()
        .map_err(|e| Error::Backend(format!("fs sync: {e}")))?;
    drop(file);
    fs::rename(&tmp_path, path).map_err(|e| Error::Backend(format!("fs rename: {e}")))?;
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

fn collect_keys_from_partition_dir(
    dir: &Path,
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
        if name.ends_with(".ver") {
            continue;
        }
        let key = unhex(&name)?;
        if key.starts_with(prefix) {
            out.push(key);
        }
    }
    Ok(())
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
    use crate::logs::keys::LOG_DIR_BY_BLOCK_TABLE;
    use crate::logs::table_specs::LogDirByBlockSpec;
    use crate::store::publication::PublicationState;
    use crate::store::publication::{CasOutcome, MetaPublicationStore, PublicationStore};
    use crate::store::traits::{BlobStore, BlobTableId, MetaStore, PutCond};
    use bytes::Bytes;
    use futures::executor::block_on;
    use std::sync::Arc;

    const TEST_BLOB_TABLE: BlobTableId = BlobTableId::new("test_blob");
    const PAGE_LIMIT: usize = 4;
    const ENTRY_COUNT: usize = PAGE_LIMIT + 1;

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
        let publication_store = Arc::new(MetaPublicationStore::new((*store).clone()));
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
        let publication_store = Arc::new(MetaPublicationStore::new((*store).clone()));
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
        let partition = LogDirByBlockSpec::partition(0);

        block_on(async {
            for index in 0..ENTRY_COUNT as u64 {
                meta_store
                    .scan_put(
                        LOG_DIR_BY_BLOCK_TABLE,
                        &partition,
                        &LogDirByBlockSpec::clustering(index),
                        Bytes::from_static(b"v"),
                        PutCond::Any,
                    )
                    .await
                    .expect("seed scannable meta key");
                let blob_key = format!("list-prefix/{index:04}").into_bytes();
                blob_store
                    .put_blob(TEST_BLOB_TABLE, &blob_key, Bytes::from_static(b"v"))
                    .await
                    .expect("seed blob key");
            }

            let mut meta_cursor = None;
            let mut meta_seen = Vec::new();
            loop {
                let page = meta_store
                    .scan_list(
                        LOG_DIR_BY_BLOCK_TABLE,
                        &partition,
                        b"",
                        meta_cursor.take(),
                        PAGE_LIMIT,
                    )
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
                    .list_prefix(
                        TEST_BLOB_TABLE,
                        b"list-prefix/",
                        blob_cursor.take(),
                        PAGE_LIMIT,
                    )
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
            assert_eq!(meta_seen.len(), ENTRY_COUNT);
            assert_eq!(meta_unique.len(), ENTRY_COUNT);
            assert_eq!(blob_seen.len(), ENTRY_COUNT);
            assert_eq!(blob_unique.len(), ENTRY_COUNT);
        });

        let _ = fs::remove_dir_all(root);
    }
}
