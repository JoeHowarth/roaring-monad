use bytes::Bytes;
use scylla::{Session, SessionBuilder};
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::store::traits::{DelCond, FenceToken, MetaStore, Page, PutCond, PutResult, Record};

const DEFAULT_FENCE_KEY: &str = "global";
const KNOWN_GROUPS: &[&str] = &[
    "meta",
    "block_meta",
    "block_hash_to_num",
    "manifests",
    "tails",
    "topic0_mode",
    "topic0_stats",
    "log_locators",
];

#[derive(Clone)]
pub struct ScyllaMetaStore {
    session: Arc<Session>,
    table: String,
    fence_table: String,
    fence_key: String,
}

impl ScyllaMetaStore {
    pub async fn new(nodes: &[String], keyspace: &str) -> Result<Self> {
        let mut builder = SessionBuilder::new();
        for node in nodes {
            builder = builder.known_node(node);
        }
        let session = builder
            .build()
            .await
            .map_err(|e| Error::Backend(format!("scylla connect: {e}")))?;

        session
            .query_unpaged(
                format!(
                    "CREATE KEYSPACE IF NOT EXISTS {keyspace} \
                     WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
                ),
                &[],
            )
            .await
            .map_err(|e| Error::Backend(format!("create keyspace: {e}")))?;

        session
            .use_keyspace(keyspace, false)
            .await
            .map_err(|e| Error::Backend(format!("use keyspace: {e}")))?;

        let table = "meta_kv".to_string();
        let fence_table = "meta_fence".to_string();

        session
            .query_unpaged(
                format!(
                    "CREATE TABLE IF NOT EXISTS {table} (\
                     grp text, \
                     k blob, \
                     v blob, \
                     version bigint, \
                     PRIMARY KEY ((grp), k)\
                    )"
                ),
                &[],
            )
            .await
            .map_err(|e| Error::Backend(format!("create table meta_kv: {e}")))?;

        session
            .query_unpaged(
                format!(
                    "CREATE TABLE IF NOT EXISTS {fence_table} (\
                     id text PRIMARY KEY, \
                     min_epoch bigint\
                    )"
                ),
                &[],
            )
            .await
            .map_err(|e| Error::Backend(format!("create table meta_fence: {e}")))?;

        Ok(Self {
            session: Arc::new(session),
            table,
            fence_table,
            fence_key: DEFAULT_FENCE_KEY.to_string(),
        })
    }

    pub async fn set_min_epoch(&self, min_epoch: u64) -> Result<()> {
        self.session
            .query_unpaged(
                format!(
                    "INSERT INTO {} (id, min_epoch) VALUES (?, ?)",
                    self.fence_table
                ),
                (self.fence_key.as_str(), min_epoch as i64),
            )
            .await
            .map_err(|e| Error::Backend(format!("set min epoch: {e}")))?;
        Ok(())
    }

    async fn validate_fence(&self, fence: FenceToken) -> Result<()> {
        let res = self
            .session
            .query_unpaged(
                format!("SELECT min_epoch FROM {} WHERE id = ?", self.fence_table),
                (self.fence_key.as_str(),),
            )
            .await
            .map_err(|e| Error::Backend(format!("get min epoch: {e}")))?;

        let min_epoch = first_col_i64(res).map(|v| v as u64).unwrap_or(0);
        if fence.0 < min_epoch {
            return Err(Error::LeaseLost);
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl MetaStore for ScyllaMetaStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Record>> {
        let grp = extract_group(key);
        let res = self
            .session
            .query_unpaged(
                format!(
                    "SELECT v, version FROM {} WHERE grp = ? AND k = ?",
                    self.table
                ),
                (grp.as_str(), key.to_vec()),
            )
            .await
            .map_err(|e| Error::Backend(format!("scylla get: {e}")))?;

        let row = first_row_v_version(res);
        Ok(row.map(|(v, version)| Record {
            value: Bytes::from(v),
            version: version as u64,
        }))
    }

    async fn put(
        &self,
        key: &[u8],
        value: Bytes,
        cond: PutCond,
        fence: FenceToken,
    ) -> Result<PutResult> {
        self.validate_fence(fence).await?;

        let grp = extract_group(key);
        let current = self.get(key).await?;
        let next_version = current.as_ref().map_or(1, |r| r.version + 1);

        let applied = match cond {
            PutCond::Any => {
                self.session
                    .query_unpaged(
                        format!(
                            "INSERT INTO {} (grp, k, v, version) VALUES (?, ?, ?, ?)",
                            self.table
                        ),
                        (
                            grp.as_str(),
                            key.to_vec(),
                            value.to_vec(),
                            next_version as i64,
                        ),
                    )
                    .await
                    .map_err(|e| Error::Backend(format!("scylla put any: {e}")))?;
                true
            }
            PutCond::IfAbsent => {
                let res = self
                    .session
                    .query_unpaged(
                        format!(
                            "INSERT INTO {} (grp, k, v, version) VALUES (?, ?, ?, ?) IF NOT EXISTS",
                            self.table
                        ),
                        (grp.as_str(), key.to_vec(), value.to_vec(), 1_i64),
                    )
                    .await
                    .map_err(|e| Error::Backend(format!("scylla put if absent: {e}")))?;
                lwt_applied(res).unwrap_or(false)
            }
            PutCond::IfVersion(v) => {
                let res = self
                    .session
                    .query_unpaged(
                        format!(
                            "UPDATE {} SET v = ?, version = ? WHERE grp = ? AND k = ? IF version = ?",
                            self.table
                        ),
                        (
                            value.to_vec(),
                            next_version as i64,
                            grp.as_str(),
                            key.to_vec(),
                            v as i64,
                        ),
                    )
                    .await
                    .map_err(|e| Error::Backend(format!("scylla put if version: {e}")))?;
                lwt_applied(res).unwrap_or(false)
            }
        };

        let version = self.get(key).await?.map(|r| r.version);
        Ok(PutResult { applied, version })
    }

    async fn delete(&self, key: &[u8], cond: DelCond, fence: FenceToken) -> Result<()> {
        self.validate_fence(fence).await?;

        let grp = extract_group(key);
        match cond {
            DelCond::Any => {
                self.session
                    .query_unpaged(
                        format!("DELETE FROM {} WHERE grp = ? AND k = ?", self.table),
                        (grp.as_str(), key.to_vec()),
                    )
                    .await
                    .map_err(|e| Error::Backend(format!("scylla delete: {e}")))?;
            }
            DelCond::IfVersion(v) => {
                let _ = self
                    .session
                    .query_unpaged(
                        format!(
                            "DELETE FROM {} WHERE grp = ? AND k = ? IF version = ?",
                            self.table
                        ),
                        (grp.as_str(), key.to_vec(), v as i64),
                    )
                    .await
                    .map_err(|e| Error::Backend(format!("scylla delete if version: {e}")))?;
            }
        }
        Ok(())
    }

    async fn list_prefix(
        &self,
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page> {
        let groups: Vec<String> = if prefix.is_empty() {
            KNOWN_GROUPS.iter().map(|s| s.to_string()).collect()
        } else {
            vec![extract_group(prefix)]
        };

        let mut keys = Vec::new();
        let start = cursor.unwrap_or_default();

        for grp in groups {
            if keys.len() >= limit {
                break;
            }
            let res = self
                .session
                .query_unpaged(
                    format!("SELECT k FROM {} WHERE grp = ?", self.table),
                    (grp.as_str(),),
                )
                .await
                .map_err(|e| Error::Backend(format!("scylla list_prefix: {e}")))?;

            if let Ok(rows_result) = res.into_rows_result()
                && let Ok(iter) = rows_result.rows::<(Vec<u8>,)>()
            {
                for row in iter {
                    let (k,) = row.map_err(|e| Error::Backend(format!("decode row: {e}")))?;
                    if k < start || !k.starts_with(prefix) {
                        continue;
                    }
                    keys.push(k);
                    if keys.len() >= limit {
                        break;
                    }
                }
            }
        }

        keys.sort();
        let next_cursor = if keys.len() == limit {
            keys.last().cloned()
        } else {
            None
        };
        Ok(Page { keys, next_cursor })
    }
}

fn lwt_applied(res: scylla::QueryResult) -> Option<bool> {
    let rows_result = res.into_rows_result().ok()?;
    let mut it = rows_result.rows::<(bool,)>().ok()?;
    it.next().and_then(|r| r.ok()).map(|x| x.0)
}

fn first_col_i64(res: scylla::QueryResult) -> Option<i64> {
    let rows_result = res.into_rows_result().ok()?;
    let mut it = rows_result.rows::<(i64,)>().ok()?;
    it.next().and_then(|r| r.ok()).map(|x| x.0)
}

fn first_row_v_version(res: scylla::QueryResult) -> Option<(Vec<u8>, i64)> {
    let rows_result = res.into_rows_result().ok()?;
    let mut it = rows_result.rows::<(Vec<u8>, i64)>().ok()?;
    it.next().and_then(|r| r.ok())
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
