use bytes::Bytes;
use scylla::frame::response::result::CqlValue;
use scylla::prepared_statement::PreparedStatement;
use scylla::{Session, SessionBuilder};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{Duration, sleep};

use crate::codec::finalized_state::{decode_publication_state, encode_publication_state};
use crate::domain::keys::PUBLICATION_STATE_KEY;
use crate::domain::types::PublicationState;
use crate::error::{Error, Result};
use crate::store::publication::{CasOutcome, FenceStore, PublicationStore};
use crate::store::traits::{DelCond, FenceToken, MetaStore, Page, PutCond, PutResult, Record};

const DEFAULT_FENCE_KEY: &str = "global";
const DEFAULT_FENCE_CHECK_INTERVAL_MS: u64 = 1000;
const META_BUCKETS: u16 = 256;
const KNOWN_GROUPS: &[&str] = &[
    "meta",
    "publication_state",
    "block_meta",
    "block_log_headers",
    "block_hash_to_num",
    "log_dir",
    "log_dir_sub",
    "log_dir_frag",
    "open_stream_page",
    "stream_frag_meta",
    "stream_page_meta",
    "manifests",
    "tails",
];

#[derive(Debug, Clone)]
pub struct ScyllaTelemetrySnapshot {
    pub cas_attempts: u64,
    pub cas_conflicts: u64,
    pub timeout_errors: u64,
    pub cas_attempts_by_op: BTreeMap<String, u64>,
    pub cas_conflicts_by_op: BTreeMap<String, u64>,
    pub timeout_errors_by_op: BTreeMap<String, u64>,
}

#[derive(Debug, Default)]
struct ScyllaTelemetry {
    cas_attempts: AtomicU64,
    cas_conflicts: AtomicU64,
    timeout_errors: AtomicU64,
    cas_attempts_by_op: Mutex<BTreeMap<String, u64>>,
    cas_conflicts_by_op: Mutex<BTreeMap<String, u64>>,
    timeout_errors_by_op: Mutex<BTreeMap<String, u64>>,
}

impl ScyllaTelemetry {
    fn record_cas_attempt(&self, op: &str) {
        let _ = self.cas_attempts.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut m) = self.cas_attempts_by_op.lock() {
            *m.entry(op.to_string()).or_default() += 1;
        }
    }

    fn record_cas_conflict(&self, op: &str) {
        let _ = self.cas_conflicts.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut m) = self.cas_conflicts_by_op.lock() {
            *m.entry(op.to_string()).or_default() += 1;
        }
    }

    fn record_timeout(&self, op: &str) {
        let _ = self.timeout_errors.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut m) = self.timeout_errors_by_op.lock() {
            *m.entry(op.to_string()).or_default() += 1;
        }
    }

    fn snapshot(&self) -> ScyllaTelemetrySnapshot {
        ScyllaTelemetrySnapshot {
            cas_attempts: self.cas_attempts.load(Ordering::Relaxed),
            cas_conflicts: self.cas_conflicts.load(Ordering::Relaxed),
            timeout_errors: self.timeout_errors.load(Ordering::Relaxed),
            cas_attempts_by_op: self
                .cas_attempts_by_op
                .lock()
                .map(|m| m.clone())
                .unwrap_or_default(),
            cas_conflicts_by_op: self
                .cas_conflicts_by_op
                .lock()
                .map(|m| m.clone())
                .unwrap_or_default(),
            timeout_errors_by_op: self
                .timeout_errors_by_op
                .lock()
                .map(|m| m.clone())
                .unwrap_or_default(),
        }
    }
}

#[derive(Clone)]
pub struct ScyllaMetaStore {
    session: Arc<Session>,
    fence_key: String,
    max_retries: u32,
    base_delay_ms: u64,
    max_delay_ms: u64,
    fence_check_interval_ms: u64,
    cached_min_epoch: Arc<AtomicU64>,
    last_fence_check_ms: Arc<AtomicU64>,
    telemetry: Arc<ScyllaTelemetry>,
    stmts: Arc<ScyllaStatements>,
}

#[derive(Debug)]
struct ScyllaStatements {
    set_min_epoch: PreparedStatement,
    get_min_epoch: PreparedStatement,
    get: PreparedStatement,
    put_any: PreparedStatement,
    put_if_absent: PreparedStatement,
    put_if_version: PreparedStatement,
    delete_any: PreparedStatement,
    delete_if_version: PreparedStatement,
    list_prefix: PreparedStatement,
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
                     bucket smallint, \
                     k blob, \
                     v blob, \
                     version bigint, \
                     PRIMARY KEY ((grp, bucket), k)\
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

        let statements = ScyllaStatements {
            set_min_epoch: prepare_statement(
                &session,
                format!("INSERT INTO {fence_table} (id, min_epoch) VALUES (?, ?)"),
                "set_min_epoch",
            )
            .await?,
            get_min_epoch: prepare_statement(
                &session,
                format!("SELECT min_epoch FROM {fence_table} WHERE id = ?"),
                "get_min_epoch",
            )
            .await?,
            get: prepare_statement(
                &session,
                format!("SELECT v, version FROM {table} WHERE grp = ? AND bucket = ? AND k = ?"),
                "get",
            )
            .await?,
            put_any: prepare_statement(
                &session,
                format!(
                    "INSERT INTO {table} (grp, bucket, k, v, version) VALUES (?, ?, ?, ?, ?)"
                ),
                "put_any",
            )
            .await?,
            put_if_absent: prepare_statement(
                &session,
                format!(
                    "INSERT INTO {table} (grp, bucket, k, v, version) VALUES (?, ?, ?, ?, ?) IF NOT EXISTS"
                ),
                "put_if_absent",
            )
            .await?,
            put_if_version: prepare_statement(
                &session,
                format!(
                    "UPDATE {table} SET v = ?, version = ? WHERE grp = ? AND bucket = ? AND k = ? IF version = ?"
                ),
                "put_if_version",
            )
            .await?,
            delete_any: prepare_statement(
                &session,
                format!("DELETE FROM {table} WHERE grp = ? AND bucket = ? AND k = ?"),
                "delete_any",
            )
            .await?,
            delete_if_version: prepare_statement(
                &session,
                format!(
                    "DELETE FROM {table} WHERE grp = ? AND bucket = ? AND k = ? IF version = ?"
                ),
                "delete_if_version",
            )
            .await?,
            list_prefix: prepare_statement(
                &session,
                format!("SELECT k FROM {table} WHERE grp = ? AND bucket = ?"),
                "list_prefix",
            )
            .await?,
        };

        Ok(Self {
            session: Arc::new(session),
            fence_key: DEFAULT_FENCE_KEY.to_string(),
            max_retries: 10,
            base_delay_ms: 50,
            max_delay_ms: 5000,
            fence_check_interval_ms: DEFAULT_FENCE_CHECK_INTERVAL_MS,
            cached_min_epoch: Arc::new(AtomicU64::new(0)),
            last_fence_check_ms: Arc::new(AtomicU64::new(0)),
            telemetry: Arc::new(ScyllaTelemetry::default()),
            stmts: Arc::new(statements),
        })
    }

    pub fn with_retry_policy(
        mut self,
        max_retries: u32,
        base_delay_ms: u64,
        max_delay_ms: u64,
    ) -> Self {
        self.max_retries = max_retries;
        self.base_delay_ms = base_delay_ms;
        self.max_delay_ms = max_delay_ms;
        self
    }

    pub fn telemetry_snapshot(&self) -> ScyllaTelemetrySnapshot {
        self.telemetry.snapshot()
    }

    pub async fn set_min_epoch(&self, min_epoch: u64) -> Result<()> {
        let stmt = self.stmts.set_min_epoch.clone();
        self.with_retry("set_min_epoch", || async {
            self.session
                .execute_unpaged(&stmt, (self.fence_key.as_str(), min_epoch as i64))
                .await
                .map_err(|e| Error::Backend(format!("set min epoch: {e}")))?;
            Ok(())
        })
        .await?;
        self.cached_min_epoch.store(min_epoch, Ordering::Relaxed);
        self.last_fence_check_ms
            .store(now_millis_u64(), Ordering::Relaxed);
        Ok(())
    }

    async fn validate_fence(&self, fence: FenceToken) -> Result<()> {
        let cached_min_epoch = self.cached_min_epoch.load(Ordering::Relaxed);
        if fence.0 < cached_min_epoch {
            return Err(Error::LeaseLost);
        }

        let now_ms = now_millis_u64();
        let last_check_ms = self.last_fence_check_ms.load(Ordering::Relaxed);
        if now_ms.saturating_sub(last_check_ms) < self.fence_check_interval_ms {
            return Ok(());
        }

        let stmt = self.stmts.get_min_epoch.clone();
        let res = self
            .with_retry("validate_fence", || async {
                self.session
                    .execute_unpaged(&stmt, (self.fence_key.as_str(),))
                    .await
                    .map_err(|e| Error::Backend(format!("get min epoch: {e}")))
            })
            .await?;

        let min_epoch = first_col_i64(res).map(|v| v as u64).unwrap_or(0);
        self.cached_min_epoch.store(min_epoch, Ordering::Relaxed);
        self.last_fence_check_ms.store(now_ms, Ordering::Relaxed);
        if fence.0 < min_epoch {
            return Err(Error::LeaseLost);
        }
        Ok(())
    }
}

impl MetaStore for ScyllaMetaStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Record>> {
        let grp = extract_group(key);
        let key_vec = key.to_vec();
        let bucket = key_bucket(&key_vec);
        let stmt = self.stmts.get.clone();
        let res = self
            .with_retry("get", || async {
                self.session
                    .execute_unpaged(&stmt, (grp.as_str(), bucket, key_vec.clone()))
                    .await
                    .map_err(|e| Error::Backend(format!("scylla get: {e}")))
            })
            .await?;

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
        let key_vec = key.to_vec();
        let bucket = key_bucket(&key_vec);

        match cond {
            PutCond::Any => {
                let stmt = self.stmts.put_any.clone();
                self.with_retry("put_any", || async {
                    self.session
                        .execute_unpaged(
                            &stmt,
                            (
                                grp.as_str(),
                                bucket,
                                key_vec.clone(),
                                value.to_vec(),
                                now_millis_u64() as i64,
                            ),
                        )
                        .await
                        .map_err(|e| Error::Backend(format!("scylla put any: {e}")))?;
                    Ok(())
                })
                .await?;
                Ok(PutResult {
                    applied: true,
                    version: None,
                })
            }
            PutCond::IfAbsent => {
                let cas_op = format!("put_if_absent:{grp}");
                self.telemetry.record_cas_attempt(&cas_op);
                let stmt = self.stmts.put_if_absent.clone();
                let res = self
                    .with_retry("put_if_absent", || async {
                        self.session
                            .execute_unpaged(
                                &stmt,
                                (grp.as_str(), bucket, key_vec.clone(), value.to_vec(), 1_i64),
                            )
                            .await
                            .map_err(|e| Error::Backend(format!("scylla put if absent: {e}")))
                    })
                    .await?;
                let applied = lwt_applied(res)?;
                if !applied {
                    self.telemetry.record_cas_conflict(&cas_op);
                }
                Ok(PutResult {
                    applied,
                    version: applied.then_some(1),
                })
            }
            PutCond::IfVersion(v) => {
                let cas_op = format!("put_if_version:{grp}");
                self.telemetry.record_cas_attempt(&cas_op);
                let stmt = self.stmts.put_if_version.clone();
                let res = self
                    .with_retry("put_if_version", || async {
                        self.session
                            .execute_unpaged(
                                &stmt,
                                (
                                    value.to_vec(),
                                    (v + 1) as i64,
                                    grp.as_str(),
                                    bucket,
                                    key_vec.clone(),
                                    v as i64,
                                ),
                            )
                            .await
                            .map_err(|e| Error::Backend(format!("scylla put if version: {e}")))
                    })
                    .await?;
                let applied = lwt_applied(res)?;
                if !applied {
                    self.telemetry.record_cas_conflict(&cas_op);
                }
                Ok(PutResult {
                    applied,
                    version: applied.then_some(v + 1),
                })
            }
        }
    }

    async fn delete(&self, key: &[u8], cond: DelCond, fence: FenceToken) -> Result<()> {
        self.validate_fence(fence).await?;

        let grp = extract_group(key);
        let bucket = key_bucket(key);
        match cond {
            DelCond::Any => {
                let stmt = self.stmts.delete_any.clone();
                self.with_retry("delete_any", || async {
                    self.session
                        .execute_unpaged(&stmt, (grp.as_str(), bucket, key.to_vec()))
                        .await
                        .map_err(|e| Error::Backend(format!("scylla delete: {e}")))?;
                    Ok(())
                })
                .await?;
            }
            DelCond::IfVersion(v) => {
                let stmt = self.stmts.delete_if_version.clone();
                let _ = self
                    .with_retry("delete_if_version", || async {
                        self.session
                            .execute_unpaged(&stmt, (grp.as_str(), bucket, key.to_vec(), v as i64))
                            .await
                            .map_err(|e| Error::Backend(format!("scylla delete if version: {e}")))
                    })
                    .await?;
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
        let has_cursor = cursor.is_some();
        let start = cursor.unwrap_or_default();

        for grp in groups {
            for bucket in 0..META_BUCKETS {
                if keys.len() >= limit {
                    break;
                }
                let stmt = self.stmts.list_prefix.clone();
                let res = self
                    .with_retry("list_prefix", || async {
                        self.session
                            .execute_unpaged(&stmt, (grp.as_str(), bucket as i16))
                            .await
                            .map_err(|e| Error::Backend(format!("scylla list_prefix: {e}")))
                    })
                    .await?;

                if let Ok(rows_result) = res.into_rows_result()
                    && let Ok(iter) = rows_result.rows::<(Vec<u8>,)>()
                {
                    for row in iter {
                        let (k,) = row.map_err(|e| Error::Backend(format!("decode row: {e}")))?;
                        if (has_cursor && k <= start)
                            || (!has_cursor && k < start)
                            || !k.starts_with(prefix)
                        {
                            continue;
                        }
                        keys.push(k);
                        if keys.len() >= limit {
                            break;
                        }
                    }
                }
            }
            if keys.len() >= limit {
                break;
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

impl ScyllaMetaStore {
    async fn with_retry<T, F, Fut>(&self, op: &str, mut f: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: core::future::Future<Output = Result<T>>,
    {
        let mut attempt: u32 = 0;
        loop {
            match f().await {
                Ok(v) => return Ok(v),
                Err(e) => {
                    if is_timeout_backend_error(&e) {
                        self.telemetry.record_timeout(op);
                    }
                    if attempt >= self.max_retries || !is_retryable_backend_error(&e) {
                        return Err(e);
                    }
                    let backoff =
                        compute_backoff_ms(attempt, self.base_delay_ms, self.max_delay_ms);
                    sleep(Duration::from_millis(backoff)).await;
                    attempt = attempt.saturating_add(1);
                }
            }
        }
    }
}

async fn prepare_statement(
    session: &Session,
    query: String,
    label: &str,
) -> Result<PreparedStatement> {
    session
        .prepare(query)
        .await
        .map_err(|e| Error::Backend(format!("prepare statement {label}: {e}")))
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

#[allow(deprecated)]
fn lwt_applied(res: scylla::QueryResult) -> Result<bool> {
    let legacy = res
        .into_legacy_result()
        .map_err(|e| Error::Backend(format!("scylla lwt decode: {e}")))?;
    let Some(rows) = legacy.rows else {
        return Err(Error::Decode("missing lwt rows"));
    };
    let Some(first) = rows.first() else {
        return Err(Error::Decode("empty lwt rows"));
    };
    let Some(Some(first_col)) = first.columns.first() else {
        return Err(Error::Decode("missing [applied] column"));
    };
    match first_col {
        CqlValue::Boolean(b) => Ok(*b),
        _ => Err(Error::Decode("invalid [applied] column type")),
    }
}

impl PublicationStore for ScyllaMetaStore {
    async fn load(&self) -> Result<Option<PublicationState>> {
        let Some(record) = self.get(PUBLICATION_STATE_KEY).await? else {
            return Ok(None);
        };
        Ok(Some(decode_publication_state(&record.value)?))
    }

    async fn create_if_absent(
        &self,
        initial: &PublicationState,
    ) -> Result<CasOutcome<PublicationState>> {
        let result = self
            .put(
                PUBLICATION_STATE_KEY,
                encode_publication_state(initial),
                PutCond::IfAbsent,
                FenceToken(initial.epoch),
            )
            .await?;
        if result.applied {
            return Ok(CasOutcome::Applied(initial.clone()));
        }
        Ok(CasOutcome::Failed {
            current: self.load().await?,
        })
    }

    async fn compare_and_set(
        &self,
        expected: &PublicationState,
        next: &PublicationState,
    ) -> Result<CasOutcome<PublicationState>> {
        let Some(current) = self.get(PUBLICATION_STATE_KEY).await? else {
            return Ok(CasOutcome::Failed { current: None });
        };
        let current_state = decode_publication_state(&current.value)?;
        if current_state != *expected {
            return Ok(CasOutcome::Failed {
                current: Some(current_state),
            });
        }

        let result = self
            .put(
                PUBLICATION_STATE_KEY,
                encode_publication_state(next),
                PutCond::IfVersion(current.version),
                FenceToken(next.epoch),
            )
            .await?;
        if result.applied {
            return Ok(CasOutcome::Applied(next.clone()));
        }

        Ok(CasOutcome::Failed {
            current: self.load().await?,
        })
    }
}

impl FenceStore for ScyllaMetaStore {
    async fn advance_fence(&self, min_epoch: u64) -> Result<()> {
        let next_min_epoch = self.cached_min_epoch.load(Ordering::Relaxed).max(min_epoch);
        self.set_min_epoch(next_min_epoch).await
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

fn key_bucket(key: &[u8]) -> i16 {
    // FNV-1a hash for deterministic, low-cost bucket placement.
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in key {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    (hash % u64::from(META_BUCKETS)) as i16
}

fn compute_backoff_ms(attempt: u32, base_ms: u64, max_ms: u64) -> u64 {
    let factor = 1u64 << core::cmp::min(attempt, 8);
    core::cmp::min(base_ms.saturating_mul(factor), max_ms)
}

fn now_millis_u64() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn is_timeout_backend_error(err: &Error) -> bool {
    let Error::Backend(msg) = err else {
        return false;
    };
    msg.to_ascii_lowercase().contains("timeout")
}

fn is_retryable_backend_error(err: &Error) -> bool {
    let Error::Backend(msg) = err else {
        return false;
    };
    let s = msg.to_ascii_lowercase();
    is_timeout_backend_error(err)
        || s.contains("temporar")
        || s.contains("connection")
        || s.contains("reset")
        || s.contains("refused")
        || s.contains("unavailable")
        || s.contains("overloaded")
        || s.contains("failed to perform a connection setup request")
}
