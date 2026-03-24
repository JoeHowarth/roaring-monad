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

use crate::error::{Error, Result};
use crate::store::manifest::{REQUIRED_POINT_TABLES, REQUIRED_SCANNABLE_TABLES};
use crate::store::traits::{
    DelCond, MetaStore, Page, PutCond, PutResult, Record, ScannableTableId, TableId,
};

const DEFAULT_FENCE_KEY: &str = "global";
const META_BUCKETS: u16 = 256;
const FENCE_TABLE_NAME: &str = "meta_fence";

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

/// Cheap clone handle to the same Scylla session, prepared statements, and telemetry.
#[derive(Clone)]
pub struct ScyllaMetaStore {
    session: Arc<Session>,
    fence_key: String,
    max_retries: u32,
    base_delay_ms: u64,
    max_delay_ms: u64,
    cached_min_epoch: Arc<AtomicU64>,
    telemetry: Arc<ScyllaTelemetry>,
    stmts: Arc<ScyllaStatements>,
}

#[derive(Debug)]
struct ScyllaStatements {
    set_min_epoch: PreparedStatement,
    point: BTreeMap<TableId, PointTableStatements>,
    scannable: BTreeMap<ScannableTableId, ScannableTableStatements>,
}

#[derive(Debug)]
struct PointTableStatements {
    get: PreparedStatement,
    put_any: PreparedStatement,
    put_if_absent: PreparedStatement,
    put_if_version: PreparedStatement,
    delete_any: PreparedStatement,
    delete_if_version: PreparedStatement,
}

#[derive(Debug)]
struct ScannableTableStatements {
    get: PreparedStatement,
    put_any: PreparedStatement,
    put_if_absent: PreparedStatement,
    put_if_version: PreparedStatement,
    delete_any: PreparedStatement,
    delete_if_version: PreparedStatement,
    list: PreparedStatement,
}

#[derive(Debug, Clone, Copy)]
struct PointTableSchema {
    id: TableId,
    table_name: &'static str,
}

impl PointTableSchema {
    const fn named(id: TableId) -> Self {
        Self {
            id,
            table_name: id.as_str(),
        }
    }
}

impl PointTableSchema {
    async fn ensure(self, session: &Session) -> Result<()> {
        session
            .query_unpaged(
                format!(
                    "CREATE TABLE IF NOT EXISTS {} (\
                     bucket smallint, \
                     k blob, \
                     v blob, \
                     version bigint, \
                     PRIMARY KEY ((bucket), k)\
                    )",
                    self.table_name
                ),
                &[],
            )
            .await
            .map_err(|e| Error::Backend(format!("create table {}: {e}", self.table_name)))?;
        Ok(())
    }

    async fn prepare(self, session: &Session) -> Result<PointTableStatements> {
        Ok(PointTableStatements {
            get: prepare_statement(
                session,
                format!(
                    "SELECT v, version FROM {} WHERE bucket = ? AND k = ?",
                    self.table_name
                ),
                "point_get",
            )
            .await?,
            put_any: prepare_statement(
                session,
                format!(
                    "INSERT INTO {} (bucket, k, v, version) VALUES (?, ?, ?, ?)",
                    self.table_name
                ),
                "point_put_any",
            )
            .await?,
            put_if_absent: prepare_statement(
                session,
                format!(
                    "INSERT INTO {} (bucket, k, v, version) VALUES (?, ?, ?, ?) IF NOT EXISTS",
                    self.table_name
                ),
                "point_put_if_absent",
            )
            .await?,
            put_if_version: prepare_statement(
                session,
                format!(
                    "UPDATE {} SET v = ?, version = ? WHERE bucket = ? AND k = ? IF version = ?",
                    self.table_name
                ),
                "point_put_if_version",
            )
            .await?,
            delete_any: prepare_statement(
                session,
                format!("DELETE FROM {} WHERE bucket = ? AND k = ?", self.table_name),
                "point_delete_any",
            )
            .await?,
            delete_if_version: prepare_statement(
                session,
                format!(
                    "DELETE FROM {} WHERE bucket = ? AND k = ? IF version = ?",
                    self.table_name
                ),
                "point_delete_if_version",
            )
            .await?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct ScannableTableSchema {
    id: ScannableTableId,
    table_name: &'static str,
}

impl ScannableTableSchema {
    const fn named(id: ScannableTableId) -> Self {
        Self {
            id,
            table_name: id.as_str(),
        }
    }
}

impl ScannableTableSchema {
    async fn ensure(self, session: &Session) -> Result<()> {
        session
            .query_unpaged(
                format!(
                    "CREATE TABLE IF NOT EXISTS {} (\
                     pk blob, \
                     ck blob, \
                     v blob, \
                     version bigint, \
                     PRIMARY KEY ((pk), ck)\
                    )",
                    self.table_name
                ),
                &[],
            )
            .await
            .map_err(|e| Error::Backend(format!("create table {}: {e}", self.table_name)))?;
        Ok(())
    }

    async fn prepare(self, session: &Session) -> Result<ScannableTableStatements> {
        Ok(ScannableTableStatements {
            get: prepare_statement(
                session,
                format!(
                    "SELECT v, version FROM {} WHERE pk = ? AND ck = ?",
                    self.table_name
                ),
                "scan_get",
            )
            .await?,
            put_any: prepare_statement(
                session,
                format!(
                    "INSERT INTO {} (pk, ck, v, version) VALUES (?, ?, ?, ?)",
                    self.table_name
                ),
                "scan_put_any",
            )
            .await?,
            put_if_absent: prepare_statement(
                session,
                format!(
                    "INSERT INTO {} (pk, ck, v, version) VALUES (?, ?, ?, ?) IF NOT EXISTS",
                    self.table_name
                ),
                "scan_put_if_absent",
            )
            .await?,
            put_if_version: prepare_statement(
                session,
                format!(
                    "UPDATE {} SET v = ?, version = ? WHERE pk = ? AND ck = ? IF version = ?",
                    self.table_name
                ),
                "scan_put_if_version",
            )
            .await?,
            delete_any: prepare_statement(
                session,
                format!("DELETE FROM {} WHERE pk = ? AND ck = ?", self.table_name),
                "scan_delete_any",
            )
            .await?,
            delete_if_version: prepare_statement(
                session,
                format!(
                    "DELETE FROM {} WHERE pk = ? AND ck = ? IF version = ?",
                    self.table_name
                ),
                "scan_delete_if_version",
            )
            .await?,
            list: prepare_statement(
                session,
                format!("SELECT ck FROM {} WHERE pk = ?", self.table_name),
                "scan_list",
            )
            .await?,
        })
    }
}

const POINT_TABLE_SCHEMAS: [PointTableSchema; REQUIRED_POINT_TABLES.len()] = [
    PointTableSchema::named(REQUIRED_POINT_TABLES[0]),
    PointTableSchema::named(REQUIRED_POINT_TABLES[1]),
    PointTableSchema::named(REQUIRED_POINT_TABLES[2]),
    PointTableSchema::named(REQUIRED_POINT_TABLES[3]),
    PointTableSchema::named(REQUIRED_POINT_TABLES[4]),
    PointTableSchema::named(REQUIRED_POINT_TABLES[5]),
    PointTableSchema::named(REQUIRED_POINT_TABLES[6]),
    PointTableSchema::named(REQUIRED_POINT_TABLES[7]),
    PointTableSchema::named(REQUIRED_POINT_TABLES[8]),
    PointTableSchema::named(REQUIRED_POINT_TABLES[9]),
    PointTableSchema::named(REQUIRED_POINT_TABLES[10]),
];

const SCANNABLE_TABLE_SCHEMAS: [ScannableTableSchema; REQUIRED_SCANNABLE_TABLES.len()] = [
    ScannableTableSchema::named(REQUIRED_SCANNABLE_TABLES[0]),
    ScannableTableSchema::named(REQUIRED_SCANNABLE_TABLES[1]),
    ScannableTableSchema::named(REQUIRED_SCANNABLE_TABLES[2]),
    ScannableTableSchema::named(REQUIRED_SCANNABLE_TABLES[3]),
    ScannableTableSchema::named(REQUIRED_SCANNABLE_TABLES[4]),
    ScannableTableSchema::named(REQUIRED_SCANNABLE_TABLES[5]),
];

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

        let mut point = BTreeMap::new();
        for schema in POINT_TABLE_SCHEMAS {
            schema.ensure(&session).await?;
            point.insert(schema.id, schema.prepare(&session).await?);
        }

        let mut scannable = BTreeMap::new();
        for schema in SCANNABLE_TABLE_SCHEMAS {
            schema.ensure(&session).await?;
            scannable.insert(schema.id, schema.prepare(&session).await?);
        }

        session
            .query_unpaged(
                format!(
                    "CREATE TABLE IF NOT EXISTS {FENCE_TABLE_NAME} (\
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
                format!("INSERT INTO {FENCE_TABLE_NAME} (id, min_epoch) VALUES (?, ?)"),
                "set_min_epoch",
            )
            .await?,
            point,
            scannable,
        };

        Ok(Self {
            session: Arc::new(session),
            fence_key: DEFAULT_FENCE_KEY.to_string(),
            max_retries: 10,
            base_delay_ms: 50,
            max_delay_ms: 5000,
            cached_min_epoch: Arc::new(AtomicU64::new(0)),
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
        Ok(())
    }

    fn point_statements(&self, table: TableId) -> Result<&PointTableStatements> {
        self.stmts
            .point
            .get(&table)
            .ok_or_else(|| Error::Backend(format!("unknown scylla point table {}", table.as_str())))
    }

    fn scannable_statements(&self, table: ScannableTableId) -> Result<&ScannableTableStatements> {
        self.stmts.scannable.get(&table).ok_or_else(|| {
            Error::Backend(format!("unknown scylla scannable table {}", table.as_str()))
        })
    }
}

impl MetaStore for ScyllaMetaStore {
    async fn get(&self, table: TableId, key: &[u8]) -> Result<Option<Record>> {
        let key_vec = key.to_vec();
        let bucket = key_bucket(&key_vec);
        let stmt = self.point_statements(table)?.get.clone();
        let res = self
            .with_retry("get", || async {
                self.session
                    .execute_unpaged(&stmt, (bucket, key_vec.clone()))
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
        table: TableId,
        key: &[u8],
        value: Bytes,
        cond: PutCond,
    ) -> Result<PutResult> {
        let key_vec = key.to_vec();
        let bucket = key_bucket(&key_vec);

        match cond {
            PutCond::Any => {
                let stmt = self.point_statements(table)?.put_any.clone();
                self.with_retry("put_any", || async {
                    self.session
                        .execute_unpaged(
                            &stmt,
                            (
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
                let cas_op = format!("put_if_absent:{}", table.as_str());
                self.telemetry.record_cas_attempt(&cas_op);
                let stmt = self.point_statements(table)?.put_if_absent.clone();
                let res = self
                    .with_retry("put_if_absent", || async {
                        self.session
                            .execute_unpaged(
                                &stmt,
                                (bucket, key_vec.clone(), value.to_vec(), 1_i64),
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
                let cas_op = format!("put_if_version:{}", table.as_str());
                self.telemetry.record_cas_attempt(&cas_op);
                let stmt = self.point_statements(table)?.put_if_version.clone();
                let res = self
                    .with_retry("put_if_version", || async {
                        self.session
                            .execute_unpaged(
                                &stmt,
                                (
                                    value.to_vec(),
                                    (v + 1) as i64,
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

    async fn delete(&self, table: TableId, key: &[u8], cond: DelCond) -> Result<()> {
        let bucket = key_bucket(key);
        match cond {
            DelCond::Any => {
                let stmt = self.point_statements(table)?.delete_any.clone();
                self.with_retry("delete_any", || async {
                    self.session
                        .execute_unpaged(&stmt, (bucket, key.to_vec()))
                        .await
                        .map_err(|e| Error::Backend(format!("scylla delete: {e}")))?;
                    Ok(())
                })
                .await?;
            }
            DelCond::IfVersion(v) => {
                let stmt = self.point_statements(table)?.delete_if_version.clone();
                let _ = self
                    .with_retry("delete_if_version", || async {
                        self.session
                            .execute_unpaged(&stmt, (bucket, key.to_vec(), v as i64))
                            .await
                            .map_err(|e| Error::Backend(format!("scylla delete if version: {e}")))
                    })
                    .await?;
            }
        }
        Ok(())
    }

    async fn scan_get(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
    ) -> Result<Option<Record>> {
        let stmt = self.scannable_statements(table)?.get.clone();
        let res = self
            .with_retry("scan_get", || async {
                self.session
                    .execute_unpaged(&stmt, (partition.to_vec(), clustering.to_vec()))
                    .await
                    .map_err(|e| Error::Backend(format!("scylla scan get: {e}")))
            })
            .await?;

        let row = first_row_v_version(res);
        Ok(row.map(|(v, version)| Record {
            value: Bytes::from(v),
            version: version as u64,
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
        let partition_vec = partition.to_vec();
        let clustering_vec = clustering.to_vec();

        match cond {
            PutCond::Any => {
                let stmt = self.scannable_statements(table)?.put_any.clone();
                self.with_retry("scan_put_any", || async {
                    self.session
                        .execute_unpaged(
                            &stmt,
                            (
                                partition_vec.clone(),
                                clustering_vec.clone(),
                                value.to_vec(),
                                now_millis_u64() as i64,
                            ),
                        )
                        .await
                        .map_err(|e| Error::Backend(format!("scylla scan put any: {e}")))?;
                    Ok(())
                })
                .await?;
                Ok(PutResult {
                    applied: true,
                    version: None,
                })
            }
            PutCond::IfAbsent => {
                let cas_op = format!("scan_put_if_absent:{}", table.as_str());
                self.telemetry.record_cas_attempt(&cas_op);
                let stmt = self.scannable_statements(table)?.put_if_absent.clone();
                let res = self
                    .with_retry("scan_put_if_absent", || async {
                        self.session
                            .execute_unpaged(
                                &stmt,
                                (
                                    partition_vec.clone(),
                                    clustering_vec.clone(),
                                    value.to_vec(),
                                    1_i64,
                                ),
                            )
                            .await
                            .map_err(|e| Error::Backend(format!("scylla scan put if absent: {e}")))
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
                let cas_op = format!("scan_put_if_version:{}", table.as_str());
                self.telemetry.record_cas_attempt(&cas_op);
                let stmt = self.scannable_statements(table)?.put_if_version.clone();
                let res = self
                    .with_retry("scan_put_if_version", || async {
                        self.session
                            .execute_unpaged(
                                &stmt,
                                (
                                    value.to_vec(),
                                    (v + 1) as i64,
                                    partition_vec.clone(),
                                    clustering_vec.clone(),
                                    v as i64,
                                ),
                            )
                            .await
                            .map_err(|e| Error::Backend(format!("scylla scan put if version: {e}")))
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

    async fn scan_delete(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
        cond: DelCond,
    ) -> Result<()> {
        match cond {
            DelCond::Any => {
                let stmt = self.scannable_statements(table)?.delete_any.clone();
                self.with_retry("scan_delete_any", || async {
                    self.session
                        .execute_unpaged(&stmt, (partition.to_vec(), clustering.to_vec()))
                        .await
                        .map_err(|e| Error::Backend(format!("scylla scan delete: {e}")))?;
                    Ok(())
                })
                .await?;
            }
            DelCond::IfVersion(v) => {
                let stmt = self.scannable_statements(table)?.delete_if_version.clone();
                let _ = self
                    .with_retry("scan_delete_if_version", || async {
                        self.session
                            .execute_unpaged(
                                &stmt,
                                (partition.to_vec(), clustering.to_vec(), v as i64),
                            )
                            .await
                            .map_err(|e| {
                                Error::Backend(format!("scylla scan delete if version: {e}"))
                            })
                    })
                    .await?;
            }
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
        let mut keys = Vec::new();
        let has_cursor = cursor.is_some();
        let start = cursor.unwrap_or_default();
        let stmt = self.scannable_statements(table)?.list.clone();
        let res = self
            .with_retry("scan_list", || async {
                self.session
                    .execute_unpaged(&stmt, (partition.to_vec(),))
                    .await
                    .map_err(|e| Error::Backend(format!("scylla scan list: {e}")))
            })
            .await?;

        if let Ok(rows_result) = res.into_rows_result()
            && let Ok(iter) = rows_result.rows::<(Vec<u8>,)>()
        {
            for row in iter {
                let (k,) = row.map_err(|e| Error::Backend(format!("decode row: {e}")))?;
                if (has_cursor && k <= start) || (!has_cursor && k < start) {
                    continue;
                }
                if !k.starts_with(prefix) {
                    continue;
                }
                keys.push(k);
                if keys.len() >= limit {
                    break;
                }
            }
        }

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
