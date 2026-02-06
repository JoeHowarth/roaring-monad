use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use finalized_log_index::api::{FinalizedIndexService, FinalizedLogIndex};
use finalized_log_index::config::Config;
use finalized_log_index::domain::filter::{LogFilter, QueryOptions};
use finalized_log_index::domain::types::{Block, Log};
use finalized_log_index::error::{Error, Result};
use finalized_log_index::store::blob::InMemoryBlobStore;
use finalized_log_index::store::meta::InMemoryMetaStore;
use finalized_log_index::store::traits::{
    BlobStore, DelCond, FenceToken, MetaStore, Page, PutCond, PutResult, Record,
};
use futures::executor::block_on;

#[derive(Clone, Copy)]
enum FaultOp {
    MetaPut,
    BlobPut,
}

#[derive(Clone)]
struct FaultPlan {
    op: FaultOp,
    prefix: Vec<u8>,
    fail_on_match: usize,
    seen_matches: usize,
    armed: bool,
}

#[derive(Default)]
struct FaultInjector {
    plan: Mutex<Option<FaultPlan>>,
}

impl FaultInjector {
    fn arm(&self, op: FaultOp, prefix: &[u8], fail_on_match: usize) {
        let mut guard = self.plan.lock().expect("injector lock");
        *guard = Some(FaultPlan {
            op,
            prefix: prefix.to_vec(),
            fail_on_match,
            seen_matches: 0,
            armed: true,
        });
    }

    fn clear(&self) {
        let mut guard = self.plan.lock().expect("injector lock");
        *guard = None;
    }

    fn maybe_fail(&self, op: FaultOp, key: &[u8]) -> Result<()> {
        let mut guard = self.plan.lock().expect("injector lock");
        let Some(plan) = guard.as_mut() else {
            return Ok(());
        };
        if !plan.armed || !matches_op(plan.op, op) || !key.starts_with(&plan.prefix) {
            return Ok(());
        }

        plan.seen_matches = plan.seen_matches.saturating_add(1);
        if plan.seen_matches == plan.fail_on_match {
            plan.armed = false;
            return Err(Error::Backend("injected crash fault".to_string()));
        }
        Ok(())
    }
}

fn matches_op(a: FaultOp, b: FaultOp) -> bool {
    matches!(
        (a, b),
        (FaultOp::MetaPut, FaultOp::MetaPut) | (FaultOp::BlobPut, FaultOp::BlobPut)
    )
}

#[derive(Clone)]
struct FaultyMetaStore {
    inner: Arc<InMemoryMetaStore>,
    injector: Arc<FaultInjector>,
}

#[async_trait]
impl MetaStore for FaultyMetaStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Record>> {
        self.inner.get(key).await
    }

    async fn put(
        &self,
        key: &[u8],
        value: Bytes,
        cond: PutCond,
        fence: FenceToken,
    ) -> Result<PutResult> {
        self.injector.maybe_fail(FaultOp::MetaPut, key)?;
        self.inner.put(key, value, cond, fence).await
    }

    async fn delete(&self, key: &[u8], cond: DelCond, fence: FenceToken) -> Result<()> {
        self.inner.delete(key, cond, fence).await
    }

    async fn list_prefix(
        &self,
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page> {
        self.inner.list_prefix(prefix, cursor, limit).await
    }
}

#[derive(Clone)]
struct FaultyBlobStore {
    inner: Arc<InMemoryBlobStore>,
    injector: Arc<FaultInjector>,
}

#[async_trait]
impl BlobStore for FaultyBlobStore {
    async fn put_blob(&self, key: &[u8], value: Bytes) -> Result<()> {
        self.injector.maybe_fail(FaultOp::BlobPut, key)?;
        self.inner.put_blob(key, value).await
    }

    async fn get_blob(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get_blob(key).await
    }

    async fn delete_blob(&self, key: &[u8]) -> Result<()> {
        self.inner.delete_blob(key).await
    }

    async fn list_prefix(
        &self,
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page> {
        self.inner.list_prefix(prefix, cursor, limit).await
    }
}

fn mk_log(address: u8, topic0: u8, topic1: u8, block_num: u64, tx_idx: u32, log_idx: u32) -> Log {
    Log {
        address: [address; 20],
        topics: vec![[topic0; 32], [topic1; 32]],
        data: vec![address, topic0, topic1],
        block_num,
        tx_idx,
        log_idx,
        block_hash: [block_num as u8; 32],
    }
}

fn mk_block(block_num: u64, parent_hash: [u8; 32], logs: Vec<Log>) -> Block {
    Block {
        block_num,
        block_hash: [block_num as u8; 32],
        parent_hash,
        logs,
    }
}

fn mk_service(
    meta: Arc<InMemoryMetaStore>,
    blob: Arc<InMemoryBlobStore>,
    injector: Arc<FaultInjector>,
) -> FinalizedIndexService<FaultyMetaStore, FaultyBlobStore> {
    FinalizedIndexService::new(
        Config {
            target_entries_per_chunk: 1,
            target_chunk_bytes: 1,
            planner_max_or_terms: 64,
            ..Config::default()
        },
        FaultyMetaStore {
            inner: meta,
            injector: injector.clone(),
        },
        FaultyBlobStore {
            inner: blob,
            injector,
        },
        1,
    )
}

#[test]
fn ingest_retry_survives_faults_at_phase_boundaries() {
    block_on(async {
        let cases = vec![
            ("logs_put", FaultOp::MetaPut, b"logs/".as_slice(), 1usize),
            (
                "block_meta_put",
                FaultOp::MetaPut,
                b"block_meta/".as_slice(),
                1usize,
            ),
            (
                "block_hash_to_num_put",
                FaultOp::MetaPut,
                b"block_hash_to_num/".as_slice(),
                1usize,
            ),
            (
                "manifest_put",
                FaultOp::MetaPut,
                b"manifests/".as_slice(),
                1usize,
            ),
            ("tail_put", FaultOp::MetaPut, b"tails/".as_slice(), 1usize),
            (
                "state_cas_put",
                FaultOp::MetaPut,
                b"meta/state".as_slice(),
                1usize,
            ),
            (
                "chunk_blob_put",
                FaultOp::BlobPut,
                b"chunks/".as_slice(),
                1usize,
            ),
        ];

        for (name, op, prefix, fail_on_match) in cases {
            let meta = Arc::new(InMemoryMetaStore::default());
            let blob = Arc::new(InMemoryBlobStore::default());
            let injector = Arc::new(FaultInjector::default());

            let b1 = mk_block(
                1,
                [0; 32],
                vec![mk_log(1, 10, 20, 1, 0, 0), mk_log(2, 11, 21, 1, 0, 1)],
            );
            let b2 = mk_block(
                2,
                b1.block_hash,
                vec![mk_log(1, 10, 22, 2, 0, 0), mk_log(3, 12, 23, 2, 0, 1)],
            );

            let svc = mk_service(meta.clone(), blob.clone(), injector.clone());
            injector.arm(op, prefix, fail_on_match);
            let err = svc
                .ingest_finalized_block(b1.clone())
                .await
                .expect_err("fault should fire");
            assert!(matches!(err, Error::Backend(_)), "case={name}");

            injector.clear();
            let svc = mk_service(meta.clone(), blob.clone(), injector.clone());
            svc.ingest_finalized_block(b1.clone())
                .await
                .expect("retry b1 succeeds");
            svc.ingest_finalized_block(b2.clone())
                .await
                .expect("ingest b2 succeeds");

            assert_eq!(
                svc.indexed_finalized_head().await.expect("head"),
                2,
                "case={name}"
            );

            let logs = svc
                .query_finalized(
                    LogFilter {
                        from_block: Some(1),
                        to_block: Some(2),
                        block_hash: None,
                        address: None,
                        topic0: None,
                        topic1: None,
                        topic2: None,
                        topic3: None,
                    },
                    QueryOptions { max_results: None },
                )
                .await
                .expect("query logs");

            let mut seen = HashSet::new();
            for l in &logs {
                seen.insert((l.block_num, l.tx_idx, l.log_idx));
            }
            assert_eq!(logs.len(), 4, "case={name}");
            assert_eq!(seen.len(), 4, "case={name}");

            let log_page = meta
                .list_prefix(b"logs/", None, usize::MAX)
                .await
                .expect("list logs");
            assert_eq!(log_page.keys.len(), 4, "case={name}");
        }
    });
}

#[test]
fn crash_loop_eventually_commits_without_corrupting_state() {
    block_on(async {
        let meta = Arc::new(InMemoryMetaStore::default());
        let blob = Arc::new(InMemoryBlobStore::default());
        let injector = Arc::new(FaultInjector::default());

        let b1 = mk_block(
            1,
            [0; 32],
            vec![mk_log(7, 3, 1, 1, 0, 0), mk_log(8, 4, 2, 1, 0, 1)],
        );

        // Simulate repeated crashes across different write boundaries before a successful run.
        let staged_faults = vec![
            (FaultOp::MetaPut, b"logs/".as_slice(), 2usize),
            (FaultOp::MetaPut, b"manifests/".as_slice(), 1usize),
            (FaultOp::BlobPut, b"chunks/".as_slice(), 1usize),
            (FaultOp::MetaPut, b"meta/state".as_slice(), 1usize),
        ];

        for (op, prefix, fail_on_match) in staged_faults {
            injector.arm(op, prefix, fail_on_match);
            let svc = mk_service(meta.clone(), blob.clone(), injector.clone());
            let err = svc
                .ingest_finalized_block(b1.clone())
                .await
                .expect_err("fault must trigger");
            assert!(matches!(err, Error::Backend(_)));
            injector.clear();
        }

        let svc = mk_service(meta.clone(), blob.clone(), injector.clone());
        svc.ingest_finalized_block(b1.clone())
            .await
            .expect("eventual ingest should succeed");

        assert_eq!(svc.indexed_finalized_head().await.expect("head"), 1);

        let logs = svc
            .query_finalized(
                LogFilter {
                    from_block: Some(1),
                    to_block: Some(1),
                    block_hash: None,
                    address: None,
                    topic0: None,
                    topic1: None,
                    topic2: None,
                    topic3: None,
                },
                QueryOptions { max_results: None },
            )
            .await
            .expect("query");
        assert_eq!(logs.len(), 2);

        let log_page = meta
            .list_prefix(b"logs/", None, usize::MAX)
            .await
            .expect("list logs");
        assert_eq!(log_page.keys.len(), 2);
    });
}
