#![cfg(feature = "distributed-stores")]

use finalized_history_query::core::state::BLOCK_RECORD_TABLE;
use finalized_history_query::store::scylla::ScyllaMetaStore;
use finalized_history_query::store::traits::{MetaStore, PutCond};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lwt_if_absent_has_single_winner() {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    let keyspace = format!("finalized_history_query_lwt_absent_{}", stamp);

    let store = ScyllaMetaStore::new(&["127.0.0.1:9042".to_string()], &keyspace)
        .await
        .expect("connect scylla");
    let key = b"cas_race".to_vec();
    let s1 = store.clone();
    let s2 = store.clone();

    let t1 = tokio::spawn(async move {
        s1.put(
            BLOCK_RECORD_TABLE,
            &key,
            bytes::Bytes::from_static(b"writer1"),
            PutCond::IfAbsent,
        )
        .await
        .expect("put1")
        .applied
    });

    let key2 = b"cas_race".to_vec();
    let t2 = tokio::spawn(async move {
        s2.put(
            BLOCK_RECORD_TABLE,
            &key2,
            bytes::Bytes::from_static(b"writer2"),
            PutCond::IfAbsent,
        )
        .await
        .expect("put2")
        .applied
    });

    let a = t1.await.expect("join1");
    let b = t2.await.expect("join2");
    assert_ne!(a, b, "exactly one writer must win IF NOT EXISTS");

    let rec = store
        .get(BLOCK_RECORD_TABLE, b"cas_race")
        .await
        .expect("get")
        .expect("row");
    let v = String::from_utf8(rec.value.to_vec()).expect("utf8");
    assert!(v == "writer1" || v == "writer2");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lwt_if_version_has_single_winner() {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    let keyspace = format!("finalized_history_query_lwt_ver_{}", stamp);

    let store = ScyllaMetaStore::new(&["127.0.0.1:9042".to_string()], &keyspace)
        .await
        .expect("connect scylla");
    let seed = store
        .put(
            BLOCK_RECORD_TABLE,
            b"ver_race",
            bytes::Bytes::from_static(b"seed"),
            PutCond::IfAbsent,
        )
        .await
        .expect("seed put");
    assert!(seed.applied);
    let expected_version = seed.version.expect("seed version");

    let s1 = store.clone();
    let s2 = store.clone();

    let t1 = tokio::spawn(async move {
        s1.put(
            BLOCK_RECORD_TABLE,
            b"ver_race",
            bytes::Bytes::from_static(b"writer1"),
            PutCond::IfVersion(expected_version),
        )
        .await
        .expect("put1")
        .applied
    });

    let t2 = tokio::spawn(async move {
        s2.put(
            BLOCK_RECORD_TABLE,
            b"ver_race",
            bytes::Bytes::from_static(b"writer2"),
            PutCond::IfVersion(expected_version),
        )
        .await
        .expect("put2")
        .applied
    });

    let a = t1.await.expect("join1");
    let b = t2.await.expect("join2");
    assert_ne!(a, b, "exactly one writer must win IF version=expected");

    let rec = store
        .get(BLOCK_RECORD_TABLE, b"ver_race")
        .await
        .expect("get")
        .expect("row");
    let v = String::from_utf8(rec.value.to_vec()).expect("utf8");
    assert!(v == "writer1" || v == "writer2");
}
