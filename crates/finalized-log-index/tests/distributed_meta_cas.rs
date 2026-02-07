#![cfg(feature = "distributed-stores")]

use finalized_log_index::store::scylla::ScyllaMetaStore;
use finalized_log_index::store::traits::{FenceToken, MetaStore, PutCond};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lwt_if_absent_has_single_winner() {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    let keyspace = format!("finalized_index_lwt_absent_{}", stamp);

    let store = ScyllaMetaStore::new(&["127.0.0.1:9042".to_string()], &keyspace)
        .await
        .expect("connect scylla");
    store.set_min_epoch(1).await.expect("set min epoch");

    let key = b"meta/cas_race".to_vec();
    let s1 = store.clone();
    let s2 = store.clone();

    let t1 = tokio::spawn(async move {
        s1.put(
            &key,
            bytes::Bytes::from_static(b"writer1"),
            PutCond::IfAbsent,
            FenceToken(1),
        )
        .await
        .expect("put1")
        .applied
    });

    let key2 = b"meta/cas_race".to_vec();
    let t2 = tokio::spawn(async move {
        s2.put(
            &key2,
            bytes::Bytes::from_static(b"writer2"),
            PutCond::IfAbsent,
            FenceToken(1),
        )
        .await
        .expect("put2")
        .applied
    });

    let a = t1.await.expect("join1");
    let b = t2.await.expect("join2");
    assert_ne!(a, b, "exactly one writer must win IF NOT EXISTS");

    let rec = store
        .get(b"meta/cas_race")
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
    let keyspace = format!("finalized_index_lwt_ver_{}", stamp);

    let store = ScyllaMetaStore::new(&["127.0.0.1:9042".to_string()], &keyspace)
        .await
        .expect("connect scylla");
    store.set_min_epoch(1).await.expect("set min epoch");

    let seed = store
        .put(
            b"meta/ver_race",
            bytes::Bytes::from_static(b"seed"),
            PutCond::IfAbsent,
            FenceToken(1),
        )
        .await
        .expect("seed put");
    assert!(seed.applied);
    let expected_version = seed.version.expect("seed version");

    let s1 = store.clone();
    let s2 = store.clone();

    let t1 = tokio::spawn(async move {
        s1.put(
            b"meta/ver_race",
            bytes::Bytes::from_static(b"writer1"),
            PutCond::IfVersion(expected_version),
            FenceToken(1),
        )
        .await
        .expect("put1")
        .applied
    });

    let t2 = tokio::spawn(async move {
        s2.put(
            b"meta/ver_race",
            bytes::Bytes::from_static(b"writer2"),
            PutCond::IfVersion(expected_version),
            FenceToken(1),
        )
        .await
        .expect("put2")
        .applied
    });

    let a = t1.await.expect("join1");
    let b = t2.await.expect("join2");
    assert_ne!(a, b, "exactly one writer must win IF version=expected");

    let rec = store
        .get(b"meta/ver_race")
        .await
        .expect("get")
        .expect("row");
    let v = String::from_utf8(rec.value.to_vec()).expect("utf8");
    assert!(v == "writer1" || v == "writer2");
}
