use log_workload_gen::runtime::bounded_queue::bounded;

#[tokio::test]
async fn bounded_queue_tracks_depth_and_max_depth() {
    let (tx, mut rx, depth) = bounded::<u64>(2).expect("bounded queue");

    tx.send(1).await.expect("send 1");
    tx.send(2).await.expect("send 2");

    assert_eq!(depth.current(), 2);
    assert_eq!(depth.max(), 2);

    let a = rx.recv().await;
    let b = rx.recv().await;
    assert_eq!(a, Some(1));
    assert_eq!(b, Some(2));

    assert_eq!(depth.current(), 0);
    assert_eq!(depth.max(), 2);
}

#[tokio::test]
async fn bounded_queue_rejects_zero_capacity() {
    let err = bounded::<u8>(0).err().expect("capacity 0 must fail");
    assert!(err.to_string().contains("capacity"));
}
