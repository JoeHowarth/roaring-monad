use criterion::{BatchSize, BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use finalized_log_index::api::{FinalizedIndexService, FinalizedLogIndex};
use finalized_log_index::config::Config;
use finalized_log_index::domain::filter::{Clause, LogFilter, QueryOptions};
use finalized_log_index::domain::types::{Block, Log};
use finalized_log_index::store::blob::InMemoryBlobStore;
use finalized_log_index::store::meta::InMemoryMetaStore;
use futures::executor::block_on;

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

fn build_service(
    target_entries_per_chunk: u32,
) -> FinalizedIndexService<InMemoryMetaStore, InMemoryBlobStore> {
    FinalizedIndexService::new(
        Config {
            target_entries_per_chunk,
            planner_max_or_terms: 256,
            ..Config::default()
        },
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        1,
    )
}

fn seed_blocks(
    svc: &FinalizedIndexService<InMemoryMetaStore, InMemoryBlobStore>,
    blocks: u64,
    logs_per_block: u32,
) {
    block_on(async {
        let mut parent = [0u8; 32];
        for b in 1..=blocks {
            let mut logs = Vec::with_capacity(logs_per_block as usize);
            for i in 0..logs_per_block {
                let addr = (i % 32) as u8;
                let topic0 = (i % 8) as u8;
                let topic1 = (i % 64) as u8;
                logs.push(mk_log(addr, topic0, topic1, b, 0, i));
            }
            let block = mk_block(b, parent, logs);
            parent = block.block_hash;
            svc.ingest_finalized_block(block)
                .await
                .expect("seed ingest");
        }
    });
}

fn bench_ingest(c: &mut Criterion) {
    let mut group = c.benchmark_group("ingest");
    for logs_per_block in [10u32, 100u32, 1_000u32] {
        group.bench_with_input(
            BenchmarkId::from_parameter(logs_per_block),
            &logs_per_block,
            |b, &lpb| {
                b.iter_batched(
                    || build_service(64),
                    |svc| {
                        block_on(async {
                            let mut parent = [0u8; 32];
                            for n in 1..=20u64 {
                                let mut logs = Vec::with_capacity(lpb as usize);
                                for i in 0..lpb {
                                    logs.push(mk_log(
                                        (i % 16) as u8,
                                        (i % 8) as u8,
                                        (i % 32) as u8,
                                        n,
                                        0,
                                        i,
                                    ));
                                }
                                let block = mk_block(n, parent, logs);
                                parent = block.block_hash;
                                svc.ingest_finalized_block(block).await.expect("ingest");
                            }
                        });
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }
    group.finish();
}

fn bench_query_filtered(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_filtered");
    let svc = build_service(64);
    seed_blocks(&svc, 200, 100);

    let filter = LogFilter {
        from_block: Some(50),
        to_block: Some(200),
        block_hash: None,
        address: Some(Clause::One([5; 20])),
        topic0: Some(Clause::One([1; 32])),
        topic1: Some(Clause::One([5; 32])),
        topic2: None,
        topic3: None,
    };

    group.bench_function("addr_topic_filtered", |b| {
        b.iter(|| {
            let res = block_on(svc.query_finalized(
                black_box(filter.clone()),
                QueryOptions {
                    max_results: Some(1_000),
                },
            ))
            .expect("query");
            black_box(res.len())
        })
    });

    group.finish();
}

fn bench_query_or_list(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_or_list");
    let svc = build_service(256);
    seed_blocks(&svc, 200, 100);

    for n in [4usize, 32usize, 128usize] {
        let addresses: Vec<[u8; 20]> = (0..n).map(|i| [(i as u8) % 64; 20]).collect();
        let filter = LogFilter {
            from_block: Some(1),
            to_block: Some(200),
            block_hash: None,
            address: Some(Clause::Or(addresses)),
            topic0: None,
            topic1: None,
            topic2: None,
            topic3: None,
        };
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let res = block_on(svc.query_finalized(
                    black_box(filter.clone()),
                    QueryOptions {
                        max_results: Some(10_000),
                    },
                ))
                .expect("query");
                black_box(res.len())
            })
        });
    }

    group.finish();
}

fn bench_query_mixed_large(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_mixed_large");
    let svc = build_service(512);
    seed_blocks(&svc, 1_000, 200);

    let filters = [
        LogFilter {
            from_block: Some(300),
            to_block: Some(1_000),
            block_hash: None,
            address: Some(Clause::One([11; 20])),
            topic0: Some(Clause::One([3; 32])),
            topic1: None,
            topic2: None,
            topic3: None,
        },
        LogFilter {
            from_block: Some(200),
            to_block: Some(1_000),
            block_hash: None,
            address: None,
            topic0: Some(Clause::One([2; 32])),
            topic1: Some(Clause::One([2; 32])),
            topic2: None,
            topic3: None,
        },
        LogFilter {
            from_block: Some(1),
            to_block: Some(1_000),
            block_hash: None,
            address: Some(Clause::Or(
                (0..48usize).map(|i| [(i % 64) as u8; 20]).collect(),
            )),
            topic0: None,
            topic1: None,
            topic2: None,
            topic3: None,
        },
        LogFilter {
            from_block: Some(800),
            to_block: Some(1_000),
            block_hash: None,
            address: None,
            topic0: None,
            topic1: None,
            topic2: None,
            topic3: None,
        },
    ];

    group.bench_function("mixed_workload_4way", |b| {
        let mut idx = 0usize;
        b.iter(|| {
            let filter = filters[idx % filters.len()].clone();
            idx = idx.wrapping_add(1);
            let res = block_on(svc.query_finalized(
                black_box(filter),
                QueryOptions {
                    max_results: Some(20_000),
                },
            ))
            .expect("query");
            black_box(res.len())
        })
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_ingest,
    bench_query_filtered,
    bench_query_or_list,
    bench_query_mixed_large
);
criterion_main!(benches);
