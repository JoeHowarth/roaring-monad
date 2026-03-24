mod common;

use criterion::{BatchSize, BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use finalized_history_query::query::runner::QueryMaterializer;
use finalized_history_query::store::blob::InMemoryBlobStore;
use finalized_history_query::store::meta::InMemoryMetaStore;
use futures::executor::block_on;

use crate::common::{
    HIGH_SHARD, SeededLogBlock, high_shard_blocks, log_id, materializer, mk_log,
    seed_materialized_blocks, spanning_bucket_blocks,
};

fn bench_cache_shapes(c: &mut Criterion) {
    let mut group = c.benchmark_group("materialize_cache_shape");
    let meta = InMemoryMetaStore::default();
    let blob = InMemoryBlobStore::default();
    let blocks = vec![
        SeededLogBlock {
            block_num: 100,
            first_log_id: 0,
            logs: (0..8).map(|idx| mk_log(1, 1, 1, 100, 0, idx)).collect(),
        },
        SeededLogBlock {
            block_num: 101,
            first_log_id: 8,
            logs: (0..8).map(|idx| mk_log(2, 2, 2, 101, 0, idx)).collect(),
        },
    ];
    seed_materialized_blocks(&meta, &blob, &blocks);

    let target_id = log_id(0, 10);
    let same_bucket_other_block_id = log_id(0, 2);

    group.bench_function("cold_cache", |b| {
        b.iter_batched(
            || materializer(&meta, &blob),
            |mut materializer| {
                block_on(QueryMaterializer::load_by_id(
                    &mut materializer,
                    black_box(target_id),
                ))
                .expect("load cold log")
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("warm_bucket_cold_header", |b| {
        let mut materializer = materializer(&meta, &blob);
        block_on(QueryMaterializer::load_by_id(
            &mut materializer,
            same_bucket_other_block_id,
        ))
        .expect("prime same bucket");
        b.iter(|| {
            block_on(QueryMaterializer::load_by_id(
                &mut materializer,
                black_box(target_id),
            ))
            .expect("load with warm bucket")
        })
    });

    group.bench_function("warm_bucket_warm_header", |b| {
        let mut materializer = materializer(&meta, &blob);
        block_on(QueryMaterializer::load_by_id(&mut materializer, target_id))
            .expect("prime same block");
        b.iter(|| {
            block_on(QueryMaterializer::load_by_id(
                &mut materializer,
                black_box(target_id),
            ))
            .expect("load with warm caches")
        })
    });

    group.finish();
}

fn bench_locality(c: &mut Criterion) {
    let mut group = c.benchmark_group("materialize_locality");
    let meta = InMemoryMetaStore::default();
    let blob = InMemoryBlobStore::default();
    let blocks = (0..32u64)
        .map(|block_offset| SeededLogBlock {
            block_num: 500 + block_offset,
            first_log_id: block_offset * 8,
            logs: (0..8)
                .map(|idx| mk_log(block_offset as u8, 3, 4, 500 + block_offset, 0, idx))
                .collect(),
        })
        .collect::<Vec<_>>();
    seed_materialized_blocks(&meta, &blob, &blocks);

    let same_block_ids = (0..8).map(|local| log_id(0, local)).collect::<Vec<_>>();
    let cross_block_ids = (0..32)
        .map(|block_offset| log_id(0, (block_offset * 8) as u32))
        .collect::<Vec<_>>();

    for (name, ids) in [
        ("same_block_warm", same_block_ids),
        ("cross_block_warm", cross_block_ids),
    ] {
        group.bench_with_input(BenchmarkId::from_parameter(name), &ids, |b, ids| {
            let mut materializer = materializer(&meta, &blob);
            b.iter(|| {
                for &id in ids {
                    block_on(QueryMaterializer::load_by_id(
                        &mut materializer,
                        black_box(id),
                    ))
                    .expect("materialize warm locality")
                    .expect("seeded id must exist");
                }
            })
        });
    }

    group.finish();
}

fn bench_bucket_boundaries_and_high_shard(c: &mut Criterion) {
    let mut group = c.benchmark_group("materialize_boundary_cases");

    let spanning_meta = InMemoryMetaStore::default();
    let spanning_blob = InMemoryBlobStore::default();
    let spanning_blocks = spanning_bucket_blocks();
    seed_materialized_blocks(&spanning_meta, &spanning_blob, &spanning_blocks);
    let spanning_target =
        finalized_history_query::core::ids::LogId::new(spanning_blocks[0].first_log_id + 7);

    group.bench_function("block_spans_multiple_directory_buckets_warm", |b| {
        let mut materializer = materializer(&spanning_meta, &spanning_blob);
        block_on(QueryMaterializer::load_by_id(
            &mut materializer,
            spanning_target,
        ))
        .expect("prime spanning block");
        b.iter(|| {
            block_on(QueryMaterializer::load_by_id(
                &mut materializer,
                black_box(spanning_target),
            ))
            .expect("load spanning block")
        })
    });

    let high_meta = InMemoryMetaStore::default();
    let high_blob = InMemoryBlobStore::default();
    seed_materialized_blocks(&high_meta, &high_blob, &high_shard_blocks());
    let high_target = log_id(HIGH_SHARD, (1 << 24) - 24);

    group.bench_function("high_shard_cold", |b| {
        b.iter_batched(
            || materializer(&high_meta, &high_blob),
            |mut materializer| {
                block_on(QueryMaterializer::load_by_id(
                    &mut materializer,
                    black_box(high_target),
                ))
                .expect("load high-shard log")
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_cache_shapes,
    bench_locality,
    bench_bucket_boundaries_and_high_shard
);
criterion_main!(benches);
