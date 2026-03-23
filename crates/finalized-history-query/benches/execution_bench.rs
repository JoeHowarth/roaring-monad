mod common;

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use finalized_history_query::logs::query::execution::execute_candidates;
use futures::executor::block_on;

use crate::common::{
    HIGH_SHARD, PassThroughMaterializer, dense_bitmap, low_shards, patterned_bitmap, primary_range,
    shard_bitmap_set,
};

fn bench_single_clause_iteration(c: &mut Criterion) {
    let mut group = c.benchmark_group("execution_single_clause");
    let bitmap = patterned_bitmap(5, 97, 4_096);

    for (name, shards) in [
        ("one_shard", vec![0]),
        ("eight_shards", low_shards(8)),
        ("sixty_four_shards", low_shards(64)),
        ("high_shard", vec![HIGH_SHARD]),
    ] {
        let clause_sets = vec![shard_bitmap_set(&shards, &bitmap)];
        let id_range = primary_range(
            *shards.first().expect("non-empty shards"),
            0,
            *shards.last().expect("non-empty shards"),
            bitmap.max().unwrap_or(0).saturating_add(1),
        );

        group.bench_with_input(BenchmarkId::from_parameter(name), &name, |b, _| {
            let mut materializer = PassThroughMaterializer::new(1_024);
            b.iter(|| {
                block_on(execute_candidates(
                    black_box(clause_sets.clone()),
                    black_box(id_range),
                    &(),
                    &mut materializer,
                    usize::MAX,
                ))
                .expect("execute candidates")
            })
        });
    }

    group.finish();
}

fn bench_intersections(c: &mut Criterion) {
    let mut group = c.benchmark_group("execution_intersection");

    let sparse_a = shard_bitmap_set(&low_shards(8), &patterned_bitmap(10, 127, 2_048));
    let sparse_b = shard_bitmap_set(&low_shards(8), &patterned_bitmap(10, 254, 1_024));
    let dense_a = shard_bitmap_set(&low_shards(8), &dense_bitmap(0, 32_768));
    let dense_b = shard_bitmap_set(&low_shards(8), &dense_bitmap(16_384, 32_768));
    let union_like = shard_bitmap_set(&low_shards(64), &dense_bitmap(0, 8_192));
    let id_range = primary_range(0, 0, 63, 65_535);

    for (name, clause_sets) in [
        ("sparse", vec![sparse_a, sparse_b]),
        ("dense", vec![dense_a, dense_b]),
        (
            "union_like_many_shards",
            vec![union_like.clone(), union_like],
        ),
    ] {
        group.bench_with_input(BenchmarkId::from_parameter(name), &name, |b, _| {
            let mut materializer = PassThroughMaterializer::new(2_048);
            b.iter(|| {
                block_on(execute_candidates(
                    black_box(clause_sets.clone()),
                    black_box(id_range),
                    &(),
                    &mut materializer,
                    usize::MAX,
                ))
                .expect("execute candidates")
            })
        });
    }

    group.finish();
}

fn bench_empty_clause_and_clipping(c: &mut Criterion) {
    let mut group = c.benchmark_group("execution_clip_and_empty");

    group.bench_function("empty_clause_set_full_range", |b| {
        let mut materializer = PassThroughMaterializer::new(512);
        let id_range = primary_range(0, 0, 0, 32_767);
        b.iter(|| {
            block_on(execute_candidates(
                black_box(Vec::new()),
                black_box(id_range),
                &(),
                &mut materializer,
                8_192,
            ))
            .expect("execute empty clause set")
        })
    });

    let clipped_set = vec![shard_bitmap_set(&[0, 1, 2], &dense_bitmap(0, 65_535))];
    for (name, id_range) in [
        ("first_edge", primary_range(0, 32_768, 2, MAX_LOCAL)),
        ("last_edge", primary_range(0, 0, 2, 32_768)),
        ("both_edges", primary_range(0, 32_768, 2, 32_768)),
    ] {
        group.bench_with_input(BenchmarkId::from_parameter(name), &name, |b, _| {
            let mut materializer = PassThroughMaterializer::new(2_048);
            b.iter(|| {
                block_on(execute_candidates(
                    black_box(clipped_set.clone()),
                    black_box(id_range),
                    &(),
                    &mut materializer,
                    usize::MAX,
                ))
                .expect("execute clipped candidates")
            })
        });
    }

    group.finish();
}

const MAX_LOCAL: u32 = (1 << 24) - 1;

criterion_group!(
    benches,
    bench_single_clause_iteration,
    bench_intersections,
    bench_empty_clause_and_clipping
);
criterion_main!(benches);
