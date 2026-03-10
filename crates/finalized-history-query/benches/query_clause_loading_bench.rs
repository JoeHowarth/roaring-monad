mod common;

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use finalized_history_query::logs::query::load_clause_sets_for_benchmark;
use finalized_history_query::store::blob::InMemoryBlobStore;
use finalized_history_query::store::meta::InMemoryMetaStore;
use futures::executor::block_on;

use crate::common::{
    HIGH_SHARD, dense_bitmap, log_id, low_shards, patterned_bitmap, seed_stream_state,
    stream_for_address, wide_or_filter,
};

fn bench_storage_shapes(c: &mut Criterion) {
    let mut group = c.benchmark_group("clause_loading_storage_shape");
    let sparse_chunk = patterned_bitmap(5, 97, 4_096);
    let dense_chunk = dense_bitmap(0, 65_536);
    let tail_entries = dense_bitmap(70_000, 8_192);

    for (name, manifest_chunks, tail, shards, from_local, to_local) in [
        (
            "manifest_only_one_shard",
            vec![sparse_chunk.clone()],
            None,
            vec![0],
            0,
            131_071,
        ),
        (
            "tail_only_many_shards",
            Vec::new(),
            Some(tail_entries.clone()),
            low_shards(8),
            0,
            131_071,
        ),
        (
            "manifest_plus_tail_many_shards_narrow",
            vec![dense_chunk],
            Some(tail_entries),
            low_shards(8),
            60_000,
            76_000,
        ),
        (
            "high_shard_manifest_only",
            vec![sparse_chunk],
            None,
            vec![HIGH_SHARD],
            0,
            131_071,
        ),
    ] {
        let meta = InMemoryMetaStore::default();
        let blob = InMemoryBlobStore::default();
        let address = [0; 20];
        for shard in &shards {
            seed_stream_state(
                &meta,
                &blob,
                &stream_for_address(address, *shard),
                &manifest_chunks,
                tail.as_ref(),
            );
        }
        let filter = wide_or_filter(1);
        let from = log_id(*shards.first().expect("non-empty shards"), from_local);
        let to = log_id(*shards.last().expect("non-empty shards"), to_local);

        group.bench_with_input(BenchmarkId::from_parameter(name), &name, |b, _| {
            b.iter(|| {
                block_on(load_clause_sets_for_benchmark(
                    &meta,
                    &blob,
                    black_box(&filter),
                    black_box(from),
                    black_box(to),
                ))
                .expect("load clause sets")
            })
        });
    }

    group.finish();
}

fn bench_or_widths(c: &mut Criterion) {
    let mut group = c.benchmark_group("clause_loading_or_width");

    for width in [1usize, 4, 16, 64, 256] {
        let meta = InMemoryMetaStore::default();
        let blob = InMemoryBlobStore::default();
        let shards = low_shards(8);
        for value in 0..width {
            let address = [(value % 64) as u8; 20];
            for shard in &shards {
                seed_stream_state(
                    &meta,
                    &blob,
                    &stream_for_address(address, *shard),
                    &[patterned_bitmap(value as u32, 113, 2_048)],
                    Some(&dense_bitmap(40_000, 2_048)),
                );
            }
        }
        let filter = wide_or_filter(width);
        let from = log_id(0, 0);
        let to = log_id(7, 131_071);

        group.bench_with_input(BenchmarkId::from_parameter(width), &width, |b, _| {
            b.iter(|| {
                block_on(load_clause_sets_for_benchmark(
                    &meta,
                    &blob,
                    black_box(&filter),
                    black_box(from),
                    black_box(to),
                ))
                .expect("load OR clause sets")
            })
        });
    }

    group.finish();
}

fn bench_sparse_vs_dense_chunks(c: &mut Criterion) {
    let mut group = c.benchmark_group("clause_loading_density");
    let shards = low_shards(8);

    for (name, chunks) in [
        ("sparse_chunks", vec![patterned_bitmap(0, 257, 1_024)]),
        ("dense_chunks", vec![dense_bitmap(0, 131_072)]),
    ] {
        let meta = InMemoryMetaStore::default();
        let blob = InMemoryBlobStore::default();
        let address = [0; 20];
        for shard in &shards {
            seed_stream_state(
                &meta,
                &blob,
                &stream_for_address(address, *shard),
                &chunks,
                None,
            );
        }
        let filter = wide_or_filter(1);
        let from = log_id(0, 0);
        let to = log_id(7, 131_071);

        group.bench_with_input(BenchmarkId::from_parameter(name), &name, |b, _| {
            b.iter(|| {
                block_on(load_clause_sets_for_benchmark(
                    &meta,
                    &blob,
                    black_box(&filter),
                    black_box(from),
                    black_box(to),
                ))
                .expect("load dense or sparse clause sets")
            })
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_storage_shapes,
    bench_or_widths,
    bench_sparse_vs_dense_chunks
);
criterion_main!(benches);
