mod common;

use criterion::{BatchSize, BenchmarkId, Criterion, black_box, criterion_group, criterion_main};

use crate::common::{
    build_counting_service, build_service, contiguous_block_filter, intersection_filter,
    mixed_page_filter, narrow_indexed_filter, non_contiguous_block_filter, pagination_filter,
    query_len, query_page, seed_contiguous_block_fixture, seed_mixed_page_fixture,
    seed_non_contiguous_block_fixture, seed_service_blocks, seed_sparse_cross_block_fixture,
    sparse_cross_block_filter, wide_or_filter,
};

fn bench_narrow_indexed_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_end_to_end_narrow");
    let svc = build_service();
    seed_service_blocks(&svc, 200, 100);

    group.bench_function("address_and_topics", |b| {
        let filter = narrow_indexed_filter();
        b.iter(|| black_box(query_len(&svc, 50, 200, black_box(filter.clone()), 1_000)))
    });

    group.finish();
}

fn bench_intersections_and_or_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_end_to_end_indexed_mix");
    let svc = build_service();
    seed_service_blocks(&svc, 500, 200);

    group.bench_function("multi_clause_intersection", |b| {
        let filter = intersection_filter();
        b.iter(|| black_box(query_len(&svc, 150, 500, black_box(filter.clone()), 5_000)))
    });

    for width in [4usize, 16, 64, 256] {
        group.bench_with_input(BenchmarkId::from_parameter(width), &width, |b, &width| {
            let filter = wide_or_filter(width);
            b.iter(|| black_box(query_len(&svc, 1, 500, black_box(filter.clone()), 20_000)))
        });
    }

    group.finish();
}

fn bench_pagination_heavy_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_end_to_end_pagination");
    let svc = build_service();
    seed_service_blocks(&svc, 600, 128);

    group.bench_function("resume_token_walk", |b| {
        let filter = pagination_filter();
        b.iter(|| {
            let mut resume_log_id = None;
            let mut pages = 0usize;
            loop {
                let page = query_page(&svc, 1, 600, black_box(filter.clone()), 256, resume_log_id);
                pages = pages.saturating_add(1);
                if !page.meta.has_more || pages >= 8 {
                    break black_box(page.items.len());
                }
                resume_log_id = page.meta.next_resume_id;
            }
        })
    });

    group.finish();
}

fn bench_query_storage_patterns(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_end_to_end_storage_patterns");

    group.bench_with_input(
        BenchmarkId::new("sparse_cross_block", "warm"),
        &(),
        |b, _| {
            let (svc, counters) = build_counting_service();
            seed_sparse_cross_block_fixture(&svc, 64);
            let _ = query_page(&svc, 1, 64, sparse_cross_block_filter(), 128, None);
            b.iter(|| {
                counters.reset();
                let page = query_page(
                    &svc,
                    1,
                    64,
                    black_box(sparse_cross_block_filter()),
                    128,
                    None,
                );
                black_box((page.items.len(), counters.snapshot()))
            })
        },
    );

    group.bench_with_input(
        BenchmarkId::new("sparse_cross_block", "cold"),
        &(),
        |b, _| {
            b.iter_batched(
                || {
                    let (svc, counters) = build_counting_service();
                    seed_sparse_cross_block_fixture(&svc, 64);
                    (svc, counters)
                },
                |(svc, counters)| {
                    counters.reset();
                    let page = query_page(
                        &svc,
                        1,
                        64,
                        black_box(sparse_cross_block_filter()),
                        128,
                        None,
                    );
                    black_box((page.items.len(), counters.snapshot()))
                },
                BatchSize::SmallInput,
            )
        },
    );

    group.bench_with_input(
        BenchmarkId::new("same_block_contiguous", "warm"),
        &(),
        |b, _| {
            let (svc, counters) = build_counting_service();
            seed_contiguous_block_fixture(&svc, 1, 64);
            let _ = query_page(&svc, 1, 1, contiguous_block_filter(), 128, None);
            b.iter(|| {
                counters.reset();
                let page = query_page(&svc, 1, 1, black_box(contiguous_block_filter()), 128, None);
                black_box((page.items.len(), counters.snapshot()))
            })
        },
    );

    group.bench_with_input(
        BenchmarkId::new("same_block_contiguous", "cold"),
        &(),
        |b, _| {
            b.iter_batched(
                || {
                    let (svc, counters) = build_counting_service();
                    seed_contiguous_block_fixture(&svc, 1, 64);
                    (svc, counters)
                },
                |(svc, counters)| {
                    counters.reset();
                    let page =
                        query_page(&svc, 1, 1, black_box(contiguous_block_filter()), 128, None);
                    black_box((page.items.len(), counters.snapshot()))
                },
                BatchSize::SmallInput,
            )
        },
    );

    group.bench_with_input(
        BenchmarkId::new("same_block_non_contiguous", "warm"),
        &(),
        |b, _| {
            let (svc, counters) = build_counting_service();
            seed_non_contiguous_block_fixture(&svc, 1, 64);
            let _ = query_page(&svc, 1, 1, non_contiguous_block_filter(), 128, None);
            b.iter(|| {
                counters.reset();
                let page = query_page(
                    &svc,
                    1,
                    1,
                    black_box(non_contiguous_block_filter()),
                    128,
                    None,
                );
                black_box((page.items.len(), counters.snapshot()))
            })
        },
    );

    group.bench_with_input(
        BenchmarkId::new("same_block_non_contiguous", "cold"),
        &(),
        |b, _| {
            b.iter_batched(
                || {
                    let (svc, counters) = build_counting_service();
                    seed_non_contiguous_block_fixture(&svc, 1, 64);
                    (svc, counters)
                },
                |(svc, counters)| {
                    counters.reset();
                    let page = query_page(
                        &svc,
                        1,
                        1,
                        black_box(non_contiguous_block_filter()),
                        128,
                        None,
                    );
                    black_box((page.items.len(), counters.snapshot()))
                },
                BatchSize::SmallInput,
            )
        },
    );

    group.bench_with_input(BenchmarkId::new("mixed_page", "warm"), &(), |b, _| {
        let (svc, counters) = build_counting_service();
        seed_mixed_page_fixture(&svc);
        let _ = query_page(&svc, 1, 9, mixed_page_filter(), 128, None);
        b.iter(|| {
            counters.reset();
            let page = query_page(&svc, 1, 9, black_box(mixed_page_filter()), 128, None);
            black_box((page.items.len(), counters.snapshot()))
        })
    });

    group.bench_with_input(BenchmarkId::new("mixed_page", "cold"), &(), |b, _| {
        b.iter_batched(
            || {
                let (svc, counters) = build_counting_service();
                seed_mixed_page_fixture(&svc);
                (svc, counters)
            },
            |(svc, counters)| {
                counters.reset();
                let page = query_page(&svc, 1, 9, black_box(mixed_page_filter()), 128, None);
                black_box((page.items.len(), counters.snapshot()))
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_narrow_indexed_queries,
    bench_intersections_and_or_queries,
    bench_pagination_heavy_queries,
    bench_query_storage_patterns
);
criterion_main!(benches);
