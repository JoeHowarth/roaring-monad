mod common;

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};

use crate::common::{
    build_service, intersection_filter, narrow_indexed_filter, pagination_filter, query_len,
    query_page, seed_service_blocks, wide_or_filter,
};

fn bench_narrow_indexed_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_end_to_end_narrow");
    let svc = build_service(64);
    seed_service_blocks(&svc, 200, 100);

    group.bench_function("address_and_topics", |b| {
        let filter = narrow_indexed_filter();
        b.iter(|| black_box(query_len(&svc, 50, 200, black_box(filter.clone()), 1_000)))
    });

    group.finish();
}

fn bench_intersections_and_or_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_end_to_end_indexed_mix");
    let svc = build_service(256);
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
    let svc = build_service(128);
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
                resume_log_id = page.meta.next_resume_log_id;
            }
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_narrow_indexed_queries,
    bench_intersections_and_or_queries,
    bench_pagination_heavy_queries
);
criterion_main!(benches);
