use std::env;
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use finalized_log_index::api::{FinalizedIndexService, FinalizedLogIndex};
use finalized_log_index::config::Config;
use finalized_log_index::domain::filter::{Clause, LogFilter, QueryOptions};
use finalized_log_index::domain::types::{Block, Log};
use finalized_log_index::store::blob::InMemoryBlobStore;
use finalized_log_index::store::fs::{FsBlobStore, FsMetaStore};
use finalized_log_index::store::meta::InMemoryMetaStore;
use finalized_log_index::store::traits::{BlobStore, MetaStore};
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

fn parse_arg<T>(name: &str, default: T) -> T
where
    T: std::str::FromStr,
{
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == name
            && let Some(value) = args.next()
            && let Ok(parsed) = value.parse::<T>()
        {
            return parsed;
        }
    }
    default
}

fn parse_string_arg(name: &str, default: &str) -> String {
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == name
            && let Some(value) = args.next()
        {
            return value;
        }
    }
    default.to_string()
}

fn has_flag(name: &str) -> bool {
    env::args().any(|a| a == name)
}

fn percentile_micros(sorted_micros: &[u128], pct: f64) -> u128 {
    if sorted_micros.is_empty() {
        return 0;
    }
    let idx = ((sorted_micros.len() - 1) as f64 * pct).round() as usize;
    sorted_micros[idx]
}

async fn ingest_blocks<M: MetaStore, B: BlobStore>(
    svc: &FinalizedIndexService<M, B>,
    blocks: u64,
    logs_per_block: u32,
) {
    let mut parent = [0u8; 32];
    for block_num in 1..=blocks {
        let mut logs = Vec::with_capacity(logs_per_block as usize);
        for i in 0..logs_per_block {
            let address = (i % 128) as u8;
            let topic0 = (i % 32) as u8;
            let topic1 = ((i + (block_num as u32 % 32)) % 128) as u8;
            logs.push(mk_log(address, topic0, topic1, block_num, 0, i));
        }
        let block = mk_block(block_num, parent, logs);
        parent = block.block_hash;
        svc.ingest_finalized_block(block)
            .await
            .expect("ingest block");
    }
}

async fn run_queries<M: MetaStore, B: BlobStore>(
    svc: &FinalizedIndexService<M, B>,
    blocks: u64,
    queries: usize,
    max_results: usize,
) -> (Vec<u128>, usize, Duration) {
    let mut latencies = Vec::with_capacity(queries);
    let query_start = Instant::now();
    let mut total_returned = 0usize;
    for i in 0..queries {
        let start = Instant::now();
        let filter = match i % 4 {
            0 => LogFilter {
                from_block: Some(blocks.saturating_sub(5_000).max(1)),
                to_block: Some(blocks),
                block_hash: None,
                address: Some(Clause::One([(i % 64) as u8; 20])),
                topic0: Some(Clause::One([(i % 16) as u8; 32])),
                topic1: None,
                topic2: None,
                topic3: None,
            },
            1 => LogFilter {
                from_block: Some(blocks.saturating_sub(8_000).max(1)),
                to_block: Some(blocks),
                block_hash: None,
                address: None,
                topic0: Some(Clause::One([(i % 32) as u8; 32])),
                topic1: Some(Clause::One([(i % 64) as u8; 32])),
                topic2: None,
                topic3: None,
            },
            2 => {
                let list: Vec<[u8; 20]> = (0..32).map(|v| [((v + i) % 96) as u8; 20]).collect();
                LogFilter {
                    from_block: Some(blocks.saturating_sub(3_000).max(1)),
                    to_block: Some(blocks),
                    block_hash: None,
                    address: Some(Clause::Or(list)),
                    topic0: None,
                    topic1: None,
                    topic2: None,
                    topic3: None,
                }
            }
            _ => LogFilter {
                from_block: Some(blocks.saturating_sub(1_000).max(1)),
                to_block: Some(blocks),
                block_hash: None,
                address: None,
                topic0: None,
                topic1: None,
                topic2: None,
                topic3: None,
            },
        };

        let out = svc
            .query_finalized(
                filter,
                QueryOptions {
                    max_results: Some(max_results),
                },
            )
            .await
            .expect("query");
        total_returned += out.len();
        latencies.push(start.elapsed().as_micros());
    }

    (latencies, total_returned, query_start.elapsed())
}

async fn run_stress<M: MetaStore, B: BlobStore>(
    svc: &FinalizedIndexService<M, B>,
    blocks: u64,
    logs_per_block: u32,
    queries: usize,
    max_results: usize,
) -> (Duration, Vec<u128>, usize, Duration) {
    let ingest_start = Instant::now();
    ingest_blocks(svc, blocks, logs_per_block).await;
    let ingest_elapsed = ingest_start.elapsed();
    let (latencies, total_returned, query_elapsed) =
        run_queries(svc, blocks, queries, max_results).await;
    (ingest_elapsed, latencies, total_returned, query_elapsed)
}

fn main() {
    let blocks = parse_arg("--blocks", 20_000u64);
    let logs_per_block = parse_arg("--logs-per-block", 120u32);
    let queries = parse_arg("--queries", 2_000usize);
    let chunk_size = parse_arg("--chunk-size", 512u32);
    let max_results = parse_arg("--max-results", 10_000usize);
    let backend = parse_string_arg("--backend", "memory");
    let fs_root = parse_string_arg("--fs-root", "/tmp/finalized-log-index-stress");
    let restart_before_query = has_flag("--restart-before-query");

    block_on(async move {
        let config = Config {
            target_entries_per_chunk: chunk_size,
            planner_max_or_terms: 512,
            ..Config::default()
        };

        let (ingest_elapsed, mut latencies, total_returned, query_elapsed) = if backend == "fs" {
            let root = PathBuf::from(&fs_root);
            if root.exists() {
                fs::remove_dir_all(&root).expect("remove prior fs root");
            }
            fs::create_dir_all(&root).expect("create fs root");

            if restart_before_query {
                let svc = FinalizedIndexService::new(
                    config.clone(),
                    FsMetaStore::new(&root, 1).expect("fs meta"),
                    FsBlobStore::new(&root).expect("fs blob"),
                    1,
                );
                let ingest_start = Instant::now();
                ingest_blocks(&svc, blocks, logs_per_block).await;
                let ingest_elapsed = ingest_start.elapsed();
                drop(svc);

                let query_svc = FinalizedIndexService::new(
                    config,
                    FsMetaStore::new(&root, 1).expect("fs meta reopen"),
                    FsBlobStore::new(&root).expect("fs blob reopen"),
                    1,
                );
                let (latencies, total_returned, query_elapsed) =
                    run_queries(&query_svc, blocks, queries, max_results).await;
                (ingest_elapsed, latencies, total_returned, query_elapsed)
            } else {
                let svc = FinalizedIndexService::new(
                    config,
                    FsMetaStore::new(&root, 1).expect("fs meta"),
                    FsBlobStore::new(&root).expect("fs blob"),
                    1,
                );
                run_stress(&svc, blocks, logs_per_block, queries, max_results).await
            }
        } else {
            let svc = FinalizedIndexService::new(
                config,
                InMemoryMetaStore::default(),
                InMemoryBlobStore::default(),
                1,
            );
            run_stress(&svc, blocks, logs_per_block, queries, max_results).await
        };

        latencies.sort_unstable();
        let total_logs = blocks as u128 * logs_per_block as u128;
        let ingest_tps = if ingest_elapsed.as_secs_f64() > 0.0 {
            total_logs as f64 / ingest_elapsed.as_secs_f64()
        } else {
            0.0
        };
        let qps = if query_elapsed.as_secs_f64() > 0.0 {
            queries as f64 / query_elapsed.as_secs_f64()
        } else {
            0.0
        };

        println!(
            "stress_config backend={backend} blocks={blocks} logs_per_block={logs_per_block} queries={queries} chunk_size={chunk_size} max_results={max_results} restart_before_query={restart_before_query}"
        );
        if backend == "fs" {
            println!("fs_root={fs_root}");
        }
        println!(
            "ingest total_logs={total_logs} elapsed_ms={} logs_per_sec={ingest_tps:.2}",
            ingest_elapsed.as_millis()
        );
        println!(
            "query total_queries={queries} elapsed_ms={} qps={qps:.2} total_returned={total_returned}",
            query_elapsed.as_millis()
        );
        println!(
            "latency_us p50={} p95={} p99={} max={}",
            percentile_micros(&latencies, 0.50),
            percentile_micros(&latencies, 0.95),
            percentile_micros(&latencies, 0.99),
            latencies.last().copied().unwrap_or(0)
        );
    });
}
